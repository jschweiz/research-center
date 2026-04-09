from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse

import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from app.core.config import get_settings
from app.core.outbound import fetch_safe_response
from app.db.models import IngestionRunType, RunStatus
from app.db.session import get_session_factory
from app.integrations.alphaxiv import AlphaXivClient, AlphaXivPaperBundle
from app.integrations.extractors import ContentExtractor, ExtractedContent
from app.integrations.gmail import GmailConnector, NewsletterMessage
from app.integrations.gmail_imap import GmailImapConnector
from app.schemas.ops import IngestionRunHistoryRead, OperationBasicInfoRead
from app.schemas.profile import AlphaXivSearchSettings
from app.services.connections import ConnectionService
from app.services.profile import load_profile_snapshot
from app.services.text import normalize_whitespace
from app.services.vault_runtime import (
    RunRecorder,
    classify_written_kind,
    content_hash,
    document_identity_hash,
    readable_doc_id,
    utcnow,
)
from app.vault.models import RawDocumentFrontmatter, VaultSourceDefinition, VaultSourcesConfig
from app.vault.store import LeaseBusyError, VaultStore

TRACKING_PREFIXES = ("utm_", "mc_", "ref", "source")
WEBSITE_TIMEOUT_SECONDS = 20
DEFAULT_HTML_FILENAME = "original.html"
NEWSLETTER_ENTRY_LIMIT = 8
ALPHAXIV_PODCAST_FILENAME = "alphaxiv-podcast.mp3"
ALPHAXIV_DISCOVERY_PAGE_SIZE = 50
NEWSLETTER_NOISE_TEXT = (
    "unsubscribe",
    "privacy policy",
    "advertis",
    "sponsor",
    "view in browser",
    "manage preferences",
)
TLDR_ISSUE_HEADING_RE = re.compile(r"^TLDR\s+\d{4}-\d{2}-\d{2}$", re.IGNORECASE)
TLDR_READ_TIME_RE = re.compile(r"\((\d+\s+minute(?:s)? read)\)\s*$", re.IGNORECASE)
TLDR_AD_HOSTS = {
    "advertise.tldr.tech",
    "hub.sparklp.co",
    "jobs.ashbyhq.com",
    "refer.tldr.tech",
}
MEDIUM_HIGHLIGHTS_HEADING_RE = re.compile(r"^today[’']s highlights$", re.IGNORECASE)
MEDIUM_READ_TIME_RE = re.compile(r"^\d+\s+min read$", re.IGNORECASE)
MEDIUM_HOSTS = {"medium.com", "www.medium.com"}
MISTRAL_NEWS_SCRIPT_ENTRY_RE = re.compile(
    r'\{\\"id\\":\\"[^"]+\\",\\"slug\\":\\"(?P<slug>[^"]+)\\",'
    r'.*?\\"date\\":\\"(?P<date>[^"]+)\\",'
    r'.*?\\"category\\":\{.*?\\"name\\":\\"(?P<category>[^"]+)\\",.*?\}'
    r',\\"title\\":\\"(?P<title>(?:(?!\\",\\"description\\":).)*)\\",'
    r'\\"description\\":(?:(?P<description_null>null)|\\"(?P<description>(?:(?!\\",\\"locale\\":\\"en\\").)*)\\")'
    r',\\"locale\\":\\"en\\"\}',
    re.DOTALL,
)
RawAssetContent = str | bytes
RawAssetMap = dict[str, RawAssetContent]

DEFAULT_SOURCES = VaultSourcesConfig.model_validate(
    {
        "sources": [
            {
                "id": "openai-website",
                "type": "website",
                "name": "OpenAI Website",
                "enabled": True,
                "raw_kind": "blog-post",
                "custom_pipeline_id": "openai-website",
                "classification_mode": "fixed",
                "decomposition_mode": "none",
                "description": "OpenAI official website posts discovered from the public site feed.",
                "tags": ["openai", "official", "website", "blog-post"],
                "url": "https://openai.com/news/rss.xml",
                "max_items": 20,
                "config_json": {
                    "discovery_mode": "rss_feed",
                    "website_url": "https://openai.com/news/",
                    "allowed_hosts": ["openai.com"],
                    "article_path_prefixes": ["/index/"],
                },
            },
            {
                "id": "anthropic-research",
                "type": "website",
                "name": "Anthropic Research",
                "enabled": True,
                "raw_kind": "blog-post",
                "custom_pipeline_id": "anthropic-research",
                "classification_mode": "fixed",
                "decomposition_mode": "none",
                "description": "Anthropic research posts discovered from the official research index.",
                "tags": ["anthropic", "official", "research", "website", "blog-post"],
                "url": "https://www.anthropic.com/research",
                "max_items": 20,
                "config_json": {
                    "discovery_mode": "website_index",
                    "website_url": "https://www.anthropic.com/research",
                    "allowed_hosts": ["www.anthropic.com", "anthropic.com"],
                    "article_path_prefixes": ["/research/"],
                    "exclude_patterns": [r"^/research$", r"^/research/team/"],
                },
            },
            {
                "id": "mistral-research",
                "type": "website",
                "name": "Mistral Research",
                "enabled": True,
                "raw_kind": "blog-post",
                "custom_pipeline_id": "mistral-research",
                "classification_mode": "fixed",
                "decomposition_mode": "none",
                "description": "Mistral research posts discovered from the official news page filtered to Research.",
                "tags": ["mistral", "official", "research", "website", "blog-post"],
                "url": "https://mistral.ai/news?category=research",
                "max_items": 20,
                "config_json": {
                    "discovery_mode": "website_index",
                    "website_url": "https://mistral.ai/news?category=research",
                    "allowed_hosts": ["mistral.ai", "www.mistral.ai"],
                    "article_path_prefixes": ["/news/"],
                    "script_entry_parser": "mistral_news_posts",
                    "required_categories": ["Research"],
                },
            },
            {
                "id": "tldr-email",
                "type": "gmail_newsletter",
                "name": "TLDR Email",
                "enabled": True,
                "raw_kind": "newsletter",
                "custom_pipeline_id": "tldr-email",
                "classification_mode": "written_content_auto",
                "decomposition_mode": "newsletter_entries",
                "description": "TLDR newsletter emails pulled from Gmail.",
                "tags": ["newsletter", "tldr", "email", "ai"],
                "url": None,
                "max_items": 20,
                "config_json": {
                    "senders": [
                        "dan@tldrnewsletter.com",
                        "hi@tldrnewsletter.com",
                        "newsletter@tldrnewsletter.com",
                    ],
                },
            },
            {
                "id": "medium-email",
                "type": "gmail_newsletter",
                "name": "Medium Email",
                "enabled": True,
                "raw_kind": "newsletter",
                "custom_pipeline_id": "medium-email",
                "classification_mode": "written_content_auto",
                "decomposition_mode": "newsletter_entries",
                "description": "Medium digest emails pulled from Gmail.",
                "tags": ["newsletter", "medium", "email"],
                "url": None,
                "max_items": 20,
                "config_json": {
                    "senders": ["noreply@medium.com"],
                },
            },
            {
                "id": "alphaxiv-paper",
                "type": "website",
                "name": "alphaXiv Papers",
                "enabled": True,
                "raw_kind": "paper",
                "custom_pipeline_id": "alphaxiv-paper",
                "classification_mode": "fixed",
                "decomposition_mode": "none",
                "description": "alphaXiv paper listings normalized into paper raw documents.",
                "tags": ["paper", "alphaxiv", "research"],
                "url": "https://www.alphaxiv.org/",
                "max_items": 20,
                "config_json": {
                    "discovery_mode": "website_index",
                    "website_url": "https://www.alphaxiv.org/",
                    "allowed_hosts": ["www.alphaxiv.org", "alphaxiv.org"],
                    "article_path_prefixes": ["/abs/"],
                },
            },
        ]
    }
)


@dataclass(frozen=True)
class WebsiteEntry:
    link: str
    title: str
    published_at: datetime | None = None
    summary: str | None = None


@dataclass(frozen=True)
class NewsletterEntry:
    title: str
    link: str | None
    body: str
    external_key: str
    kind: str
    asset_map: RawAssetMap


@dataclass(frozen=True)
class ParsedNewsletterStory:
    section: str
    title: str
    link: str | None
    summary: str


@dataclass(frozen=True)
class ParsedNewsletterSection:
    title: str
    stories: list[ParsedNewsletterStory]


@dataclass(frozen=True)
class SourceSyncResult:
    source_count: int
    synced_document_count: int
    failed_source_count: int


@dataclass(frozen=True)
class SyncWriteResult:
    doc_id: str
    kind: str
    title: str
    published_at: datetime | None
    created: bool
    updated: bool


class SourceFetchCancelledError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.run_id = run_id
        self.source_id = source_id


class VaultFetchService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.store = VaultStore()
        self.extractor = ContentExtractor()
        self.alphaxiv = AlphaXivClient(timeout_seconds=WEBSITE_TIMEOUT_SECONDS)
        self.runs = RunRecorder(self.store)
        self.store.ensure_layout()
        self.ensure_default_sources_config()

    def ensure_default_sources_config(self) -> VaultSourcesConfig:
        current = self.store.load_sources_config()
        if current.sources:
            return current
        self.store.save_sources_config(DEFAULT_SOURCES)
        return self.store.load_sources_config()

    def get_source(self, source_id: str) -> VaultSourceDefinition | None:
        config = self.ensure_default_sources_config()
        return next((source for source in config.sources if source.id == source_id), None)

    def sync_enabled_sources(self, *, trigger: str = "manual_fetch") -> SourceSyncResult:
        config = self.ensure_default_sources_config()
        enabled_sources = [source for source in config.sources if source.enabled]
        if not self.settings.vault_source_pipelines_enabled:
            return SourceSyncResult(
                source_count=len(enabled_sources),
                synced_document_count=0,
                failed_source_count=0,
            )

        run = self.runs.start(
            run_type=IngestionRunType.INGEST,
            operation_kind="raw_fetch",
            trigger=trigger,
            title="Raw fetch",
            summary="Fetching configured sources into the raw vault.",
        )
        lease = None
        try:
            try:
                lease = self.store.acquire_lease(name="raw-fetch", owner="mac", ttl_seconds=900)
            except LeaseBusyError as exc:
                run.errors.append(str(exc))
                self.runs.finish(
                    run,
                    status=RunStatus.FAILED,
                    summary="Raw fetch skipped because another fetch run is already active.",
                )
                return SourceSyncResult(
                    source_count=len(enabled_sources),
                    synced_document_count=0,
                    failed_source_count=len(enabled_sources),
                )
        except Exception:
            if lease is not None:
                self.store.release_lease(lease)
            raise

        try:
            synced_document_count = 0
            failed_source_count = 0
            run.source_count = len(enabled_sources)
            for source in enabled_sources:
                synced_count, source_run = self._sync_source_with_run(source, trigger=trigger)
                synced_document_count += synced_count
                if source_run.status == RunStatus.FAILED:
                    failed_source_count += 1
                    run.failed_source_count += 1
                    for error in source_run.errors:
                        run.errors.append(f"{source.name}: {error}")
                self.runs.log(
                    run,
                    f"{source.name}: {source_run.summary}",
                    level="error" if source_run.status == RunStatus.FAILED else "info",
                )

            run.total_titles = synced_document_count
            run.basic_info.extend(
                [
                    OperationBasicInfoRead(label="Sources", value=str(len(enabled_sources))),
                    OperationBasicInfoRead(
                        label="Raw docs synced", value=str(synced_document_count)
                    ),
                    OperationBasicInfoRead(label="Failures", value=str(failed_source_count)),
                    OperationBasicInfoRead(
                        label="Sources config", value="SQLite `vault_sources`"
                    ),
                ]
            )
            summary = (
                f"Fetched {synced_document_count} raw document"
                f"{'' if synced_document_count == 1 else 's'} from {len(enabled_sources)} source"
                f"{'' if len(enabled_sources) == 1 else 's'}."
            )
            if failed_source_count:
                summary += f" {failed_source_count} source{'s' if failed_source_count != 1 else ''} failed."
            self.runs.finish(
                run,
                status=RunStatus.SUCCEEDED if failed_source_count == 0 else RunStatus.FAILED,
                summary=summary,
            )
            return SourceSyncResult(
                source_count=len(enabled_sources),
                synced_document_count=synced_document_count,
                failed_source_count=failed_source_count,
            )
        finally:
            if lease is not None:
                self.store.release_lease(lease)

    def sync_source_by_id(
        self,
        source_id: str,
        *,
        trigger: str = "manual_source_fetch",
        max_items: int | None = None,
    ) -> IngestionRunHistoryRead:
        source = self.get_source(source_id)
        if source is None:
            raise RuntimeError(f"Unknown source: {source_id}")
        if not self.settings.vault_source_pipelines_enabled:
            raise RuntimeError("Source pipelines are disabled in this environment.")

        lease = None
        try:
            lease = self.store.acquire_lease(name="raw-fetch", owner="mac", ttl_seconds=900)
            _, run_record = self._sync_source_with_run(source, trigger=trigger, max_items=max_items)
            if self._run_was_cancelled(run_record):
                raise SourceFetchCancelledError(
                    str(run_record.summary),
                    run_id=run_record.id,
                    source_id=source.id,
                )
            if run_record.status == RunStatus.FAILED:
                detail = run_record.errors[0] if run_record.errors else run_record.summary
                raise RuntimeError(detail)
            return run_record
        finally:
            if lease is not None:
                self.store.release_lease(lease)

    def request_stop_for_source(self, source_id: str) -> IngestionRunHistoryRead:
        source = self.get_source(source_id)
        if source is None:
            raise RuntimeError(f"Unknown source: {source_id}")

        run = self.latest_run_for_source(source_id)
        if run is None or run.status not in {RunStatus.RUNNING, RunStatus.PENDING}:
            raise RuntimeError(f"No running fetch exists for {source.name}.")

        if not self.store.is_operation_stop_requested(run.id):
            self.store.request_operation_stop(
                run_id=run.id,
                source_id=source_id,
                requested_by="local-control",
            )
        return self._load_run_record(run.id) or run

    def latest_run_for_source(self, source_id: str) -> IngestionRunHistoryRead | None:
        records = self.store.load_run_records()
        for payload in reversed(records):
            if self._run_matches_source(payload, source_id):
                return IngestionRunHistoryRead.model_validate(payload)
        return None

    def _sync_source_with_run(
        self,
        source: VaultSourceDefinition,
        *,
        trigger: str,
        max_items: int | None = None,
    ) -> tuple[int, IngestionRunHistoryRead]:
        effective_max_items = max_items if max_items is not None else source.max_items
        run = self.runs.start(
            run_type=IngestionRunType.INGEST,
            operation_kind="raw_fetch",
            trigger=f"{trigger}:{source.id}" if trigger else source.id,
            title=f"Fetch {source.name}",
            summary=f"Fetching raw documents for {source.name}.",
        )
        run.source_count = 1
        run.basic_info.extend(
            [
                OperationBasicInfoRead(label="Source ID", value=source.id),
                OperationBasicInfoRead(label="Source", value=source.name),
                OperationBasicInfoRead(label="Source type", value=source.type),
                OperationBasicInfoRead(label="Raw kind", value=source.raw_kind),
                OperationBasicInfoRead(label="Classification", value=source.classification_mode),
                OperationBasicInfoRead(label="Decomposition", value=source.decomposition_mode),
                OperationBasicInfoRead(label="Max items", value=str(effective_max_items)),
            ]
        )
        if effective_max_items != source.max_items:
            run.basic_info.append(
                OperationBasicInfoRead(label="Configured max items", value=str(source.max_items))
            )
        if source.custom_pipeline_id:
            run.basic_info.append(
                OperationBasicInfoRead(label="Custom pipeline", value=source.custom_pipeline_id)
            )
        self.runs.log(run, f"Starting raw fetch for {source.name}.")
        fetch_step = self.runs.start_step(run, step_kind="raw_fetch", source_id=source.id)

        try:
            try:
                synced, kinds, created_count, updated_count, decomposition_count = self._sync_source(
                    source,
                    run,
                    max_items=effective_max_items,
                )
            except SourceFetchCancelledError as exc:
                message = str(exc)
                run.errors.append(message)
                run.basic_info.append(
                    OperationBasicInfoRead(label="Canceled", value="local-control")
                )
                self.runs.log(run, message, level="warning")
                self.runs.finish_step(run, fetch_step, status=RunStatus.FAILED)
                record = self.runs.finish(
                    run,
                    status=RunStatus.FAILED,
                    summary=f"Raw fetch canceled for {source.name}.",
                )
                return 0, record
            except Exception as exc:
                run.errors.append(str(exc))
                self.runs.log(run, f"{source.name}: {exc}", level="error")
                self.runs.finish_step(run, fetch_step, status=RunStatus.FAILED)
                return 0, self.runs.finish(
                    run,
                    status=RunStatus.FAILED,
                    summary=f"Raw fetch failed for {source.name}.",
                )

            self.runs.finish_step(
                run,
                fetch_step,
                status=RunStatus.SUCCEEDED,
                created_count=created_count,
                updated_count=updated_count,
                counts_by_kind=kinds,
            )
            if decomposition_count:
                decomposition_step = self.runs.start_step(
                    run, step_kind="newsletter_decomposition", source_id=source.id
                )
                self.runs.log_step(
                    run,
                    decomposition_step,
                    f"{source.name}: created or refreshed {decomposition_count} derived newsletter entr{'y' if decomposition_count == 1 else 'ies'}.",
                )
                self.runs.finish_step(
                    run,
                    decomposition_step,
                    status=RunStatus.SUCCEEDED,
                    created_count=decomposition_count,
                    counts_by_kind={
                        kind: count for kind, count in kinds.items() if kind != "newsletter"
                    },
                )
            run.total_titles = synced
            run.created_count = created_count
            run.updated_count = updated_count
            run.basic_info.extend(
                [
                    OperationBasicInfoRead(label="Raw docs synced", value=str(synced)),
                    OperationBasicInfoRead(label="Created", value=str(created_count)),
                    OperationBasicInfoRead(label="Updated", value=str(updated_count)),
                    OperationBasicInfoRead(
                        label="Kinds", value=", ".join(sorted(kinds)) if kinds else "none"
                    ),
                ]
            )
            self.runs.log(
                run,
                f"Completed raw fetch for {source.name} with {synced} raw document{'s' if synced != 1 else ''}.",
            )
            return synced, self.runs.finish(
                run,
                status=RunStatus.SUCCEEDED,
                summary=f"Fetched {synced} raw document{'s' if synced != 1 else ''} for {source.name}.",
            )
        finally:
            self.store.clear_operation_stop_request(run.id)

    @staticmethod
    def _run_matches_source(payload: dict[str, object], source_id: str) -> bool:
        if payload.get("operation_kind") != "raw_fetch":
            return False
        basic_info = payload.get("basic_info")
        if not isinstance(basic_info, list):
            return False
        for entry in basic_info:
            if not isinstance(entry, dict):
                continue
            if entry.get("label") == "Source ID" and entry.get("value") == source_id:
                return True
        return False

    @staticmethod
    def _run_was_cancelled(run: IngestionRunHistoryRead) -> bool:
        return any(entry.label == "Canceled" and entry.value == "local-control" for entry in run.basic_info)

    def _load_run_record(self, run_id: str) -> IngestionRunHistoryRead | None:
        for payload in reversed(self.store.load_run_records()):
            if str(payload.get("id") or "").strip() == run_id:
                return IngestionRunHistoryRead.model_validate(payload)
        return None

    def _raise_if_stop_requested(
        self,
        *,
        run,
        source: VaultSourceDefinition,
    ) -> None:
        if not self.store.is_operation_stop_requested(run.id):
            return
        raise SourceFetchCancelledError(
            f"Source fetch for {source.name} was canceled from local-control.",
            run_id=run.id,
            source_id=source.id,
        )

    def _sync_source(
        self,
        source: VaultSourceDefinition,
        run,
        *,
        max_items: int,
    ) -> tuple[int, dict[str, int], int, int, int]:
        if source.type == "website":
            return self._sync_website_source(source, run, max_items=max_items)
        if source.type == "gmail_newsletter":
            connector = self._build_gmail_connector()
            if connector is None:
                raise RuntimeError("Gmail ingest credentials are not configured.")
            return self._sync_gmail_source(source, connector, run, max_items=max_items)
        raise RuntimeError(f"Unsupported source type: {source.type}")

    def _sync_website_source(
        self,
        source: VaultSourceDefinition,
        run,
        *,
        max_items: int,
    ) -> tuple[int, dict[str, int], int, int, int]:
        if source.custom_pipeline_id == "alphaxiv-paper":
            return self._sync_alphaxiv_source(source, run, max_items=max_items)

        synced = 0
        created_count = 0
        updated_count = 0
        counts_by_kind: dict[str, int] = {}
        self._raise_if_stop_requested(run=run, source=source)
        for entry in self._discover_website_entries(source, max_entries=max_items, run=run):
            self._raise_if_stop_requested(run=run, source=source)
            extracted = self._extract_website_entry(entry)
            title = extracted.title or entry.title or entry.link
            body = extracted.cleaned_text or entry.summary or entry.title or entry.link
            published_at = extracted.published_at or entry.published_at
            kind = self._resolve_raw_kind(source=source, source_url=entry.link, title=title)
            asset_map = {}
            html = (
                extracted.raw_payload.get("html")
                if isinstance(extracted.raw_payload, dict)
                else None
            )
            if isinstance(html, str) and html.strip():
                asset_map[DEFAULT_HTML_FILENAME] = html
            result = self._upsert_raw_document(
                source=source,
                kind=kind,
                stable_key=normalize_url(entry.link),
                external_key=normalize_url(entry.link),
                title=title,
                body=body,
                source_url=normalize_url(entry.link),
                source_name=source.name,
                authors=[],
                published_at=published_at,
                tags=source.tags,
                asset_map=asset_map,
                doc_role="primary",
                parent_id=None,
                index_visibility="visible",
                short_summary=None,
            )
            counts_by_kind[result.kind] = counts_by_kind.get(result.kind, 0) + 1
            created_count += 1 if result.created else 0
            updated_count += 1 if result.updated else 0
            synced += 1
            self.runs.log(
                run, self._format_raw_document_sync_log(source_name=source.name, result=result)
            )
        self._raise_if_stop_requested(run=run, source=source)
        return synced, counts_by_kind, created_count, updated_count, 0

    def _sync_alphaxiv_source(
        self,
        source: VaultSourceDefinition,
        run,
        *,
        max_items: int,
    ) -> tuple[int, dict[str, int], int, int, int]:
        synced = 0
        created_count = 0
        updated_count = 0
        counts_by_kind: dict[str, int] = {}
        self._raise_if_stop_requested(run=run, source=source)
        for entry in self._discover_website_entries(source, max_entries=max_items, run=run):
            self._raise_if_stop_requested(run=run, source=source)
            try:
                bundle = self.alphaxiv.fetch_paper(entry.link)
                title = bundle.title or entry.title or entry.link
                published_at = (
                    bundle.published_at or bundle.first_published_at or entry.published_at
                )
                result = self._upsert_raw_document(
                    source=source,
                    kind=source.raw_kind,
                    stable_key=normalize_url(entry.link),
                    external_key=normalize_url(entry.link),
                    title=title,
                    body=self._render_alphaxiv_body(bundle),
                    source_url=normalize_url(bundle.abs_url),
                    source_name=source.name,
                    authors=bundle.authors,
                    published_at=published_at,
                    tags=self._alphaxiv_tags(source, bundle),
                    asset_map=self._build_alphaxiv_asset_map(bundle),
                    doc_role="primary",
                    parent_id=None,
                    index_visibility="visible",
                    short_summary=bundle.short_summary,
                    hash_title=bundle.canonical_id or bundle.paper_id or title,
                    hash_body=self._alphaxiv_hash_identity(bundle),
                )
            except Exception as exc:
                self.runs.log(
                    run,
                    f"{source.name}: alphaXiv enrichment failed for {entry.link}: {exc}. Falling back to generic extraction.",
                    level="error",
                )
                extracted = self._extract_website_entry(entry)
                title = extracted.title or entry.title or entry.link
                body = extracted.cleaned_text or entry.summary or entry.title or entry.link
                published_at = extracted.published_at or entry.published_at
                asset_map = {}
                html = (
                    extracted.raw_payload.get("html")
                    if isinstance(extracted.raw_payload, dict)
                    else None
                )
                if isinstance(html, str) and html.strip():
                    asset_map[DEFAULT_HTML_FILENAME] = html
                result = self._upsert_raw_document(
                    source=source,
                    kind=source.raw_kind,
                    stable_key=normalize_url(entry.link),
                    external_key=normalize_url(entry.link),
                    title=title,
                    body=body,
                    source_url=normalize_url(entry.link),
                    source_name=source.name,
                    authors=[],
                    published_at=published_at,
                    tags=source.tags,
                    asset_map=asset_map,
                    doc_role="primary",
                    parent_id=None,
                    index_visibility="visible",
                    short_summary=None,
                )

            counts_by_kind[result.kind] = counts_by_kind.get(result.kind, 0) + 1
            created_count += 1 if result.created else 0
            updated_count += 1 if result.updated else 0
            synced += 1
            self.runs.log(
                run, self._format_raw_document_sync_log(source_name=source.name, result=result)
            )
        self._raise_if_stop_requested(run=run, source=source)
        return synced, counts_by_kind, created_count, updated_count, 0

    def _alphaxiv_tags(
        self,
        source: VaultSourceDefinition,
        bundle: AlphaXivPaperBundle,
    ) -> list[str]:
        extra_tags = list(bundle.topics)
        if bundle.github_url:
            extra_tags.append("github")
        if bundle.podcast_url:
            extra_tags.append("audio")
        if bundle.transcript:
            extra_tags.append("transcript")
        if bundle.short_summary:
            extra_tags.append("summary")
        return merge_unique_strings(*source.tags, *extra_tags)

    def _alphaxiv_hash_identity(self, bundle: AlphaXivPaperBundle) -> str:
        identity_parts = [
            bundle.canonical_id or bundle.paper_id or bundle.title,
            bundle.source_url or bundle.abs_url,
            bundle.first_published_at.isoformat() if bundle.first_published_at else "",
        ]
        return "\n".join(part for part in identity_parts if part)

    def _render_alphaxiv_body(self, bundle: AlphaXivPaperBundle) -> str:
        lines = [f"# {bundle.title}"]
        if bundle.short_summary:
            lines.extend(["", "## alphaXiv Summary", "", bundle.short_summary])

        metadata_lines = [
            f"alphaXiv URL: {bundle.abs_url}",
            *([f"Source paper: {bundle.source_url}"] if bundle.source_url else []),
            *([f"PDF: {bundle.pdf_url}"] if bundle.pdf_url else []),
            *([f"Cover image: {bundle.image_url}"] if bundle.image_url else []),
            *([f"GitHub: {bundle.github_url}"] if bundle.github_url else []),
            *([f"GitHub stars: {bundle.github_stars}"] if bundle.github_stars is not None else []),
            *([f"Paper ID: {bundle.paper_id}"] if bundle.paper_id else []),
            *([f"Canonical ID: {bundle.canonical_id}"] if bundle.canonical_id else []),
            *([f"Paper group ID: {bundle.group_id}"] if bundle.group_id else []),
            *([f"Version ID: {bundle.version_id}"] if bundle.version_id else []),
            *([f"Version: {bundle.version_label}"] if bundle.version_label else []),
            *(
                [f"Version order: {bundle.version_order}"]
                if bundle.version_order is not None
                else []
            ),
            *([f"Published at: {bundle.published_at.isoformat()}"] if bundle.published_at else []),
            *(
                [f"First published at: {bundle.first_published_at.isoformat()}"]
                if bundle.first_published_at
                else []
            ),
            *([f"Updated at: {bundle.updated_at.isoformat()}"] if bundle.updated_at else []),
            *([f"License: {bundle.license}"] if bundle.license else []),
            *(
                [f"Citations: {bundle.citations_count}"]
                if bundle.citations_count is not None
                else []
            ),
            *(["BibTeX saved in `alphaxiv-citation.bib`"] if bundle.citation_bibtex else []),
        ]
        if metadata_lines:
            lines.extend(["", "## Metadata", ""])
            lines.extend(f"- {line}" for line in metadata_lines)

        if bundle.authors:
            lines.extend(["", "## Authors", ""])
            lines.extend(f"- {author}" for author in bundle.authors)

        if bundle.topics:
            lines.extend(["", "## Topics", ""])
            lines.extend(f"- {topic}" for topic in bundle.topics)

        metrics_lines = self._alphaxiv_metrics_lines(bundle.metrics)
        if metrics_lines:
            lines.extend(["", "## Metrics", ""])
            lines.extend(f"- {line}" for line in metrics_lines)

        if bundle.abstract:
            lines.extend(["", "## Abstract", "", bundle.abstract])

        summary_sections = (
            (
                "Problem",
                bundle.summary.get("originalProblem") if isinstance(bundle.summary, dict) else None,
            ),
            (
                "Method",
                bundle.summary.get("solution") if isinstance(bundle.summary, dict) else None,
            ),
            (
                "Results",
                bundle.summary.get("results") if isinstance(bundle.summary, dict) else None,
            ),
            (
                "Takeaways",
                bundle.summary.get("keyInsights") if isinstance(bundle.summary, dict) else None,
            ),
        )
        for heading, content in summary_sections:
            items = self._alphaxiv_text_list(content)
            if not items:
                continue
            lines.extend(["", f"## {heading}", ""])
            lines.extend(f"- {item}" for item in items)

        if bundle.overview_markdown:
            lines.extend(
                [
                    "",
                    "## Full Overview",
                    "",
                    "Saved in `alphaxiv-overview.md` and `alphaxiv-overview.json`.",
                ]
            )
        if bundle.overview_languages:
            lines.extend(["", "## Overview Languages", ""])
            lines.extend(f"- {language}" for language in bundle.overview_languages)

        if bundle.podcast_url or bundle.transcript:
            lines.extend(["", "## Audio Summary", ""])
            if bundle.podcast_url:
                lines.append(f"- MP3: {bundle.podcast_url}")
            if bundle.podcast_audio:
                lines.append(f"- Audio asset: `{ALPHAXIV_PODCAST_FILENAME}`")
            if bundle.transcript_url:
                lines.append(f"- Transcript JSON: {bundle.transcript_url}")
            if bundle.transcript:
                lines.append(f"- Transcript lines: {len(bundle.transcript)}")
                lines.append(
                    "- Transcript assets: `alphaxiv-transcript.md`, `alphaxiv-transcript.json`"
                )

        ai_detection_lines = self._alphaxiv_ai_detection_lines(bundle.ai_detection)
        if ai_detection_lines:
            lines.extend(["", "## AI Detection", ""])
            lines.extend(f"- {line}" for line in ai_detection_lines)

        resource_lines = self._alphaxiv_resource_lines(bundle)
        if resource_lines:
            lines.extend(["", "## Resources", ""])
            lines.extend(f"- {line}" for line in resource_lines)

        if bundle.similar_papers:
            lines.extend(["", "## Similar Papers", ""])
            for paper in bundle.similar_papers[:5]:
                title = str(paper.get("title") or "").strip()
                if not title:
                    continue
                canonical_id = str(paper.get("canonical_id") or "").strip()
                summary = (
                    str(((paper.get("paper_summary") or {}).get("summary")) or "").strip()
                    if isinstance(paper, dict)
                    else ""
                )
                label = f"{title} ({canonical_id})" if canonical_id else title
                if summary:
                    lines.append(f"- {label}: {summary}")
                else:
                    lines.append(f"- {label}")
            lines.append("- Full similar-paper payload saved in `alphaxiv-similar-papers.json`.")

        return "\n".join(lines).strip() + "\n"

    def _build_alphaxiv_asset_map(self, bundle: AlphaXivPaperBundle) -> RawAssetMap:
        asset_map = {
            "alphaxiv-paper.json": self._json_dump(bundle.paper_payload),
            "alphaxiv-preview.json": self._json_dump(bundle.preview_payload),
            "alphaxiv-legacy.json": self._json_dump(bundle.legacy_payload),
            "alphaxiv-metadata.json": self._json_dump(bundle.metadata_payload()),
            "alphaxiv-similar-papers.json": self._json_dump(bundle.similar_papers),
        }
        if bundle.podcast_audio:
            asset_map[ALPHAXIV_PODCAST_FILENAME] = bundle.podcast_audio
        if bundle.citation_bibtex:
            asset_map["alphaxiv-citation.bib"] = bundle.citation_bibtex.strip() + "\n"
        if bundle.overview_status:
            asset_map["alphaxiv-overview-status.json"] = self._json_dump(bundle.overview_status)
        if bundle.overview:
            asset_map["alphaxiv-overview.json"] = self._json_dump(bundle.overview)
        if bundle.overview_markdown:
            asset_map["alphaxiv-overview.md"] = self._render_alphaxiv_overview_markdown(bundle)
        if bundle.ai_detection:
            asset_map["alphaxiv-ai-detection.json"] = self._json_dump(bundle.ai_detection)
        if bundle.transcript:
            asset_map["alphaxiv-transcript.json"] = self._json_dump(bundle.transcript)
            asset_map["alphaxiv-transcript.md"] = self._render_alphaxiv_transcript_markdown(bundle)
        return asset_map

    def _render_alphaxiv_overview_markdown(self, bundle: AlphaXivPaperBundle) -> str:
        lines = [
            f"# {bundle.title}",
            "",
            f"- alphaXiv: {bundle.abs_url}",
        ]
        if bundle.source_url:
            lines.append(f"- Source paper: {bundle.source_url}")
        if bundle.overview_markdown:
            lines.extend(["", bundle.overview_markdown])
        citations = bundle.overview.get("citations") if isinstance(bundle.overview, dict) else None
        if isinstance(citations, list) and citations:
            lines.extend(["", "## Relevant Citations", ""])
            for citation in citations:
                if not isinstance(citation, dict):
                    continue
                title = str(citation.get("title") or "").strip()
                justification = str(citation.get("justification") or "").strip()
                if title and justification:
                    lines.append(f"- {title}: {justification}")
                elif title:
                    lines.append(f"- {title}")
        return "\n".join(lines).strip() + "\n"

    def _render_alphaxiv_transcript_markdown(self, bundle: AlphaXivPaperBundle) -> str:
        lines = [
            f"# {bundle.title}",
            "",
            f"- alphaXiv: {bundle.abs_url}",
        ]
        if bundle.podcast_url:
            lines.append(f"- Audio summary: {bundle.podcast_url}")
        if bundle.podcast_audio:
            lines.append(f"- Saved audio file: `{ALPHAXIV_PODCAST_FILENAME}`")
        if bundle.transcript_url:
            lines.append(f"- Transcript JSON: {bundle.transcript_url}")
        lines.extend(["", "## Transcript", ""])
        for turn in bundle.transcript:
            speaker = normalize_web_text(turn.get("speaker")) or "Speaker"
            line = normalize_web_text(turn.get("line")) or ""
            if not line:
                continue
            lines.append(f"**{speaker}:** {line}")
        return "\n".join(lines).strip() + "\n"

    def _alphaxiv_metrics_lines(self, metrics: dict[str, Any]) -> list[str]:
        if not isinstance(metrics, dict):
            return []
        visits = metrics.get("visits_count") or metrics.get("visitsCount") or {}
        lines: list[str] = []
        if isinstance(visits, dict):
            all_visits = visits.get("all")
            last_seven = visits.get("last_7_days") or visits.get("last7Days")
            last_thirty = visits.get("last30Days")
            last_ninety = visits.get("last90Days")
            last_day = visits.get("last24Hours")
            if all_visits is not None:
                lines.append(f"Visits (all): {all_visits}")
            if last_seven is not None:
                lines.append(f"Visits (last 7 days): {last_seven}")
            if last_thirty is not None:
                lines.append(f"Visits (last 30 days): {last_thirty}")
            if last_ninety is not None:
                lines.append(f"Visits (last 90 days): {last_ninety}")
            if last_day is not None:
                lines.append(f"Visits (last 24 hours): {last_day}")
        for label, key in (
            ("Total votes", "total_votes"),
            ("Public total votes", "public_total_votes"),
            ("X likes", "x_likes"),
            ("Questions", "questions_count"),
            ("Upvotes", "upvotes_count"),
            ("Downvotes", "downvotes_count"),
        ):
            value = metrics.get(key)
            if value is not None:
                lines.append(f"{label}: {value}")
        return lines

    def _alphaxiv_ai_detection_lines(self, payload: dict[str, Any] | None) -> list[str]:
        if not isinstance(payload, dict):
            return []
        lines = []
        for label, key in (
            ("State", "state"),
            ("Prediction", "predictionShort"),
            ("Headline", "headline"),
            ("Fraction AI", "fractionAi"),
            ("Fraction AI-assisted", "fractionAiAssisted"),
            ("Fraction human", "fractionHuman"),
        ):
            value = payload.get(key)
            if value is not None:
                lines.append(f"{label}: {value}")
        return lines

    def _alphaxiv_resource_lines(self, bundle: AlphaXivPaperBundle) -> list[str]:
        lines: list[str] = []
        if bundle.github_url:
            lines.append(f"GitHub repository: {bundle.github_url}")
        for resource in bundle.resources:
            if not isinstance(resource, dict):
                text = normalize_web_text(resource)
                if text:
                    lines.append(text)
                continue
            label = str(
                resource.get("label") or resource.get("title") or resource.get("name") or ""
            ).strip()
            url = str(resource.get("url") or resource.get("href") or "").strip()
            if label and url:
                lines.append(f"{label}: {url}")
            elif url:
                lines.append(url)
            elif label:
                lines.append(label)
        return lines

    def _alphaxiv_text_list(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [text for item in value if (text := normalize_web_text(item))]
        text = normalize_web_text(value)
        return [text] if text else []

    def _json_dump(self, payload: Any) -> str:
        return json.dumps(payload, indent=2, sort_keys=True, default=self._json_default) + "\n"

    def _json_default(self, value: Any) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    @staticmethod
    def _format_raw_document_sync_log(*, source_name: str, result: SyncWriteResult) -> str:
        if result.created:
            action = "created"
        elif result.updated:
            action = "updated"
        else:
            action = "synced"
        date_label = (
            result.published_at.date().isoformat() if result.published_at else "unknown date"
        )
        return f'{source_name}: {action} {result.kind} "{result.title}" ({date_label}).'

    def _sync_gmail_source(
        self,
        source: VaultSourceDefinition,
        connector: GmailConnector | GmailImapConnector,
        run,
        *,
        max_items: int,
    ) -> tuple[int, dict[str, int], int, int, int]:
        config = source.config_json if isinstance(source.config_json, dict) else {}
        senders = [
            str(sender).strip() for sender in (config.get("senders") or []) if str(sender).strip()
        ]
        labels = [
            str(label).strip() for label in (config.get("labels") or []) if str(label).strip()
        ]
        raw_query = str(config.get("raw_query") or "").strip() or None
        messages = connector.list_newsletters(
            senders=senders or None,
            labels=labels or None,
            raw_query=raw_query,
            max_results=max_items,
        )

        synced = 0
        created_count = 0
        updated_count = 0
        decomposition_count = 0
        counts_by_kind: dict[str, int] = {}
        self._raise_if_stop_requested(run=run, source=source)
        for message in messages:
            self._raise_if_stop_requested(run=run, source=source)
            child_entries = (
                self._extract_newsletter_entries(source, message)
                if source.decomposition_mode == "newsletter_entries"
                else []
            )
            parent_visibility = "hidden" if child_entries else "visible"
            parent_body = self._render_newsletter_body(source, message, child_entries)
            parent_result = self._upsert_raw_document(
                source=source,
                kind="newsletter",
                stable_key=message.message_id or message.permalink,
                external_key=message.message_id or message.permalink,
                title=message.subject.strip() or source.name,
                body=parent_body,
                source_url=message.permalink.strip() or None,
                source_name=source.name,
                authors=[message.sender],
                published_at=message.published_at,
                tags=source.tags,
                asset_map={DEFAULT_HTML_FILENAME: message.html_body}
                if message.html_body.strip()
                else {},
                doc_role="primary",
                parent_id=None,
                index_visibility=parent_visibility,
            )
            counts_by_kind[parent_result.kind] = counts_by_kind.get(parent_result.kind, 0) + 1
            created_count += 1 if parent_result.created else 0
            updated_count += 1 if parent_result.updated else 0
            synced += 1

            current_child_external_keys: set[str] = set()
            for entry in child_entries:
                result = self._upsert_raw_document(
                    source=source,
                    kind=entry.kind,
                    stable_key=entry.external_key,
                    external_key=entry.external_key,
                    title=entry.title,
                    body=entry.body,
                    source_url=entry.link,
                    source_name=source.name,
                    authors=[message.sender],
                    published_at=message.published_at,
                    tags=[*source.tags, entry.kind],
                    asset_map=entry.asset_map,
                    doc_role="derived",
                    parent_id=parent_result.doc_id,
                    index_visibility="visible",
                )
                current_child_external_keys.add(entry.external_key)
                counts_by_kind[result.kind] = counts_by_kind.get(result.kind, 0) + 1
                created_count += 1 if result.created else 0
                updated_count += 1 if result.updated else 0
                synced += 1
                decomposition_count += 1

            self._hide_stale_newsletter_children(
                source=source,
                parent_id=parent_result.doc_id,
                active_external_keys=current_child_external_keys,
            )
            self.runs.log(run, f"{source.name}: fetched newsletter issue {parent_result.doc_id}.")

        self._raise_if_stop_requested(run=run, source=source)
        return synced, counts_by_kind, created_count, updated_count, decomposition_count

    def _extract_website_entry(self, entry: WebsiteEntry) -> ExtractedContent:
        try:
            return self.extractor.extract_from_url(entry.link)
        except Exception:
            return ExtractedContent(
                title=entry.title,
                cleaned_text=entry.summary or entry.title,
                outbound_links=[],
                published_at=entry.published_at,
                mime_type=None,
                extraction_confidence=0.2,
                raw_payload={},
            )

    def _discover_website_entries(
        self,
        source: VaultSourceDefinition,
        *,
        max_entries: int | None = None,
        run=None,
    ) -> list[WebsiteEntry]:
        effective_max_entries = max_entries if max_entries is not None else source.max_items
        if source.custom_pipeline_id == "alphaxiv-paper":
            return self._discover_alphaxiv_entries(source, max_entries=effective_max_entries, run=run)
        discovery_mode = str((source.config_json or {}).get("discovery_mode") or "").strip().lower()
        if discovery_mode == "website_index":
            return self._discover_entries_from_website_index(
                source, max_entries=effective_max_entries, run=run
            )
        return self._discover_entries_from_feed(source, max_entries=effective_max_entries, run=run)

    def _discover_alphaxiv_entries(
        self,
        source: VaultSourceDefinition,
        *,
        max_entries: int,
        run=None,
    ) -> list[WebsiteEntry]:
        if max_entries <= 0:
            return []

        search_settings = self._load_alphaxiv_search_settings()
        page_size = min(ALPHAXIV_DISCOVERY_PAGE_SIZE, max_entries)
        max_pages = max(1, ((max_entries - 1) // page_size) + 3)
        entries: list[WebsiteEntry] = []
        seen: set[str] = set()

        for page_num in range(1, max_pages + 1):
            if run is not None:
                self._raise_if_stop_requested(run=run, source=source)
            page = self.alphaxiv.fetch_feed_page(
                page_num=page_num,
                page_size=page_size,
                sort=search_settings.sort,
                interval=search_settings.interval,
                topics=search_settings.topics,
                organizations=search_settings.organizations,
                source=search_settings.source,
            )
            if page.raw_paper_count == 0:
                break

            for paper in page.papers:
                link = normalize_url(paper.abs_url)
                if not link or link in seen:
                    continue
                seen.add(link)
                entries.append(
                    WebsiteEntry(
                        link=link,
                        title=paper.title or link,
                        published_at=paper.published_at,
                        summary=paper.summary,
                    )
                )
                if len(entries) >= max_entries:
                    return entries

            if page.raw_paper_count < page.page_size:
                break

        return entries

    def _load_alphaxiv_search_settings(self) -> AlphaXivSearchSettings:
        profile = load_profile_snapshot()
        raw_settings = getattr(profile, "alphaxiv_search_settings", None)
        if hasattr(raw_settings, "model_dump"):
            payload = raw_settings.model_dump()
        elif isinstance(raw_settings, dict):
            payload = raw_settings
        else:
            payload = {
                "topics": getattr(raw_settings, "topics", None),
                "organizations": getattr(raw_settings, "organizations", None),
                "sort": getattr(raw_settings, "sort", None),
                "interval": getattr(raw_settings, "interval", None),
                "source": getattr(raw_settings, "source", None),
            }
        return AlphaXivSearchSettings.model_validate(payload)

    def _discover_entries_from_feed(
        self,
        source: VaultSourceDefinition,
        *,
        max_entries: int,
        run=None,
    ) -> list[WebsiteEntry]:
        if run is not None:
            self._raise_if_stop_requested(run=run, source=source)
        locator = str(source.url or "").strip()
        if not locator:
            raise RuntimeError(f"{source.name} is missing a feed URL.")

        response = fetch_safe_response(locator, timeout=WEBSITE_TIMEOUT_SECONDS)
        response.raise_for_status()
        response_headers = dict(response.headers)
        response_headers["content-location"] = str(response.url)
        parsed = feedparser.parse(response.content, response_headers=response_headers)
        entries: list[WebsiteEntry] = []
        seen: set[str] = set()
        for entry in list(getattr(parsed, "entries", []) or []):
            link = normalize_url(str(getattr(entry, "link", "") or "").strip())
            if not link or link in seen:
                continue
            if not self._website_link_allowed(source, link):
                continue
            seen.add(link)
            published_at = parse_datetime(
                getattr(entry, "published", None)
                or getattr(entry, "updated", None)
                or getattr(entry, "created", None)
            )
            title = str(getattr(entry, "title", None) or link).strip() or link
            summary = normalize_web_text(
                getattr(entry, "summary", None) or getattr(entry, "description", None)
            )
            entries.append(
                WebsiteEntry(
                    link=link,
                    title=title,
                    published_at=published_at,
                    summary=summary,
                )
            )
            if len(entries) >= max_entries:
                break
        return entries

    def _discover_entries_from_website_index(
        self,
        source: VaultSourceDefinition,
        *,
        max_entries: int,
        run=None,
    ) -> list[WebsiteEntry]:
        if run is not None:
            self._raise_if_stop_requested(run=run, source=source)
        index_url = self._website_index_url(source)
        if not index_url:
            raise RuntimeError(f"{source.name} is missing a website index URL.")

        response = fetch_safe_response(index_url, timeout=WEBSITE_TIMEOUT_SECONDS)
        response.raise_for_status()
        base_url = str(response.url)
        soup = BeautifulSoup(response.text, "html.parser")
        entries: list[WebsiteEntry] = []
        entry_indexes: dict[str, int] = {}
        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href") or "").strip()
            if (
                not href
                or href.startswith("#")
                or href.lower().startswith(("javascript:", "mailto:", "tel:"))
            ):
                continue
            link = normalize_url(urljoin(base_url, href))
            if not self._website_link_allowed(source, link):
                continue
            if link in entry_indexes:
                continue
            title = self._website_index_entry_title(anchor, link)
            entry_indexes[link] = len(entries)
            entries.append(WebsiteEntry(link=link, title=title))
            if len(entries) >= max_entries:
                break

        for entry in self._discover_structured_website_index_entries(
            source,
            html=response.text,
            base_url=base_url,
        ):
            existing_index = entry_indexes.get(entry.link)
            if existing_index is not None:
                entries[existing_index] = self._merge_website_entry(entries[existing_index], entry)
                continue
            if len(entries) >= max_entries:
                continue
            entry_indexes[entry.link] = len(entries)
            entries.append(entry)
        return entries

    @staticmethod
    def _merge_website_entry(existing: WebsiteEntry, incoming: WebsiteEntry) -> WebsiteEntry:
        return WebsiteEntry(
            link=existing.link,
            title=incoming.title or existing.title,
            published_at=incoming.published_at or existing.published_at,
            summary=incoming.summary or existing.summary,
        )

    def _discover_structured_website_index_entries(
        self,
        source: VaultSourceDefinition,
        *,
        html: str,
        base_url: str,
    ) -> list[WebsiteEntry]:
        config = source.config_json if isinstance(source.config_json, dict) else {}
        parser_name = str(config.get("script_entry_parser") or "").strip().lower()
        if parser_name == "mistral_news_posts":
            return self._discover_mistral_news_script_entries(
                source,
                html=html,
                base_url=base_url,
            )
        return []

    def _discover_mistral_news_script_entries(
        self,
        source: VaultSourceDefinition,
        *,
        html: str,
        base_url: str,
    ) -> list[WebsiteEntry]:
        config = source.config_json if isinstance(source.config_json, dict) else {}
        required_categories = {
            str(category or "").strip().lower()
            for category in (config.get("required_categories") or [])
            if str(category or "").strip()
        }
        entries: list[WebsiteEntry] = []
        seen: set[str] = set()
        for match in MISTRAL_NEWS_SCRIPT_ENTRY_RE.finditer(html):
            slug = self._decode_script_json_fragment(match.group("slug"))
            category = self._decode_script_json_fragment(match.group("category"))
            if required_categories and category.lower() not in required_categories:
                continue

            link = normalize_url(urljoin(base_url, f"/news/{slug}"))
            if not self._website_link_allowed(source, link):
                continue
            if link in seen:
                continue
            seen.add(link)

            title = self._decode_script_json_fragment(match.group("title"))
            summary = None
            raw_description = match.group("description")
            if raw_description is not None:
                summary = normalize_web_text(self._decode_script_json_fragment(raw_description))

            entries.append(
                WebsiteEntry(
                    link=link,
                    title=title,
                    published_at=parse_datetime(match.group("date")),
                    summary=summary,
                )
            )
        return entries

    def _decode_script_json_fragment(self, value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        try:
            return str(json.loads(f'"{raw}"'))
        except json.JSONDecodeError:
            return raw.replace('\\"', '"').replace("\\/", "/")

    def _website_index_url(self, source: VaultSourceDefinition) -> str:
        config = source.config_json if isinstance(source.config_json, dict) else {}
        for candidate in (
            config.get("website_index_url"),
            source.url,
            config.get("website_url"),
        ):
            value = str(candidate or "").strip()
            if value:
                return value
        return ""

    def _website_link_allowed(self, source: VaultSourceDefinition, link: str) -> bool:
        config = source.config_json if isinstance(source.config_json, dict) else {}
        parsed = urlparse(link)
        hostname = (parsed.hostname or "").lower()
        allowed_hosts = {(urlparse(self._website_index_url(source)).hostname or "").lower()}
        for raw_host in config.get("allowed_hosts") or []:
            host = str(raw_host or "").strip().lower()
            if host:
                allowed_hosts.add(host)
        allowed_hosts.discard("")
        if allowed_hosts and hostname not in allowed_hosts:
            return False

        raw_prefixes = config.get("article_path_prefixes") or []
        prefixes = tuple(
            prefix if prefix.startswith("/") else f"/{prefix}"
            for prefix in (
                str(raw_prefix or "").strip() for raw_prefix in raw_prefixes if raw_prefix
            )
            if prefix
        )
        path = parsed.path or "/"
        if prefixes and not any(path.startswith(prefix) for prefix in prefixes):
            return False

        exclude_patterns = tuple(
            str(pattern or "").strip()
            for pattern in (config.get("exclude_patterns") or [])
            if str(pattern or "").strip()
        )
        candidate = path if not parsed.query else f"{path}?{parsed.query}"
        return not any(
            re.search(pattern, candidate, flags=re.IGNORECASE) for pattern in exclude_patterns
        )

    def _website_index_entry_title(self, anchor, link: str) -> str:
        heading = anchor.find(["h1", "h2", "h3", "h4", "h5", "h6"])
        if heading:
            heading_text = re.sub(r"\s+", " ", heading.get_text(" ", strip=True))
            if heading_text:
                return heading_text

        for candidate in (
            anchor.get_text(" ", strip=True),
            anchor.get("aria-label"),
            anchor.get("title"),
        ):
            text = re.sub(r"\s+", " ", str(candidate or "").strip())
            if text:
                return text

        path_segment = urlparse(link).path.rstrip("/").split("/")[-1]
        fallback = path_segment.replace("-", " ").replace("_", " ").strip()
        return fallback.title() if fallback else "Untitled item"

    def _build_gmail_connector(self) -> GmailConnector | GmailImapConnector | None:
        if self.settings.gmail_ingest_email and self.settings.gmail_ingest_app_password:
            return GmailImapConnector(
                email_address=self.settings.gmail_ingest_email,
                app_password=self.settings.gmail_ingest_app_password,
            )
        if self.settings.gmail_ingest_access_token:
            return GmailConnector(self.settings.gmail_ingest_access_token)
        payload = self._load_stored_gmail_payload()
        if not payload:
            return None
        auth_mode = str(payload.get("auth_mode") or "").strip().lower()
        if auth_mode == "app_password" or payload.get("app_password"):
            email_address = str(payload.get("email") or "").strip()
            app_password = str(payload.get("app_password") or "").strip()
            if email_address and app_password:
                return GmailImapConnector(
                    email_address=email_address,
                    app_password=app_password,
                )
            return None
        access_token = str(payload.get("access_token") or "").strip()
        if access_token:
            return GmailConnector(access_token)
        return None

    def _load_stored_gmail_payload(self) -> dict | None:
        db = get_session_factory()()
        try:
            return ConnectionService(db).get_valid_gmail_payload()
        except Exception:
            return None
        finally:
            db.close()

    def _resolve_raw_kind(
        self,
        *,
        source: VaultSourceDefinition,
        source_url: str | None,
        title: str,
    ) -> str:
        if source.classification_mode == "written_content_auto":
            return classify_written_kind(
                source_url=source_url,
                title=title,
                source_name=source.name,
                source_id=source.id,
                default_kind="article",
            )
        return source.raw_kind

    def _render_newsletter_body(
        self,
        source: VaultSourceDefinition,
        message: NewsletterMessage,
        entries: list[NewsletterEntry],
    ) -> str:
        parsed_sections = self._parse_newsletter_sections(source, message)
        if parsed_sections:
            return self._render_structured_newsletter_body(message, parsed_sections)

        lines = [
            f"# {message.subject.strip() or 'Newsletter'}",
            "",
            f"Sender: {message.sender}",
        ]
        if message.published_at is not None:
            lines.append(f"Published At: {message.published_at.isoformat()}")
        body = message.text_body.strip()
        if body:
            lines.extend(["", "## Email Body", "", body])
        if entries:
            lines.extend(["", "## Extracted Entries", ""])
            for entry in entries:
                label = f"{entry.title} ({entry.kind})"
                if entry.link:
                    lines.append(f"- [{label}]({entry.link})")
                else:
                    lines.append(f"- {label}")
        if message.outbound_links:
            lines.extend(["", "## Relevant Links", ""])
            lines.extend(f"- {link}" for link in message.outbound_links[:50])
        return "\n".join(lines).strip() + "\n"

    def _render_structured_newsletter_body(
        self,
        message: NewsletterMessage,
        sections: list[ParsedNewsletterSection],
    ) -> str:
        lines = [f"# {message.subject.strip() or 'Newsletter'}"]
        for section in sections:
            if not section.stories:
                continue
            lines.extend(["", f"## {section.title}"])
            for story in section.stories:
                story_title = f"[{story.title}]({story.link})" if story.link else story.title
                lines.extend(["", f"### {story_title}"])
                if story.summary:
                    lines.extend(["", story.summary])
        return "\n".join(lines).strip() + "\n"

    def _render_newsletter_entry_body(
        self,
        message: NewsletterMessage,
        *,
        title: str,
        link: str | None,
        context: str,
        ordinal: int,
    ) -> str:
        lines = [
            f"# {title}",
            "",
            f"Source newsletter: {message.subject.strip() or 'Newsletter'}",
            f"Sender: {message.sender}",
        ]
        if message.published_at is not None:
            lines.append(f"Published At: {message.published_at.isoformat()}")
        if link:
            lines.append(f"Canonical URL: {link}")
        if context:
            lines.extend(["", "## Newsletter Context", "", context])
        return "\n".join(lines).strip() + "\n"

    def _extract_newsletter_entries(
        self, source: VaultSourceDefinition, message: NewsletterMessage
    ) -> list[NewsletterEntry]:
        parsed_sections = self._parse_newsletter_sections(source, message)
        if parsed_sections:
            entries = self._build_parsed_newsletter_entries(source, message, parsed_sections)
            if entries:
                return entries

        entries: list[NewsletterEntry] = []
        seen: set[str] = set()
        soup = BeautifulSoup(message.html_body or "", "html.parser")
        anchors = soup.find_all("a", href=True)
        for anchor in anchors:
            href = normalize_url(str(anchor.get("href") or "").strip())
            title = re.sub(r"\s+", " ", anchor.get_text(" ", strip=True))
            surrounding_text = (
                re.sub(r"\s+", " ", anchor.parent.get_text(" ", strip=True))
                if anchor.parent
                else ""
            )
            if not href.startswith(("http://", "https://")):
                continue
            if href == normalize_url(message.permalink):
                continue
            if any(noise in (title or "").lower() for noise in NEWSLETTER_NOISE_TEXT):
                continue
            if any(noise in surrounding_text.lower() for noise in NEWSLETTER_NOISE_TEXT):
                continue
            if href in seen:
                continue
            seen.add(href)
            resolved_title = title or self._fallback_title_from_url(href)
            if not resolved_title:
                continue
            kind = self._resolve_raw_kind(source=source, source_url=href, title=resolved_title)
            context = surrounding_text.strip()
            body = self._render_newsletter_entry_body(
                message,
                title=resolved_title,
                link=href,
                context=context,
                ordinal=len(entries) + 1,
            )
            external_key = f"{message.message_id or message.permalink}::link::{href}"
            entries.append(
                NewsletterEntry(
                    title=resolved_title,
                    link=href,
                    body=body,
                    external_key=external_key,
                    kind=kind,
                    asset_map={},
                )
            )
            if len(entries) >= NEWSLETTER_ENTRY_LIMIT:
                break

        if entries:
            return entries

        for index, raw_link in enumerate(message.outbound_links[:NEWSLETTER_ENTRY_LIMIT], start=1):
            href = normalize_url(raw_link)
            if href in seen:
                continue
            seen.add(href)
            title = self._fallback_title_from_url(href)
            kind = self._resolve_raw_kind(source=source, source_url=href, title=title)
            entries.append(
                NewsletterEntry(
                    title=title,
                    link=href,
                    body=self._render_newsletter_entry_body(
                        message,
                        title=title,
                        link=href,
                        context="Captured from the newsletter outbound link list.",
                        ordinal=index,
                    ),
                    external_key=f"{message.message_id or message.permalink}::link::{href}",
                    kind=kind,
                    asset_map={},
                )
            )
        return entries

    def _parse_newsletter_sections(
        self,
        source: VaultSourceDefinition,
        message: NewsletterMessage,
    ) -> list[ParsedNewsletterSection]:
        if source.id == "tldr-email":
            return self._parse_tldr_newsletter_sections(message)
        if source.id == "medium-email":
            return self._parse_medium_newsletter_sections(message)
        return []

    def _parse_tldr_newsletter_sections(
        self, message: NewsletterMessage
    ) -> list[ParsedNewsletterSection]:
        if not message.html_body.strip():
            return []

        soup = BeautifulSoup(message.html_body, "html.parser")
        sections: list[ParsedNewsletterSection] = []
        for heading in soup.find_all("h1"):
            section_title = normalize_web_text(heading)
            if not section_title or TLDR_ISSUE_HEADING_RE.fullmatch(section_title):
                continue
            heading_table = heading.find_parent("table")
            if heading_table is None:
                continue
            container = heading_table.find_next_sibling("table")
            if container is None:
                continue
            stories = self._extract_tldr_section_stories(section_title, container)
            if stories:
                sections.append(ParsedNewsletterSection(title=section_title, stories=stories))
        return sections

    def _extract_tldr_section_stories(
        self,
        section_title: str,
        container: BeautifulSoup,
    ) -> list[ParsedNewsletterStory]:
        stories: list[ParsedNewsletterStory] = []
        seen: set[str] = set()
        for candidate in container.find_all("table"):
            anchor = candidate.find("a", href=True)
            if anchor is None:
                continue
            raw_title = normalize_web_text(anchor.get_text(" ", strip=True))
            if not raw_title:
                continue
            title = TLDR_READ_TIME_RE.sub("", raw_title).strip()
            link = normalize_url(str(anchor.get("href") or "").strip()) or None
            summary = self._extract_story_summary(candidate, raw_title)
            if self._is_noise_story(title=title, link=link, summary=summary):
                continue
            dedupe_key = link or title.casefold()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            stories.append(
                ParsedNewsletterStory(
                    section=section_title,
                    title=title,
                    link=link,
                    summary=summary,
                )
            )
        return stories

    def _extract_story_summary(self, candidate, raw_title: str) -> str:
        text_block = candidate.find(class_="text-block") or candidate
        full_text = normalize_web_text(text_block) or ""
        if not full_text:
            return ""
        if full_text.startswith(raw_title):
            return full_text[len(raw_title) :].strip(" -:\n\t")
        return full_text.replace(raw_title, "", 1).strip(" -:\n\t")

    def _is_noise_story(self, *, title: str, link: str | None, summary: str) -> bool:
        lowered_title = title.casefold()
        lowered_summary = summary.casefold()
        if any(noise in lowered_title for noise in NEWSLETTER_NOISE_TEXT):
            return True
        if any(noise in lowered_summary for noise in NEWSLETTER_NOISE_TEXT):
            return True
        if "(sponsor)" in lowered_title or "tldr is hiring" in lowered_title:
            return True
        hostname = (urlparse(link).hostname or "").casefold() if link else ""
        return hostname in TLDR_AD_HOSTS

    def _parse_medium_newsletter_sections(
        self, message: NewsletterMessage
    ) -> list[ParsedNewsletterSection]:
        if not message.html_body.strip():
            return []

        soup = BeautifulSoup(message.html_body, "html.parser")
        section_title = "Today's highlights"
        for candidate in soup.find_all(string=True):
            text = normalize_web_text(candidate)
            if text and MEDIUM_HIGHLIGHTS_HEADING_RE.fullmatch(text):
                section_title = text
                break

        stories = self._extract_medium_stories(section_title, soup)
        if not stories:
            return []
        return [ParsedNewsletterSection(title=section_title, stories=stories)]

    def _extract_medium_stories(
        self,
        section_title: str,
        soup: BeautifulSoup,
    ) -> list[ParsedNewsletterStory]:
        stories: list[ParsedNewsletterStory] = []
        seen: set[str] = set()
        for anchor in soup.find_all("a", href=True):
            if anchor.find("h2") is None:
                continue

            link = normalize_url(str(anchor.get("href") or "").strip())
            if not self._is_medium_story_link(link):
                continue

            container = self._find_medium_story_container(anchor) or anchor.parent
            title = self._resolve_medium_story_title(anchor=anchor, container=container, link=link)
            if not title:
                continue

            dedupe_key = link or title.casefold()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            byline = self._extract_medium_story_byline(container, anchor)
            subtitle = normalize_web_text(anchor.find("h3")) or ""
            read_time = self._extract_medium_story_read_time(container)
            claps = self._extract_medium_story_metric(container, label="Claps")
            responses = self._extract_medium_story_metric(container, label="Responses")
            summary = self._render_medium_story_summary(
                byline=byline,
                subtitle=subtitle,
                read_time=read_time,
                claps=claps,
                responses=responses,
            )

            stories.append(
                ParsedNewsletterStory(
                    section=section_title,
                    title=title,
                    link=link,
                    summary=summary,
                )
            )
        return stories

    @staticmethod
    def _is_medium_story_link(link: str | None) -> bool:
        if not link:
            return False

        parsed = urlparse(link)
        hostname = (parsed.hostname or "").casefold()
        if hostname not in MEDIUM_HOSTS:
            return False

        path = parsed.path or "/"
        if path in {"/", "/download-app", "/plans"}:
            return False
        return not path.startswith(("/me/", "/jobs-at-medium/", "/policy/"))

    def _resolve_medium_story_title(self, *, anchor, container, link: str) -> str:
        title = normalize_web_text(anchor.find("h2")) or ""
        fallback = ""
        search_root = container if container is not None else anchor
        for candidate in search_root.find_all("a", href=True):
            if candidate is anchor:
                continue
            if normalize_url(str(candidate.get("href") or "").strip()) != link:
                continue
            image = candidate.find("img", alt=True)
            alt = normalize_web_text(image.get("alt")) if image is not None else None
            if alt and alt.casefold() not in {"member-only content", "claps", "responses"}:
                fallback = alt
                break

        if fallback and (not title or title.endswith(("…", "...")) or len(fallback) > len(title)):
            return fallback
        return title or fallback

    def _find_medium_story_container(self, anchor):
        for ancestor in anchor.parents:
            if getattr(ancestor, "name", None) != "div":
                continue
            if len(ancestor.find_all("h2")) != 1:
                continue
            if not self._extract_medium_story_read_time(ancestor):
                continue
            return ancestor
        return None

    def _extract_medium_story_byline(self, container, story_anchor) -> str:
        if container is None:
            return ""

        link_texts: list[str] = []
        for candidate in container.find_all("a", href=True):
            if candidate is story_anchor:
                break
            text = normalize_web_text(candidate)
            if not text or text in link_texts:
                continue
            link_texts.append(text)

        if not link_texts:
            return ""
        if len(link_texts) >= 2:
            return f"{link_texts[0]} in {link_texts[1]}"
        return link_texts[0]

    @staticmethod
    def _extract_medium_story_read_time(container) -> str:
        if container is None:
            return ""

        for raw_text in container.stripped_strings:
            text = re.sub(r"\s+", " ", str(raw_text or "").strip())
            if MEDIUM_READ_TIME_RE.fullmatch(text):
                return text
        return ""

    def _extract_medium_story_metric(self, container, *, label: str) -> str:
        if container is None:
            return ""

        marker = container.find(
            "img", alt=lambda value: str(value or "").strip().casefold() == label.casefold()
        )
        if marker is None:
            return ""

        for candidate in marker.next_elements:
            if candidate is marker:
                continue
            if getattr(candidate, "name", None) == "img":
                break
            text = normalize_web_text(candidate)
            if text:
                return text
        return ""

    @staticmethod
    def _render_medium_story_summary(
        *,
        byline: str,
        subtitle: str,
        read_time: str,
        claps: str,
        responses: str,
    ) -> str:
        details: list[str] = []
        if byline:
            details.append(byline)
        if read_time:
            details.append(read_time)
        if claps:
            details.append(f"{claps} {'clap' if claps == '1' else 'claps'}")
        if responses:
            details.append(f"{responses} {'response' if responses == '1' else 'responses'}")

        lines: list[str] = []
        if details:
            lines.append(f"> {' · '.join(details)}")
        if subtitle:
            if lines:
                lines.append("")
            lines.append(subtitle)
        return "\n".join(lines).strip()

    def _build_parsed_newsletter_entries(
        self,
        source: VaultSourceDefinition,
        message: NewsletterMessage,
        sections: list[ParsedNewsletterSection],
    ) -> list[NewsletterEntry]:
        entries: list[NewsletterEntry] = []
        seen: set[str] = set()
        stories = [story for section in sections for story in section.stories]
        for ordinal, story in enumerate(stories[:NEWSLETTER_ENTRY_LIMIT], start=1):
            if not story.link:
                continue
            href = normalize_url(story.link)
            if href in seen:
                continue
            seen.add(href)
            kind = self._resolve_raw_kind(source=source, source_url=href, title=story.title)
            context_parts = [f"Section: {story.section}"]
            if story.summary:
                context_parts.extend(["", story.summary])
            body = self._render_newsletter_entry_body(
                message,
                title=story.title,
                link=href,
                context="\n".join(context_parts).strip(),
                ordinal=ordinal,
            )
            entries.append(
                NewsletterEntry(
                    title=story.title,
                    link=href,
                    body=body,
                    external_key=f"{message.message_id or message.permalink}::link::{href}",
                    kind=kind,
                    asset_map={},
                )
            )
        return entries

    @staticmethod
    def _fallback_title_from_url(link: str) -> str:
        path_segment = urlparse(link).path.rstrip("/").split("/")[-1]
        fallback = path_segment.replace("-", " ").replace("_", " ").strip()
        return fallback.title() if fallback else "Untitled linked item"

    def _hide_stale_newsletter_children(
        self,
        *,
        source: VaultSourceDefinition,
        parent_id: str,
        active_external_keys: set[str],
    ) -> None:
        for document in self.store.list_raw_documents():
            fm = document.frontmatter
            if fm.source_id != source.id or fm.parent_id != parent_id or fm.doc_role != "derived":
                continue
            if fm.external_key in active_external_keys:
                continue
            updated = fm.model_copy(update={"status": "archived", "index_visibility": "hidden"})
            self.store.write_raw_document(
                kind=fm.kind,
                doc_id=fm.id,
                frontmatter=updated,
                body=document.body,
            )

    def _upsert_raw_document(
        self,
        *,
        source: VaultSourceDefinition,
        kind: str,
        stable_key: str,
        external_key: str,
        title: str,
        body: str,
        source_url: str | None,
        source_name: str,
        authors: list[str],
        published_at: datetime | None,
        tags: list[str],
        asset_map: RawAssetMap,
        doc_role: str,
        parent_id: str | None,
        index_visibility: str,
        short_summary: str | None = None,
        hash_title: str | None = None,
        hash_body: str | None = None,
    ) -> SyncWriteResult:
        existing_doc = self.store.find_raw_document(source_id=source.id, external_key=external_key)
        existing_frontmatter = existing_doc.frontmatter if existing_doc else None
        existing_body = existing_doc.body if existing_doc else ""
        folder_kind = existing_frontmatter.kind if existing_frontmatter else kind
        doc_id = (
            existing_frontmatter.id
            if existing_frontmatter
            else readable_doc_id(
                stable_key=stable_key,
                title=title,
                source_slug=source.id,
                published_at=published_at,
            )
        )

        folder = self.store.raw_dir / folder_kind / doc_id
        folder.mkdir(parents=True, exist_ok=True)
        for filename, content in asset_map.items():
            if isinstance(content, bytes):
                self.store.write_bytes(folder / filename, content)
            else:
                self.store.write_text(folder / filename, content)

        asset_paths = sorted(
            str(path.relative_to(folder))
            for path in folder.rglob("*")
            if path.is_file() and path.name != "source.md"
        )
        merged_authors = merge_unique_strings(
            *(existing_frontmatter.authors if existing_frontmatter else []), *authors
        )
        merged_tags = merge_unique_strings(
            *(existing_frontmatter.tags if existing_frontmatter else []), *tags
        )
        raw_title = title.strip() or (existing_frontmatter.title if existing_frontmatter else doc_id)
        resolved_title = normalize_whitespace(
            self.extractor.normalize_title(
                raw_title,
                url=source_url or (existing_frontmatter.source_url if existing_frontmatter else None),
            )
            or raw_title
        )
        resolved_source_url = source_url or (
            existing_frontmatter.source_url if existing_frontmatter else None
        )
        resolved_canonical_url = source_url or (
            existing_frontmatter.canonical_url if existing_frontmatter else None
        )
        resolved_published_at = published_at or (
            existing_frontmatter.published_at if existing_frontmatter else None
        )
        next_identity_hash = document_identity_hash(
            source_id=source.id,
            external_key=external_key,
            canonical_url=resolved_canonical_url,
            fallback_key=stable_key,
        )
        resolved_short_summary = (
            str(short_summary).strip()
            if short_summary is not None and str(short_summary).strip()
            else (existing_frontmatter.short_summary if existing_frontmatter else None)
        )
        status = existing_frontmatter.status if existing_frontmatter else "active"
        next_hash = content_hash(
            hash_title if hash_title is not None else resolved_title,
            hash_body if hash_body is not None else body,
        )
        changed = not bool(
            existing_frontmatter
            and existing_frontmatter.title == resolved_title
            and existing_frontmatter.source_url == resolved_source_url
            and existing_frontmatter.source_name == source_name
            and existing_frontmatter.authors == merged_authors
            and existing_frontmatter.published_at == resolved_published_at
            and existing_frontmatter.tags == merged_tags
            and existing_frontmatter.status == status
            and existing_frontmatter.content_hash == next_hash
            and existing_frontmatter.identity_hash == next_identity_hash
            and existing_frontmatter.asset_paths == asset_paths
            and existing_frontmatter.parent_id == parent_id
            and existing_frontmatter.index_visibility == index_visibility
            and existing_frontmatter.doc_role == doc_role
            and existing_frontmatter.short_summary == resolved_short_summary
            and existing_body.strip() == body.strip()
        )
        frontmatter = RawDocumentFrontmatter(
            id=doc_id,
            kind=folder_kind,
            title=resolved_title,
            source_url=resolved_source_url,
            source_name=source_name,
            authors=merged_authors,
            published_at=resolved_published_at,
            ingested_at=existing_frontmatter.ingested_at if existing_frontmatter else utcnow(),
            content_hash=next_hash,
            identity_hash=next_identity_hash,
            tags=merged_tags,
            status=status,
            asset_paths=asset_paths,
            source_id=source.id,
            source_pipeline_id=source.custom_pipeline_id,
            external_key=external_key,
            canonical_url=resolved_canonical_url,
            doc_role=doc_role,
            parent_id=parent_id,
            index_visibility=index_visibility,
            fetched_at=utcnow(),
            short_summary=resolved_short_summary,
            lightweight_enrichment_status=(
                "pending"
                if changed
                else (
                    existing_frontmatter.lightweight_enrichment_status
                    if existing_frontmatter
                    else "pending"
                )
            ),
            lightweight_enriched_at=None
            if changed
            else (existing_frontmatter.lightweight_enriched_at if existing_frontmatter else None),
            lightweight_enrichment_model=None
            if changed
            else (
                existing_frontmatter.lightweight_enrichment_model if existing_frontmatter else None
            ),
            lightweight_enrichment_input_hash=None
            if changed
            else (
                existing_frontmatter.lightweight_enrichment_input_hash
                if existing_frontmatter
                else None
            ),
            lightweight_enrichment_error=None
            if changed
            else (
                existing_frontmatter.lightweight_enrichment_error if existing_frontmatter else None
            ),
        )
        self.store.write_raw_document(
            kind=folder_kind,
            doc_id=doc_id,
            frontmatter=frontmatter,
            body=body,
        )
        return SyncWriteResult(
            doc_id=doc_id,
            kind=folder_kind,
            title=resolved_title,
            published_at=resolved_published_at,
            created=existing_frontmatter is None,
            updated=existing_frontmatter is not None and changed,
        )


def merge_unique_strings(*values: str) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        merged.append(value)
    return merged


def normalize_url(url: str) -> str:
    candidate = unwrap_tracking_url(str(url or "").strip())
    parsed = urlparse(candidate)
    hostname = (parsed.hostname or "").lower()
    scheme = parsed.scheme.lower()
    if scheme == "http" and hostname not in {"localhost", "127.0.0.1", "0.0.0.0"}:
        scheme = "https"
    query = urlencode(
        [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if not key.startswith(TRACKING_PREFIXES)
        ]
    )
    normalized_path = parsed.path or "/"
    return urlunparse(
        (scheme, parsed.netloc.lower(), normalized_path.rstrip("/") or "/", "", query, "")
    )


def unwrap_tracking_url(url: str) -> str:
    candidate = str(url or "").strip()
    for _ in range(3):
        parsed = urlparse(candidate)
        hostname = (parsed.hostname or "").lower()
        if hostname != "tracking.tldrnewsletter.com":
            return candidate
        match = re.match(r"^/CL\d*/([^/]+)/", parsed.path)
        if not match:
            return candidate
        unwrapped = unquote(match.group(1)).strip()
        if not unwrapped or unwrapped == candidate:
            return candidate
        candidate = unwrapped
    return candidate


def parse_datetime(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = date_parser.parse(str(value))
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def normalize_web_text(value: object) -> str | None:
    if value is None:
        return None
    text = BeautifulSoup(str(value), "html.parser").get_text(" ", strip=True)
    normalized = re.sub(r"\s+", " ", text).strip()
    return normalized or None


VaultSourceIngestionService = VaultFetchService
