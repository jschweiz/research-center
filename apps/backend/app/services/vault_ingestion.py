from __future__ import annotations

import json
import re
from contextlib import suppress
from datetime import UTC, datetime
from urllib.parse import urlparse, urlsplit

from app.core.outbound import UnsafeOutboundUrlError
from app.db.models import IngestionRunType, RunStatus, ScoreBucket
from app.integrations.extractors import ContentExtractor
from app.schemas.items import CapturedPageImportRequest
from app.schemas.ops import IngestionRunHistoryRead, ItemsIndexStatusRead, OperationBasicInfoRead
from app.services.profile import (
    DEFAULT_RANKING_THRESHOLDS,
    DEFAULT_RANKING_WEIGHTS,
    load_profile_snapshot,
)
from app.services.text import normalize_whitespace
from app.services.vault_insights import VaultInsightsService
from app.services.vault_lightweight_enrichment import VaultLightweightEnrichmentService
from app.services.vault_runtime import (
    RunRecorder,
    classify_written_kind,
    content_hash,
    document_identity_hash,
    extract_links,
    infer_content_type,
    readable_doc_id,
    utcnow,
)
from app.vault.models import (
    SUB_DOCUMENT_TAG,
    ItemsIndex,
    RawDocument,
    RawDocumentFrontmatter,
    VaultItemRecord,
    VaultItemScore,
)
from app.vault.store import LeaseBusyError, VaultStore

TEXT_SOURCE_SUFFIXES = {".md", ".markdown", ".txt", ".json"}
MANUAL_IMPORT_SOURCE_ID = "manual-import"
CAPTURE_METADATA_FILENAME = "capture.json"
CAPTURED_HTML_FILENAME = "captured.html"
FETCHED_HTML_FILENAME = "original.html"
CAPTURED_HTML_MAX_CHARS = 150000
CAPTURED_TEXT_MAX_CHARS = 60000
CAPTURED_TEXT_MIN_CHARS = 280
CAPTURED_TEXT_MIN_WORDS = 40
MANAGED_IMPORT_ASSET_FILENAMES = {
    CAPTURE_METADATA_FILENAME,
    CAPTURED_HTML_FILENAME,
    FETCHED_HTML_FILENAME,
}
AUTHOR_SPLIT_RE = re.compile(r"\s*(?:,|;|\||\band\b|&)\s*", re.IGNORECASE)


class VaultIndexService:
    def __init__(self, *, store: VaultStore | None = None, ensure_layout: bool = True) -> None:
        self.store = store or VaultStore()
        self.extractor = ContentExtractor()
        self.insights = VaultInsightsService(store=self.store, ensure_layout=ensure_layout)
        self.runs = RunRecorder(self.store)
        if ensure_layout:
            self.store.ensure_layout()

    def rebuild_items_index(self, *, trigger: str = "manual_index") -> ItemsIndex:
        run = self.runs.start(
            run_type=IngestionRunType.INGEST,
            operation_kind="vault_index",
            trigger=trigger,
            title="Vault index rebuild",
            summary="Scanning raw documents and rebuilding the item index.",
        )
        lease = None
        try:
            try:
                lease = self.store.acquire_lease(name="vault-index", owner="mac", ttl_seconds=600)
            except LeaseBusyError as exc:
                run.errors.append(str(exc))
                self.runs.finish(
                    run,
                    status=RunStatus.FAILED,
                    summary="Vault index skipped because another index rebuild is already running.",
                )
                return self.store.load_items_index()

            step = self.runs.start_step(run, step_kind="vault_index")
            self._bootstrap_missing_source_files()
            documents = self.store.list_raw_documents()
            self.store.sync_raw_documents(documents)
            items = self._expected_index_items(documents=documents, persist_normalized_frontmatter=True)
            items, insights = self.insights.enrich_items(items)
            items = self._score_items(items)
            index = ItemsIndex(generated_at=utcnow(), items=items)
            self.store.save_items_index(index)
            self.store.save_insights_index(insights)
            self.runs.finish_step(run, step, status=RunStatus.SUCCEEDED, counts_by_kind=self._counts_by_kind(items))
            run.total_titles = len(index.items)
            run.basic_info.extend(
                [
                    OperationBasicInfoRead(label="Items", value=str(len(index.items))),
                    OperationBasicInfoRead(label="Raw docs", value=str(len(documents))),
                    OperationBasicInfoRead(label="Index", value="SQLite `vault_items` + `vault_item_fts`"),
                    OperationBasicInfoRead(label="Topics", value=str(len(insights.topics))),
                    OperationBasicInfoRead(label="Rising topics", value=str(len(insights.rising_topic_ids))),
                    OperationBasicInfoRead(label="Insights index", value="SQLite `vault_topics`"),
                ]
            )
            self.runs.finish(
                run,
                status=RunStatus.SUCCEEDED,
                summary=f"Rebuilt local DB indexes from {len(documents)} raw document folders.",
            )
            return index
        except Exception as exc:
            run.errors.append(str(exc))
            self.runs.finish(run, status=RunStatus.FAILED, summary="Vault index rebuild failed.")
            raise
        finally:
            if lease is not None:
                self.store.release_lease(lease)

    def import_url(self, url: str) -> VaultItemRecord:
        run = self.runs.start(
            run_type=IngestionRunType.INGEST,
            operation_kind="raw_fetch",
            trigger="manual_import",
            title="Manual URL import",
            summary="Fetching a URL and filing it into the vault.",
        )
        try:
            extracted = self.extractor.extract_from_url(url)
        except UnsafeOutboundUrlError:
            raise
        except Exception as exc:
            run.errors.append(str(exc))
            self.runs.finish(run, status=RunStatus.FAILED, summary="Manual URL import failed.")
            raise

        now = utcnow()
        normalized_url = self._normalize_manual_url(url)
        source_name = (urlparse(url).hostname or "web").replace("www.", "")
        title = extracted.title or url
        body = extracted.cleaned_text or title
        frontmatter = RawDocumentFrontmatter(
            id=readable_doc_id(
                stable_key=normalized_url,
                title=title,
                source_slug="manual-import",
                published_at=extracted.published_at,
            ),
            kind="article",
            title=title,
            source_url=normalized_url,
            source_name=source_name,
            authors=[],
            published_at=extracted.published_at,
            ingested_at=now,
            content_hash=content_hash(title, body),
            identity_hash=document_identity_hash(
                source_id="manual-import",
                external_key=normalized_url,
                canonical_url=normalized_url,
                fallback_key=title,
            ),
            tags=[],
            status="active",
            asset_paths=[],
            source_id="manual-import",
            source_pipeline_id="manual-import",
            external_key=normalized_url,
            canonical_url=normalized_url,
            doc_role="primary",
            parent_id=None,
            index_visibility="visible",
            fetched_at=now,
            short_summary=None,
            lightweight_enrichment_status="pending",
        )
        doc_path = self.store.write_raw_document(
            kind="article",
            doc_id=frontmatter.id,
            frontmatter=frontmatter,
            body=body,
        )
        html = extracted.raw_payload.get("html") if isinstance(extracted.raw_payload, dict) else None
        if isinstance(html, str) and html.strip():
            original_path = doc_path.parent / "original.html"
            self.store.write_text(original_path, html)
            frontmatter.asset_paths = ["original.html"]
            self.store.write_raw_document(
                kind="article",
                doc_id=frontmatter.id,
                frontmatter=frontmatter,
                body=body,
            )

        with suppress(Exception):
            VaultLightweightEnrichmentService().enrich_stale_documents(
                trigger="manual_import",
                doc_id=frontmatter.id,
            )

        index = self.rebuild_items_index(trigger="manual_import")
        for item in index.items:
            if item.id == frontmatter.id:
                run.basic_info.append(OperationBasicInfoRead(label="Document", value=item.title))
                self.runs.finish(
                    run,
                    status=RunStatus.SUCCEEDED,
                    summary=f"Imported {item.title} into the vault.",
                )
                return item
        self.runs.finish(
            run,
            status=RunStatus.FAILED,
            summary="Manual URL import completed but the new item was not indexed.",
        )
        raise RuntimeError("Imported URL could not be indexed.")

    def import_captured_page(
        self,
        payload: CapturedPageImportRequest | dict[str, object],
    ) -> VaultItemRecord:
        request = (
            payload
            if isinstance(payload, CapturedPageImportRequest)
            else CapturedPageImportRequest.model_validate(payload)
        )
        run = self.runs.start(
            run_type=IngestionRunType.INGEST,
            operation_kind="raw_capture",
            trigger="manual_capture_import",
            title="Captured page import",
            summary="Saving a browser-captured page into the vault.",
        )
        now = utcnow()
        page_url = self._normalize_manual_url(str(request.url))
        canonical_hint = str(request.canonical_url) if request.canonical_url is not None else None
        canonical_url = self._resolve_captured_canonical_url(
            page_url=page_url,
            canonical_hint=canonical_hint,
        )
        existing = self.store.find_raw_document(
            source_id=MANUAL_IMPORT_SOURCE_ID,
            external_key=canonical_url,
        )
        existing_frontmatter = existing.frontmatter if existing is not None else None

        page_title = self._normalize_optional_string(request.page_title, limit=500)
        site_name = self._normalize_optional_string(request.site_name, limit=120)
        description = self._normalize_optional_string(request.description, limit=500)
        byline = self._normalize_optional_string(request.byline, limit=160)
        language = self._normalize_optional_string(request.language, limit=32)
        extraction_mode = (
            self._normalize_optional_string(request.extraction_mode, limit=48) or "capture"
        )
        author_hints = self._parse_capture_authors(request.author_hints, byline=byline)

        captured_text = self._normalize_captured_text(request.content_text)
        captured_html = self._normalize_html_asset(request.article_html)
        fetched = None
        fetched_html: str | None = None
        fetch_error: str | None = None

        body_source = "captured_text" if self._has_usable_captured_text(captured_text) else None
        if body_source is None:
            try:
                fetched = self.extractor.extract_from_url(page_url)
                fetched_text = self._normalize_captured_text(fetched.cleaned_text)
                if self._has_usable_captured_text(fetched_text):
                    captured_text = fetched_text
                    body_source = "server_fetch"
                    extraction_mode = "server-fetch"
                raw_html = fetched.raw_payload.get("html") if isinstance(fetched.raw_payload, dict) else None
                fetched_html = self._normalize_html_asset(raw_html)
            except UnsafeOutboundUrlError as exc:
                fetch_error = str(exc)
            except Exception as exc:
                fetch_error = str(exc)

        if body_source is None and existing is not None:
            body_source = "existing_document"

        if body_source is None:
            body_source = "placeholder"

        title = self._resolve_captured_page_title(
            page_title=page_title,
            fetched_title=fetched.title if fetched is not None else None,
            existing_title=existing_frontmatter.title if existing_frontmatter is not None else None,
            canonical_url=canonical_url,
        )
        published_at = self._resolve_captured_page_published_at(
            captured_published_at=request.published_at,
            fetched_published_at=fetched.published_at if fetched is not None else None,
            existing_published_at=(
                existing_frontmatter.published_at if existing_frontmatter is not None else None
            ),
        )
        source_name = self._resolve_captured_page_source_name(
            site_name=site_name,
            canonical_url=canonical_url,
            existing_source_name=(
                existing_frontmatter.source_name if existing_frontmatter is not None else None
            ),
        )
        kind = (
            existing_frontmatter.kind
            if existing_frontmatter is not None
            else classify_written_kind(
                source_url=canonical_url,
                title=title,
                source_name=source_name,
                source_id=MANUAL_IMPORT_SOURCE_ID,
                default_kind="article",
            )
        )
        merged_authors = self._merge_unique_strings(
            existing_frontmatter.authors if existing_frontmatter is not None else [],
            author_hints,
            limit=6,
        )
        short_summary = (
            existing_frontmatter.short_summary
            if existing_frontmatter is not None and existing_frontmatter.short_summary
            else description
        )
        tags = list(existing_frontmatter.tags) if existing_frontmatter is not None else []

        if body_source == "existing_document" and existing is not None:
            body = existing.body
        else:
            body_text = (
                captured_text
                if body_source != "placeholder"
                else self._placeholder_import_text(canonical_url)
            )
            body = self._render_captured_source_body(title=title, text=body_text)

        asset_payloads = self._build_capture_asset_payloads(
            request=request,
            canonical_url=canonical_url,
            body_source=body_source,
            fetch_error=fetch_error,
            captured_html=captured_html,
            fetched_html=fetched_html,
            saved_title=title,
            language=language,
            extraction_mode=extraction_mode,
        )
        retained_assets = (
            [
                asset_path
                for asset_path in existing_frontmatter.asset_paths
                if asset_path not in MANAGED_IMPORT_ASSET_FILENAMES
            ]
            if existing_frontmatter is not None
            else []
        )
        asset_paths = sorted([*retained_assets, *asset_payloads.keys()])
        doc_id = (
            existing_frontmatter.id
            if existing_frontmatter is not None
            else readable_doc_id(
                stable_key=canonical_url,
                title=title,
                source_slug=MANUAL_IMPORT_SOURCE_ID,
                published_at=published_at,
            )
        )
        frontmatter = RawDocumentFrontmatter(
            id=doc_id,
            kind=kind,
            title=title,
            source_url=canonical_url,
            source_name=source_name,
            authors=merged_authors,
            published_at=published_at,
            ingested_at=existing_frontmatter.ingested_at if existing_frontmatter is not None else now,
            content_hash=content_hash(title, body),
            identity_hash=document_identity_hash(
                source_id=MANUAL_IMPORT_SOURCE_ID,
                external_key=canonical_url,
                canonical_url=canonical_url,
                fallback_key=title,
            ),
            tags=tags,
            status=existing_frontmatter.status if existing_frontmatter is not None else "active",
            asset_paths=asset_paths,
            source_id=MANUAL_IMPORT_SOURCE_ID,
            source_pipeline_id=MANUAL_IMPORT_SOURCE_ID,
            external_key=canonical_url,
            canonical_url=canonical_url,
            doc_role=existing_frontmatter.doc_role if existing_frontmatter is not None else "primary",
            parent_id=existing_frontmatter.parent_id if existing_frontmatter is not None else None,
            index_visibility=(
                existing_frontmatter.index_visibility
                if existing_frontmatter is not None
                else "visible"
            ),
            fetched_at=now,
            short_summary=short_summary,
            lightweight_enrichment_status=(
                existing_frontmatter.lightweight_enrichment_status
                if existing_frontmatter is not None
                else "pending"
            ),
            lightweight_enriched_at=(
                existing_frontmatter.lightweight_enriched_at
                if existing_frontmatter is not None
                else None
            ),
            lightweight_enrichment_model=(
                existing_frontmatter.lightweight_enrichment_model
                if existing_frontmatter is not None
                else None
            ),
            lightweight_enrichment_input_hash=(
                existing_frontmatter.lightweight_enrichment_input_hash
                if existing_frontmatter is not None
                else None
            ),
            lightweight_enrichment_error=(
                existing_frontmatter.lightweight_enrichment_error
                if existing_frontmatter is not None
                else None
            ),
            lightweight_scoring_model=(
                existing_frontmatter.lightweight_scoring_model
                if existing_frontmatter is not None
                else None
            ),
            lightweight_scoring_input_hash=(
                existing_frontmatter.lightweight_scoring_input_hash
                if existing_frontmatter is not None
                else None
            ),
            lightweight_score=(
                existing_frontmatter.lightweight_score
                if existing_frontmatter is not None
                else None
            ),
        )
        doc_path = self.store.write_raw_document(
            kind=frontmatter.kind,
            doc_id=frontmatter.id,
            frontmatter=frontmatter,
            body=body,
        )
        self._sync_managed_import_assets(doc_path.parent, asset_payloads)

        with suppress(Exception):
            VaultLightweightEnrichmentService().enrich_stale_documents(
                trigger="manual_capture_import",
                doc_id=frontmatter.id,
            )

        index = self.rebuild_items_index(trigger="manual_capture_import")
        for item in index.items:
            if item.id == frontmatter.id:
                run.basic_info.extend(
                    [
                        OperationBasicInfoRead(label="Document", value=item.title),
                        OperationBasicInfoRead(label="Body source", value=body_source),
                        OperationBasicInfoRead(label="Canonical URL", value=canonical_url),
                    ]
                )
                self.runs.finish(
                    run,
                    status=RunStatus.SUCCEEDED,
                    summary=f"Imported {item.title} from the browser capture.",
                )
                return item
        self.runs.finish(
            run,
            status=RunStatus.FAILED,
            summary="Captured page import completed but the document was not indexed.",
        )
        raise RuntimeError("Captured page import could not be indexed.")

    def items_index_status(
        self,
        *,
        documents: list[RawDocument] | None = None,
    ) -> ItemsIndexStatusRead:
        candidates = list(documents) if documents is not None else self.store.list_raw_documents()
        expected_items = self._expected_index_items(documents=candidates, persist_normalized_frontmatter=False)
        expected_items, _insights = self.insights.enrich_items(expected_items)
        expected_items = self._score_items(expected_items)

        current_index = self.store.load_items_index()
        current_items = current_index.items if current_index is not None else []
        current_by_path = {item.raw_doc_path: item for item in current_items}
        expected_by_path = {item.raw_doc_path: item for item in expected_items}

        stale_paths = [
            path
            for path, expected in expected_by_path.items()
            if self._item_record_payload(current_by_path.get(path)) != self._item_record_payload(expected)
        ]
        orphaned_paths = [path for path in current_by_path if path not in expected_by_path]
        duplicate_current_paths = max(0, len(current_items) - len(current_by_path))
        stale_document_count = len(stale_paths) + len(orphaned_paths) + duplicate_current_paths

        return ItemsIndexStatusRead(
            up_to_date=stale_document_count == 0,
            stale_document_count=stale_document_count,
            indexed_item_count=len(current_items),
            generated_at=current_index.generated_at if current_index is not None else None,
        )

    def list_recent_runs(self, *, limit: int = 20) -> list[IngestionRunHistoryRead]:
        records = self.store.load_run_records()
        parsed = [IngestionRunHistoryRead.model_validate(payload) for payload in reversed(records) if payload]
        return parsed[:limit]

    def audit_vault(self) -> dict[str, object]:
        index = self.store.load_items_index()
        by_url: dict[str, list[str]] = {}
        by_identity_hash: dict[str, list[str]] = {}
        by_hash: dict[str, list[str]] = {}
        for item in index.items:
            by_url.setdefault(item.canonical_url, []).append(item.id)
            if item.identity_hash:
                by_identity_hash.setdefault(item.identity_hash, []).append(item.id)
            by_hash.setdefault(item.content_hash, []).append(item.id)
        duplicate_urls = {key: value for key, value in by_url.items() if key and len(value) > 1}
        duplicate_identity_hashes = {
            key: value for key, value in by_identity_hash.items() if len(value) > 1
        }
        duplicate_hashes = {key: value for key, value in by_hash.items() if len(value) > 1}
        return {
            "vault_root": str(self.store.root),
            "raw_documents": len(self.store.list_raw_documents()),
            "indexed_items": len(index.items),
            "duplicate_urls": duplicate_urls,
            "duplicate_identity_hashes": duplicate_identity_hashes,
            "duplicate_content_hashes": duplicate_hashes,
            "run_storage": "sqlite:vault_runs",
        }

    def _bootstrap_missing_source_files(self) -> None:
        for kind_dir in self.store.raw_dir.glob("*"):
            if not kind_dir.is_dir():
                continue
            for doc_dir in kind_dir.glob("*"):
                if not doc_dir.is_dir():
                    continue
                source_path = doc_dir / "source.md"
                if source_path.exists():
                    continue
                candidate_files = sorted(
                    path
                    for path in doc_dir.iterdir()
                    if path.is_file() and path.name != "source.md"
                )
                body = ""
                asset_paths = [path.name for path in candidate_files]
                if candidate_files:
                    primary = candidate_files[0]
                    if primary.suffix.lower() in TEXT_SOURCE_SUFFIXES:
                        body = primary.read_text(encoding="utf-8", errors="ignore")
                    else:
                        body = f"Original file stored at {primary.name}."
                now = datetime.fromtimestamp(doc_dir.stat().st_mtime, tz=UTC)
                title = doc_dir.name.replace("-", " ").title()
                frontmatter = RawDocumentFrontmatter(
                    id=doc_dir.name,
                    kind=kind_dir.name,
                    title=title,
                    source_url=None,
                    source_name=kind_dir.name,
                    authors=[],
                    published_at=None,
                    ingested_at=now,
                    content_hash=content_hash(title, body),
                    identity_hash=document_identity_hash(
                        source_id=kind_dir.name,
                        fallback_key=doc_dir.name,
                    ),
                    tags=[],
                    status="active",
                    asset_paths=asset_paths,
                    canonical_url=None,
                    fetched_at=now,
                )
                self.store.write_raw_document(
                    kind=kind_dir.name,
                    doc_id=doc_dir.name,
                    frontmatter=frontmatter,
                    body=body,
                )

    def _normalize_raw_document(
        self,
        frontmatter: RawDocumentFrontmatter,
        body: str,
        path: str,
    ) -> RawDocumentFrontmatter:
        canonical_url = frontmatter.canonical_url or frontmatter.source_url or f"raw/{frontmatter.kind}/{frontmatter.id}/source.md"
        normalized_title = self.extractor.normalize_title(
            frontmatter.title,
            url=canonical_url or frontmatter.source_url,
        ) or frontmatter.title
        fetched_at = frontmatter.fetched_at or frontmatter.ingested_at
        published_at = frontmatter.published_at or self._recover_published_at_from_assets(frontmatter, path)
        content_hash_value = (
            content_hash(normalized_title, body)
            if normalized_title != frontmatter.title or not frontmatter.content_hash
            else frontmatter.content_hash
        )
        identity_hash_value = frontmatter.identity_hash or document_identity_hash(
            source_id=frontmatter.source_id or frontmatter.source_pipeline_id or frontmatter.kind,
            external_key=frontmatter.external_key,
            canonical_url=canonical_url,
            fallback_key=frontmatter.id,
        )
        return frontmatter.model_copy(
            update={
                "title": normalized_title,
                "canonical_url": canonical_url,
                "published_at": published_at,
                "fetched_at": fetched_at,
                "lightweight_enrichment_status": frontmatter.lightweight_enrichment_status or "pending",
                "index_visibility": frontmatter.index_visibility or "visible",
                "doc_role": frontmatter.doc_role or "primary",
                "content_hash": content_hash_value,
                "identity_hash": identity_hash_value,
            }
        )

    def _expected_index_items(
        self,
        *,
        documents: list[RawDocument],
        persist_normalized_frontmatter: bool,
    ) -> list[VaultItemRecord]:
        items: list[VaultItemRecord] = []
        for document in documents:
            normalized = self._normalize_raw_document(document.frontmatter, document.body, document.path)
            if persist_normalized_frontmatter and normalized != document.frontmatter:
                self.store.write_raw_document(
                    kind=document.frontmatter.kind,
                    doc_id=document.frontmatter.id,
                    frontmatter=normalized,
                    body=document.body,
                )
            items.append(self._build_item_record(normalized, document.body, document.path))
        items.sort(key=self._item_sort_key, reverse=True)
        return items

    @staticmethod
    def _item_sort_key(item: VaultItemRecord) -> tuple[datetime, str]:
        return (
            item.published_at or item.fetched_at or item.ingested_at,
            item.title.lower(),
        )

    @staticmethod
    def _item_record_payload(item: VaultItemRecord | None) -> dict[str, object] | None:
        if item is None:
            return None
        return item.model_dump(mode="json")

    def _recover_published_at_from_assets(
        self,
        frontmatter: RawDocumentFrontmatter,
        path: str,
    ) -> datetime | None:
        if frontmatter.published_at is not None or "original.html" not in frontmatter.asset_paths:
            return frontmatter.published_at
        original_path = self.store.root / path
        original_html_path = original_path.parent / "original.html"
        if not original_html_path.exists():
            return None
        try:
            html = original_html_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return None
        return self.extractor.extract_published_at_from_html(html)

    def _build_item_record(
        self,
        frontmatter: RawDocumentFrontmatter,
        body: str,
        path: str,
    ) -> VaultItemRecord:
        relative_path = path
        canonical = frontmatter.canonical_url or frontmatter.source_url or relative_path
        tags = self._merge_unique_strings(
            frontmatter.tags,
            [SUB_DOCUMENT_TAG] if frontmatter.doc_role == "derived" else [],
            limit=max(len(frontmatter.tags) + 1, 1),
        )
        return VaultItemRecord(
            id=frontmatter.id,
            kind=frontmatter.kind,
            title=frontmatter.title,
            source_id=frontmatter.source_id,
            source_name=frontmatter.source_name or frontmatter.source_id or frontmatter.kind,
            organization_name=None,
            authors=frontmatter.authors,
            published_at=frontmatter.published_at,
            ingested_at=frontmatter.ingested_at,
            fetched_at=frontmatter.fetched_at,
            canonical_url=canonical,
            content_type=infer_content_type(frontmatter.kind, frontmatter.title, canonical, body),
            extraction_confidence=0.0,
            cleaned_text=body,
            outbound_links=extract_links(body),
            tags=tags,
            status=frontmatter.status,
            asset_paths=frontmatter.asset_paths,
            content_hash=frontmatter.content_hash,
            identity_hash=frontmatter.identity_hash,
            raw_doc_path=relative_path,
            doc_role=frontmatter.doc_role,
            parent_id=frontmatter.parent_id,
            index_visibility=frontmatter.index_visibility,
            short_summary=frontmatter.short_summary,
            lightweight_enrichment_status=frontmatter.lightweight_enrichment_status,
            lightweight_enriched_at=frontmatter.lightweight_enriched_at,
            lightweight_enrichment_model=frontmatter.lightweight_enrichment_model,
            lightweight_scoring_model=frontmatter.lightweight_scoring_model,
            lightweight_score=frontmatter.lightweight_score,
            updated_at=frontmatter.lightweight_enriched_at or frontmatter.fetched_at or frontmatter.ingested_at,
        )

    def _score_items(self, items: list[VaultItemRecord]) -> list[VaultItemRecord]:
        profile = load_profile_snapshot()
        weights = dict(DEFAULT_RANKING_WEIGHTS)
        if isinstance(getattr(profile, "ranking_weights", None), dict):
            weights |= dict(profile.ranking_weights)

        thresholds = dict(DEFAULT_RANKING_THRESHOLDS)
        ranking_thresholds = getattr(profile, "ranking_thresholds", None)
        if hasattr(ranking_thresholds, "model_dump"):
            thresholds |= ranking_thresholds.model_dump()
        elif isinstance(ranking_thresholds, dict):
            thresholds |= ranking_thresholds

        favorite_topics = [value.casefold() for value in getattr(profile, "favorite_topics", []) or []]
        favorite_authors = {value.casefold() for value in getattr(profile, "favorite_authors", []) or []}
        favorite_sources = {
            value.casefold() for value in getattr(profile, "favorite_sources", []) or []
        }
        ignored_topics = [value.casefold() for value in getattr(profile, "ignored_topics", []) or []]

        scored_items: list[VaultItemRecord] = []
        for item in items:
            title_text = item.title.casefold()
            tags_text = " ".join(item.tags).casefold()
            topic_labels = " ".join(ref.label for ref in item.topic_refs).casefold()
            content_text = normalize_whitespace(
                "\n".join(
                    [
                        item.title,
                        item.short_summary or "",
                        item.cleaned_text or "",
                        " ".join(item.tags),
                        " ".join(ref.label for ref in item.topic_refs),
                    ]
                )
            ).casefold()
            topic_match_count = self._contains_any(
                "\n".join([title_text, tags_text, topic_labels, content_text]),
                favorite_topics,
            )
            author_match_count = sum(
                1 for author in item.authors if author.casefold() in favorite_authors
            )
            source_match = any(
                token and token in favorite_sources
                for token in {
                    (item.source_name or "").casefold(),
                    (item.source_id or "").casefold(),
                }
            )
            ignored_penalty = self._contains_any(
                "\n".join([title_text, tags_text, topic_labels, content_text]),
                ignored_topics,
            )
            heuristic_topic_match_score = self._clamp_unit_score(
                min(topic_match_count * 0.28 + (0.16 if source_match else 0.0), 1.0)
            )
            heuristic_author_match_score = self._clamp_unit_score(min(author_match_count * 0.5, 1.0))
            heuristic_source_score = self._clamp_unit_score(
                1.0
                if source_match
                else (
                    0.42 if not favorite_sources and item.kind == "paper" else 0.24
                )
            )
            judge = item.lightweight_score
            judge_model = item.lightweight_scoring_model or ""
            judge_is_model_based = judge is not None and not str(judge_model).startswith("heuristic:")
            if judge is None:
                relevance_score = self._clamp_unit_score(
                    heuristic_topic_match_score * 0.5
                    + heuristic_author_match_score * 0.2
                    + heuristic_source_score * 0.15
                    + 0.15
                )
                source_quality_score = heuristic_source_score
                author_match_score = heuristic_author_match_score
                topic_match_score = heuristic_topic_match_score
            elif judge_is_model_based:
                source_quality_score = self._blend_model_fit_score(
                    judge_score=judge.source_fit_score,
                    heuristic_score=heuristic_source_score,
                    confidence=judge.confidence_score,
                )
                author_match_score = self._blend_model_fit_score(
                    judge_score=judge.author_fit_score,
                    heuristic_score=heuristic_author_match_score,
                    confidence=judge.confidence_score,
                )
                topic_match_score = self._blend_model_fit_score(
                    judge_score=judge.topic_fit_score,
                    heuristic_score=heuristic_topic_match_score,
                    confidence=judge.confidence_score,
                )
                relevance_score = self._clamp_unit_score(
                    judge.relevance_score * 0.84
                    + judge.evidence_fit_score * 0.1
                    + judge.confidence_score * 0.04
                    + topic_match_score * 0.02
                )
            else:
                relevance_score = self._clamp_unit_score(judge.relevance_score)
                source_quality_score = self._clamp_unit_score(judge.source_fit_score)
                author_match_score = self._clamp_unit_score(judge.author_fit_score)
                topic_match_score = self._clamp_unit_score(judge.topic_fit_score)
            novelty_score = self._clamp_unit_score(item.novelty_score * 0.65 + item.trend_score * 0.35)
            zotero_affinity_score = 0.0
            total = (
                relevance_score * float(weights["relevance"])
                + novelty_score * float(weights["novelty"])
                + source_quality_score * float(weights["source_quality"])
                + author_match_score * float(weights["author_match"])
                + topic_match_score * float(weights["topic_match"])
                + zotero_affinity_score * float(weights["zotero_affinity"])
                - min(ignored_penalty * 0.15, 0.45)
            )
            total = round(max(0.0, min(total, 1.0)), 4)
            judge_bucket_cap_applied: str | None = None
            judge_bucket_floor_applied: str | None = None
            if judge is not None and judge_is_model_based and judge.confidence_score >= 0.58:
                worth_a_skim_floor = float(thresholds["worth_a_skim_min"])
                worth_a_skim_cap = max(float(thresholds["worth_a_skim_min"]) - 0.01, 0.0)
                must_read_floor = float(thresholds["must_read_min"])
                must_read_cap = max(float(thresholds["must_read_min"]) - 0.01, 0.0)
                if judge.bucket_hint == ScoreBucket.ARCHIVE:
                    capped_total = min(total, worth_a_skim_cap)
                    if capped_total != total:
                        judge_bucket_cap_applied = judge.bucket_hint.value
                    total = capped_total
                elif judge.bucket_hint == ScoreBucket.WORTH_A_SKIM:
                    floored_total = max(total, worth_a_skim_floor)
                    if floored_total != total:
                        judge_bucket_floor_applied = judge.bucket_hint.value
                    total = floored_total
                    capped_total = min(total, must_read_cap)
                    if capped_total != total:
                        judge_bucket_cap_applied = judge.bucket_hint.value
                    total = capped_total
                elif judge.bucket_hint == ScoreBucket.MUST_READ:
                    floored_total = max(total, must_read_floor)
                    if floored_total != total:
                        judge_bucket_floor_applied = judge.bucket_hint.value
                    total = floored_total
            bucket = ScoreBucket.ARCHIVE
            if total >= float(thresholds["must_read_min"]):
                bucket = ScoreBucket.MUST_READ
            elif total >= float(thresholds["worth_a_skim_min"]):
                bucket = ScoreBucket.WORTH_A_SKIM

            reason_trace: dict[str, object] = {
                "scoring_mode": "ollama_judge" if judge_is_model_based else "heuristic",
                "favorite_source_match": bool(source_match),
                "topic_matches": topic_match_count,
                "author_matches": author_match_count,
                "ignored_penalty": ignored_penalty,
                "trend_score": round(item.trend_score, 4),
                "novelty_signal": round(item.novelty_score, 4),
                "heuristic_topic_match_score": heuristic_topic_match_score,
                "heuristic_author_match_score": heuristic_author_match_score,
                "heuristic_source_score": heuristic_source_score,
            }
            if judge is not None:
                reason_trace |= {
                    "judge_reason": judge.reason,
                    "judge_bucket_hint": judge.bucket_hint.value,
                    "judge_confidence_score": round(judge.confidence_score, 4),
                    "judge_source_fit_score": round(judge.source_fit_score, 4),
                    "judge_topic_fit_score": round(judge.topic_fit_score, 4),
                    "judge_author_fit_score": round(judge.author_fit_score, 4),
                    "judge_evidence_fit_score": round(judge.evidence_fit_score, 4),
                    "judge_model": judge_model,
                    "judge_evidence_quotes": list(judge.evidence_quotes),
                }
                if judge_bucket_cap_applied is not None:
                    reason_trace["judge_bucket_cap_applied"] = judge_bucket_cap_applied
                if judge_bucket_floor_applied is not None:
                    reason_trace["judge_bucket_floor_applied"] = judge_bucket_floor_applied

            scored_items.append(
                item.model_copy(
                    update={
                        "score": VaultItemScore(
                            relevance_score=relevance_score,
                            novelty_score=novelty_score,
                            source_quality_score=source_quality_score,
                            author_match_score=author_match_score,
                            topic_match_score=topic_match_score,
                            zotero_affinity_score=zotero_affinity_score,
                            total_score=total,
                            bucket=bucket,
                            reason_trace=reason_trace,
                        )
                    }
                )
            )
        return scored_items

    @staticmethod
    def _contains_any(text: str, values: list[str]) -> int:
        lowered = text.casefold()
        return sum(1 for value in values if value and value in lowered)

    @staticmethod
    def _clamp_unit_score(value: float) -> float:
        return round(min(max(value, 0.0), 1.0), 4)

    @classmethod
    def _blend_model_fit_score(
        cls,
        *,
        judge_score: float,
        heuristic_score: float,
        confidence: float,
    ) -> float:
        model_weight = min(max(0.65 + confidence * 0.25, 0.0), 0.9)
        return cls._clamp_unit_score(
            judge_score * model_weight + heuristic_score * (1.0 - model_weight)
        )

    @staticmethod
    def _counts_by_kind(items: list[VaultItemRecord]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in items:
            counts[item.kind] = counts.get(item.kind, 0) + 1
        return counts

    @staticmethod
    def _normalize_manual_url(url: str) -> str:
        parsed = urlparse(url.strip())
        scheme = parsed.scheme or "https"
        host = parsed.netloc.lower()
        path = parsed.path or "/"
        return f"{scheme}://{host}{path}"

    @staticmethod
    def _normalize_optional_string(value: object, *, limit: int | None = None) -> str | None:
        if value is None:
            return None
        cleaned = normalize_whitespace(str(value or ""))
        if not cleaned:
            return None
        if limit is not None:
            cleaned = cleaned[:limit].strip()
        return cleaned or None

    def _normalize_captured_text(self, value: object) -> str | None:
        if value is None:
            return None
        lines: list[str] = []
        previous_blank = False
        for raw_line in str(value).replace("\r\n", "\n").replace("\r", "\n").split("\n"):
            cleaned = re.sub(r"[ \t]+", " ", raw_line).strip()
            if not cleaned:
                if lines and not previous_blank:
                    lines.append("")
                previous_blank = True
                continue
            lines.append(cleaned)
            previous_blank = False
        normalized = "\n".join(lines).strip()
        if not normalized:
            return None
        if len(normalized) > CAPTURED_TEXT_MAX_CHARS:
            normalized = normalized[:CAPTURED_TEXT_MAX_CHARS].rsplit(" ", 1)[0].strip()
        return normalized or None

    def _normalize_html_asset(self, value: object) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized or len(normalized) > CAPTURED_HTML_MAX_CHARS:
            return None
        return normalized

    @staticmethod
    def _has_usable_captured_text(text: str | None) -> bool:
        if not text:
            return False
        if len(text) >= CAPTURED_TEXT_MIN_CHARS:
            return True
        return len(text.split()) >= CAPTURED_TEXT_MIN_WORDS

    def _resolve_captured_canonical_url(
        self,
        *,
        page_url: str,
        canonical_hint: str | None,
    ) -> str:
        if canonical_hint is None:
            return page_url
        normalized_hint = self._normalize_manual_url(canonical_hint)
        if self._origin_tuple(normalized_hint) == self._origin_tuple(page_url):
            return normalized_hint
        return page_url

    @staticmethod
    def _origin_tuple(url: str) -> tuple[str, str, int]:
        parsed = urlsplit(url)
        scheme = parsed.scheme.lower()
        hostname = (parsed.hostname or "").lower()
        port = parsed.port or (443 if scheme == "https" else 80)
        return scheme, hostname, port

    def _resolve_captured_page_title(
        self,
        *,
        page_title: str | None,
        fetched_title: str | None,
        existing_title: str | None,
        canonical_url: str,
    ) -> str:
        for candidate in (page_title, fetched_title, existing_title):
            normalized = self.extractor.normalize_title(
                self._normalize_optional_string(candidate) or "",
                url=canonical_url,
            )
            if normalized:
                return normalized[:500]
        slug = urlparse(canonical_url).path.rstrip("/").split("/")[-1]
        fallback = slug.replace("-", " ").replace("_", " ").strip().title() or canonical_url
        return fallback[:500]

    def _resolve_captured_page_source_name(
        self,
        *,
        site_name: str | None,
        canonical_url: str,
        existing_source_name: str | None,
    ) -> str:
        for candidate in (site_name, existing_source_name):
            normalized = self._normalize_optional_string(candidate, limit=120)
            if normalized:
                return normalized
        hostname = (urlparse(canonical_url).hostname or "web").replace("www.", "")
        return hostname[:120]

    @staticmethod
    def _resolve_captured_page_published_at(
        *,
        captured_published_at: datetime | None,
        fetched_published_at: datetime | None,
        existing_published_at: datetime | None,
    ) -> datetime | None:
        return captured_published_at or fetched_published_at or existing_published_at

    def _parse_capture_authors(
        self,
        author_hints: list[str],
        *,
        byline: str | None,
    ) -> list[str]:
        raw_values = list(author_hints)
        if byline:
            raw_values.append(byline)
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_value in raw_values:
            cleaned_value = self._normalize_optional_string(raw_value, limit=160)
            if not cleaned_value:
                continue
            cleaned_value = re.sub(r"^by\s+", "", cleaned_value, flags=re.IGNORECASE).strip()
            for part in AUTHOR_SPLIT_RE.split(cleaned_value):
                candidate = self._normalize_optional_string(part, limit=80)
                if not candidate:
                    continue
                lowered = candidate.casefold()
                if (
                    lowered in {"staff", "team", "editorial team", "authors"}
                    or lowered.startswith("http")
                    or "@" in candidate
                    or len(candidate.split()) > 8
                ):
                    continue
                if lowered in seen:
                    continue
                seen.add(lowered)
                normalized.append(candidate)
                if len(normalized) >= 6:
                    return normalized
        return normalized

    @staticmethod
    def _merge_unique_strings(
        primary: list[str],
        secondary: list[str],
        *,
        limit: int,
    ) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for value in [*primary, *secondary]:
            cleaned = normalize_whitespace(str(value or ""))
            if not cleaned:
                continue
            lowered = cleaned.casefold()
            if lowered in seen:
                continue
            seen.add(lowered)
            merged.append(cleaned)
            if len(merged) >= limit:
                break
        return merged

    def _render_captured_source_body(self, *, title: str, text: str) -> str:
        cleaned_text = text.strip()
        lines = cleaned_text.splitlines()
        if lines:
            first_line = lines[0].lstrip("#").strip()
            if normalize_whitespace(first_line).casefold() == normalize_whitespace(title).casefold():
                cleaned_text = "\n".join(lines[1:]).strip()
        return f"# {title}\n\n{cleaned_text}".strip() + "\n"

    @staticmethod
    def _placeholder_import_text(canonical_url: str) -> str:
        return (
            f"Manual import could not extract the full text for {canonical_url}. "
            "Open the source link to review the original content."
        )

    def _build_capture_asset_payloads(
        self,
        *,
        request: CapturedPageImportRequest,
        canonical_url: str,
        body_source: str,
        fetch_error: str | None,
        captured_html: str | None,
        fetched_html: str | None,
        saved_title: str,
        language: str | None,
        extraction_mode: str,
    ) -> dict[str, str]:
        payload = {
            "url": str(request.url),
            "canonical_url_hint": str(request.canonical_url) if request.canonical_url else None,
            "resolved_canonical_url": canonical_url,
            "page_title": request.page_title,
            "site_name": request.site_name,
            "description": request.description,
            "published_at": (
                request.published_at.astimezone(UTC).isoformat(timespec="seconds")
                if request.published_at is not None
                else None
            ),
            "author_hints": list(request.author_hints),
            "byline": request.byline,
            "language": language,
            "extraction_mode": extraction_mode,
            "body_source": body_source,
            "fetch_error": fetch_error,
            "saved_title": saved_title,
            "content_text_chars": len(request.content_text or ""),
            "article_html_chars": len(request.article_html or ""),
            "captured_html_saved": bool(captured_html),
            "fetched_html_saved": bool(fetched_html),
        }
        assets = {
            CAPTURE_METADATA_FILENAME: json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        }
        if captured_html:
            assets[CAPTURED_HTML_FILENAME] = captured_html
        if fetched_html:
            assets[FETCHED_HTML_FILENAME] = fetched_html
        return assets

    def _sync_managed_import_assets(
        self,
        folder,
        assets: dict[str, str],
    ) -> None:
        for filename in MANAGED_IMPORT_ASSET_FILENAMES:
            if filename in assets:
                self.store.write_text(folder / filename, assets[filename])
                continue
            path = folder / filename
            if path.exists():
                path.unlink()



VaultIngestionService = VaultIndexService
