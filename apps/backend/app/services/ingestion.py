from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from time import perf_counter
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.core.config import get_settings
from app.core.outbound import UnsafeOutboundUrlError, fetch_safe_response
from app.db.models import (
    ConnectionProvider,
    ContentType,
    DataMode,
    IngestionRun,
    IngestionRunType,
    Item,
    ItemCluster,
    ItemContent,
    ItemInsight,
    RunStatus,
    Source,
    SourceType,
    TriageStatus,
)
from app.integrations.extractors import ContentExtractor, ExtractedContent
from app.integrations.gmail import GmailConnector
from app.integrations.gmail_imap import GmailImapConnector
from app.integrations.llm import LLMClient
from app.integrations.papers import PaperMetadataClient
from app.services.brief_dates import edition_day_for_datetimes
from app.services.clustering import ClusterService
from app.services.connections import ConnectionService
from app.services.data_mode import merge_metadata_for_data_mode
from app.services.profile import ProfileService
from app.services.ranking import RankingService
from app.services.text import normalize_item_title

TRACKING_PREFIXES = ("utm_", "mc_", "ref", "source")
EMAIL_QUERY_RE = re.compile(r"^[^@\s,;:]+@[^@\s,;:]+\.[^@\s,;:]+$")
ARXIV_API_BASE = "https://export.arxiv.org/api/query"
SOURCE_PROBE_GMAIL_WINDOW_DAYS = 30
NEWSLETTER_FACT_QUERY_KEY = "newsletter_fact"
NEWSLETTER_FACT_URL_LIMIT = 6
OPERATION_LOG_LIMIT = 200
WEBSITE_INDEX_DEFAULT_MAX_LINKS = 20
WEBSITE_INDEX_REQUEST_TIMEOUT_SECONDS = 20


def utcnow() -> datetime:
    return datetime.now(UTC)


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    scheme = parsed.scheme.lower()
    if scheme == "http" and hostname not in {"localhost", "127.0.0.1", "0.0.0.0"}:
        scheme = "https"
    query = urlencode(
        [
            (k, v)
            for k, v in parse_qsl(parsed.query, keep_blank_values=True)
            if not k.startswith(TRACKING_PREFIXES)
        ]
    )
    normalized_path = parsed.path or "/"
    if hostname != "mail.google.com":
        normalized_path = normalized_path.rstrip("/") or "/"
    fragment = parsed.fragment if hostname == "mail.google.com" else ""
    return urlunparse((scheme, parsed.netloc.lower(), normalized_path, "", query, fragment))


def hash_content(title: str, cleaned_text: str) -> str:
    digest = hashlib.sha256()
    digest.update(title.strip().lower().encode("utf-8"))
    digest.update(cleaned_text.strip().lower().encode("utf-8"))
    return digest.hexdigest()


def merge_unique_links(existing: list[str], incoming: list[str], limit: int = 50) -> list[str]:
    merged = list(dict.fromkeys([*existing, *incoming]))
    return merged[:limit]


def infer_content_type(source_type: SourceType, title: str, url: str, text: str) -> ContentType:
    lowered = f"{title}\n{url}\n{text}".lower()
    if source_type == SourceType.GMAIL:
        return ContentType.NEWSLETTER
    if source_type == SourceType.ARXIV or "arxiv.org" in lowered or "doi:" in lowered:
        return ContentType.PAPER
    if "thread" in lowered:
        return ContentType.THREAD
    if "newsletter" in lowered:
        return ContentType.NEWSLETTER
    return ContentType.ARTICLE


def parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = date_parser.parse(str(value))
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def choose_published_at(existing: datetime | None, incoming: datetime | None) -> datetime | None:
    if not existing:
        return incoming
    if not incoming:
        return existing
    existing = existing if existing.tzinfo else existing.replace(tzinfo=UTC)
    incoming = incoming if incoming.tzinfo else incoming.replace(tzinfo=UTC)
    if (
        existing.date() == incoming.date()
        and incoming.time() == datetime.min.time()
        and existing.time() != datetime.min.time()
    ):
        return existing
    if (
        existing.date() == incoming.date()
        and existing.time() == datetime.min.time()
        and incoming.time() != datetime.min.time()
    ):
        return incoming
    return incoming


@dataclass
class IngestionRunItemSummary:
    title: str
    outcome: str
    content_type: str
    extraction_confidence: float
    duplicate_mention: bool = False

    def to_metadata(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "outcome": self.outcome,
            "content_type": self.content_type,
            "extraction_confidence": round(self.extraction_confidence, 3),
            "duplicate_mention": self.duplicate_mention,
        }


@dataclass
class SourceRunSummary:
    source_id: str | None
    source_name: str
    status: RunStatus
    items: list[IngestionRunItemSummary] = field(default_factory=list)
    extractor_fallback_count: int = 0
    ai_prompt_tokens: int = 0
    ai_completion_tokens: int = 0
    ai_total_tokens: int = 0
    error: str | None = None
    affected_edition_days: list[date] = field(default_factory=list)

    @property
    def ingested_count(self) -> int:
        return len(self.items)

    @property
    def created_count(self) -> int:
        return sum(1 for item in self.items if item.outcome == "created")

    @property
    def updated_count(self) -> int:
        return sum(1 for item in self.items if item.outcome == "updated")

    @property
    def duplicate_mention_count(self) -> int:
        return sum(1 for item in self.items if item.duplicate_mention)

    @property
    def average_extraction_confidence(self) -> float | None:
        if not self.items:
            return None
        return round(sum(item.extraction_confidence for item in self.items) / len(self.items), 3)

    def to_metadata(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "status": self.status.value,
            "ingested_count": self.ingested_count,
            "created_count": self.created_count,
            "updated_count": self.updated_count,
            "duplicate_mention_count": self.duplicate_mention_count,
            "extractor_fallback_count": self.extractor_fallback_count,
            "ai_prompt_tokens": self.ai_prompt_tokens,
            "ai_completion_tokens": self.ai_completion_tokens,
            "ai_total_tokens": self.ai_total_tokens,
            "average_extraction_confidence": self.average_extraction_confidence,
            "items": [item.to_metadata() for item in self.items],
            "error": self.error,
        }

    @classmethod
    def failed(cls, source: Source, error: str) -> SourceRunSummary:
        return cls(
            source_id=source.id,
            source_name=source.name,
            status=RunStatus.FAILED,
            error=error,
        )


@dataclass
class IngestPayloadResult:
    item: Item
    created: bool
    duplicate_mention_recorded: bool
    affected_edition_days: list[date] = field(default_factory=list)


class SourceIngestionError(RuntimeError):
    def __init__(self, message: str, summary: SourceRunSummary) -> None:
        super().__init__(message)
        self.summary = summary


class SourceProbeError(RuntimeError):
    pass


class IngestionService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.settings = get_settings()
        self.extractor = ContentExtractor()
        self.papers = PaperMetadataClient()
        self.llm = LLMClient()
        self.cluster_service = ClusterService(db)
        self.ranking_service = RankingService(db)
        self.connection_service = ConnectionService(db)
        self.profile_service = ProfileService(db)
        self._active_operation_run_id: str | None = None
        self._active_operation_source_name: str | None = None
        self._changed_item_ids: list[str] = []

    def _empty_ai_usage(self) -> dict[str, int]:
        return {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }

    def _normalize_ai_usage(self, value: Any) -> dict[str, int]:
        if not isinstance(value, dict):
            return self._empty_ai_usage()
        prompt_tokens = self._read_int(value.get("prompt_tokens"))
        completion_tokens = self._read_int(value.get("completion_tokens"))
        total_tokens = self._read_int(value.get("total_tokens"))
        if total_tokens == 0:
            total_tokens = prompt_tokens + completion_tokens
        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }

    def _merge_ai_usage(self, *usages: dict[str, int]) -> dict[str, int]:
        merged = self._empty_ai_usage()
        for usage in usages:
            normalized = self._normalize_ai_usage(usage)
            merged["prompt_tokens"] += normalized["prompt_tokens"]
            merged["completion_tokens"] += normalized["completion_tokens"]
            merged["total_tokens"] += normalized["total_tokens"]
        if merged["total_tokens"] == 0:
            merged["total_tokens"] = merged["prompt_tokens"] + merged["completion_tokens"]
        return merged

    def _estimate_ai_cost_usd(
        self,
        *,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
    ) -> float:
        cost = 0.0
        accounted_tokens = 0
        if prompt_tokens or completion_tokens:
            cost += (
                prompt_tokens / 1_000_000
            ) * self.settings.llm_input_token_cost_per_million_usd + (
                completion_tokens / 1_000_000
            ) * self.settings.llm_output_token_cost_per_million_usd
            accounted_tokens = prompt_tokens + completion_tokens
        remaining_total_tokens = max(total_tokens - accounted_tokens, 0)
        if remaining_total_tokens:
            cost += (
                remaining_total_tokens / 1_000_000
            ) * self.settings.llm_total_token_cost_per_million_usd
        return round(cost, 6)

    def start_operation_run(
        self,
        *,
        run_type: IngestionRunType,
        operation_kind: str,
        trigger: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> IngestionRun:
        run = IngestionRun(
            run_type=run_type,
            status=RunStatus.RUNNING,
            metadata_json={
                "scope": "operation",
                "operation_kind": operation_kind,
                "trigger": trigger,
                **(metadata or {}),
            },
        )
        self.db.add(run)
        self.db.commit()
        return run

    def finalize_operation_run(
        self,
        run: IngestionRun,
        *,
        status: RunStatus,
        metadata: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> IngestionRun:
        existing_metadata = run.metadata_json if isinstance(run.metadata_json, dict) else {}
        run.status = status
        run.error = error
        run.finished_at = utcnow()
        run.metadata_json = {
            **existing_metadata,
            **(metadata or {}),
        }
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def _active_sources_for_ingest(self) -> list[Source]:
        return list(
            self.db.scalars(
                select(Source)
                .options(selectinload(Source.rules))
                .where(
                    Source.active.is_(True),
                    Source.type.in_([SourceType.RSS, SourceType.GMAIL, SourceType.ARXIV]),
                )
            ).all()
        )

    def _operation_log_payload(self, *, message: str, level: str = "info") -> dict[str, str]:
        return {
            "logged_at": utcnow().isoformat(),
            "level": level,
            "message": message,
        }

    def _normalize_operation_log_payloads(self, value: Any) -> list[dict[str, str]]:
        if not isinstance(value, list):
            return []
        logs: list[dict[str, str]] = []
        for raw in value:
            if not isinstance(raw, dict):
                continue
            message = str(raw.get("message") or "").strip()
            if not message:
                continue
            logged_at = parse_datetime(raw.get("logged_at")) or utcnow()
            level = str(raw.get("level") or "info").strip().lower() or "info"
            logs.append(
                {
                    "logged_at": logged_at.isoformat(),
                    "level": level,
                    "message": message,
                }
            )
        return logs[-OPERATION_LOG_LIMIT:]

    def _set_active_operation_log_context(
        self, *, run_id: str | None, source_name: str | None = None
    ) -> None:
        self._active_operation_run_id = run_id
        self._active_operation_source_name = source_name

    def _clear_active_operation_log_context(self) -> None:
        self._active_operation_run_id = None
        self._active_operation_source_name = None

    def reset_changed_item_ids(self) -> None:
        self._changed_item_ids = []

    def drain_changed_item_ids(self) -> list[str]:
        deduped = list(dict.fromkeys(self._changed_item_ids))
        self._changed_item_ids = []
        return deduped

    def _record_changed_item_id(self, item_id: str) -> None:
        if item_id:
            self._changed_item_ids.append(item_id)

    def _log_active_source_step(
        self, message: str, *, level: str = "info", source_name: str | None = None
    ) -> None:
        if not self._active_operation_run_id:
            return
        resolved_source_name = str(source_name or self._active_operation_source_name or "").strip()
        if resolved_source_name:
            message = f"{resolved_source_name}: {message}"
        self.append_operation_log(self._active_operation_run_id, message=message, level=level)

    def _clip_log_text(self, value: str, *, limit: int = 96) -> str:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if len(text) <= limit:
            return text
        return text[: max(limit - 3, 1)].rstrip() + "..."

    def _format_duration(self, seconds: float) -> str:
        clamped_seconds = max(seconds, 0.0)
        if clamped_seconds < 1:
            return "<1s"
        if clamped_seconds < 60:
            return f"{clamped_seconds:.1f}s"
        total_seconds = int(round(clamped_seconds))
        minutes, remainder = divmod(total_seconds, 60)
        if minutes < 60:
            return f"{minutes}m {remainder:02d}s"
        hours, minutes = divmod(minutes, 60)
        return f"{hours}h {minutes:02d}m {remainder:02d}s"

    def _build_completed_source_message(self, summary: SourceRunSummary) -> str:
        details = [
            f"{summary.ingested_count} title{'s' if summary.ingested_count != 1 else ''}",
            f"{summary.created_count} new",
            f"{summary.updated_count} refreshed",
        ]
        if summary.extractor_fallback_count:
            details.append(
                f"{summary.extractor_fallback_count} extractor fallback"
                f"{'s' if summary.extractor_fallback_count != 1 else ''}"
            )
        if summary.ai_total_tokens:
            details.append(
                f"{summary.ai_total_tokens} AI token{'s' if summary.ai_total_tokens != 1 else ''}"
            )
        return f"Completed source: {summary.source_name} ({'; '.join(details)})."

    def _build_gmail_message_log_label(
        self,
        *,
        message_subject: str | None,
        message_index: int | None,
        message_count: int | None,
    ) -> str:
        label = f'Message "{self._clip_log_text(message_subject or "Untitled message", limit=72)}"'
        if message_index is None or message_count is None:
            return label
        return f"{label} ({message_index}/{message_count})"

    def _build_running_cycle_summary(
        self,
        *,
        source_count: int,
        completed_source_count: int,
        total_titles: int,
        failed_source_count: int,
        current_source_name: str | None = None,
    ) -> str:
        if source_count == 0:
            return "No active sources configured."
        parts = [
            f"{completed_source_count}/{source_count} source{'s' if source_count != 1 else ''} completed"
        ]
        if total_titles:
            parts.append(f"{total_titles} title{'s' if total_titles != 1 else ''} extracted")
        if current_source_name:
            parts.append(f"Current: {current_source_name}")
        if failed_source_count:
            parts.append(f"{failed_source_count} failed")
        return " · ".join(parts)

    def _build_cycle_progress_metadata(
        self,
        *,
        trigger: str,
        source_summaries: list[SourceRunSummary],
        source_count: int,
        logs: list[dict[str, str]],
        current_source_name: str | None = None,
    ) -> dict[str, Any]:
        metadata = self._build_cycle_metadata(trigger=trigger, source_summaries=source_summaries)
        completed_source_count = len(source_summaries)
        total_titles = metadata["ingested_count"]
        failed_source_count = metadata["failed_source_count"]
        metadata["source_count"] = source_count
        metadata["summary"] = self._build_running_cycle_summary(
            source_count=source_count,
            completed_source_count=completed_source_count,
            total_titles=total_titles,
            failed_source_count=failed_source_count,
            current_source_name=current_source_name,
        )
        metadata["basic_info"] = [
            {
                "label": "Sources",
                "value": f"{completed_source_count} / {source_count} completed"
                if source_count
                else "0 configured",
            },
            {"label": "Titles", "value": f"{total_titles} extracted"},
            {"label": "Failures", "value": str(failed_source_count)},
            {
                "label": "Current source",
                "value": current_source_name or ("Waiting for worker" if source_count else "n/a"),
            },
        ]
        metadata["logs"] = logs
        return metadata

    def create_ingest_cycle_run(self) -> IngestionRun:
        sources = self._active_sources_for_ingest()
        logs = [
            self._operation_log_payload(
                message=(
                    "Ingest requested. Waiting for the worker to start."
                    if sources
                    else "Ingest requested, but no active sources are configured."
                )
            )
        ]
        run = IngestionRun(
            run_type=IngestionRunType.INGEST,
            status=RunStatus.RUNNING,
            metadata_json=self._build_cycle_progress_metadata(
                trigger="ingest",
                source_summaries=[],
                source_count=len(sources),
                logs=logs,
                current_source_name="Waiting for worker" if sources else None,
            ),
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def append_operation_log(
        self, run_id: str, *, message: str, level: str = "info"
    ) -> IngestionRun | None:
        run = self.db.get(IngestionRun, run_id)
        if not run:
            return None
        metadata = run.metadata_json if isinstance(run.metadata_json, dict) else {}
        logs = self._normalize_operation_log_payloads(metadata.get("logs"))
        logs.append(self._operation_log_payload(message=message, level=level))
        run.metadata_json = {
            **metadata,
            "logs": logs[-OPERATION_LOG_LIMIT:],
        }
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def fail_ingest_cycle_run(
        self, run_id: str, *, error: str, message: str
    ) -> IngestionRun | None:
        run = self.db.get(IngestionRun, run_id)
        if not run:
            return None
        metadata = run.metadata_json if isinstance(run.metadata_json, dict) else {}
        logs = self._normalize_operation_log_payloads(metadata.get("logs"))
        logs.append(self._operation_log_payload(message=message, level="error"))
        run.status = RunStatus.FAILED
        run.error = error
        run.finished_at = utcnow()
        run.metadata_json = {
            **metadata,
            "summary": message,
            "logs": logs[-OPERATION_LOG_LIMIT:],
        }
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def run_all_sources(self) -> int:
        ingested, _ = self.run_all_sources_with_affected_edition_days()
        return ingested

    def run_all_sources_with_affected_edition_days(
        self, *, cycle_run_id: str | None = None
    ) -> tuple[int, set[date]]:
        self.reset_changed_item_ids()
        sources = self._active_sources_for_ingest()
        source_count = len(sources)
        cycle_run = self.db.get(IngestionRun, cycle_run_id) if cycle_run_id else None
        initial_logs = (
            self._normalize_operation_log_payloads(cycle_run.metadata_json.get("logs"))
            if cycle_run and isinstance(cycle_run.metadata_json, dict)
            else []
        )
        initial_logs.append(self._operation_log_payload(message="Ingest worker started."))
        if cycle_run is None:
            cycle_run = IngestionRun(
                run_type=IngestionRunType.INGEST,
                status=RunStatus.RUNNING,
                metadata_json=self._build_cycle_progress_metadata(
                    trigger="ingest",
                    source_summaries=[],
                    source_count=source_count,
                    logs=initial_logs,
                    current_source_name="Waiting for worker" if source_count else None,
                ),
            )
        else:
            cycle_run.status = RunStatus.RUNNING
            cycle_run.error = None
            cycle_run.finished_at = None
            cycle_run.metadata_json = self._build_cycle_progress_metadata(
                trigger="ingest",
                source_summaries=[],
                source_count=source_count,
                logs=initial_logs,
                current_source_name="Waiting for worker" if source_count else None,
            )
        self.db.add(cycle_run)
        self.db.commit()
        self.db.refresh(cycle_run)

        ingested = 0
        affected_edition_days: set[date] = set()
        source_summaries: list[SourceRunSummary] = []
        try:
            if not sources:
                final_logs = self._normalize_operation_log_payloads(
                    cycle_run.metadata_json.get("logs")
                )
                final_logs.append(
                    self._operation_log_payload(
                        message="No active sources configured.", level="success"
                    )
                )
                cycle_run.status = RunStatus.SUCCEEDED
                cycle_run.error = None
                cycle_run.metadata_json = {
                    **self._build_cycle_metadata(trigger="ingest", source_summaries=[]),
                    "source_count": 0,
                    "summary": "No active sources configured.",
                    "basic_info": [
                        {"label": "Sources", "value": "0 configured"},
                        {"label": "Titles", "value": "0 extracted"},
                        {"label": "Failures", "value": "0"},
                    ],
                    "logs": final_logs[-OPERATION_LOG_LIMIT:],
                }
                cycle_run.finished_at = utcnow()
                self.db.add(cycle_run)
                self.db.commit()
                return 0, set()
            for source in sources:
                logs = self._normalize_operation_log_payloads(cycle_run.metadata_json.get("logs"))
                logs.append(self._operation_log_payload(message=f"Starting source: {source.name}"))
                cycle_run.metadata_json = self._build_cycle_progress_metadata(
                    trigger="ingest",
                    source_summaries=source_summaries,
                    source_count=source_count,
                    logs=logs,
                    current_source_name=source.name,
                )
                self.db.add(cycle_run)
                self.db.commit()
                try:
                    self._set_active_operation_log_context(
                        run_id=cycle_run.id, source_name=source.name
                    )
                    summary = self._run_source_with_summary(source)
                    ingested += summary.ingested_count
                    affected_edition_days.update(summary.affected_edition_days)
                    source_summaries.append(summary)
                    logs = self._normalize_operation_log_payloads(
                        cycle_run.metadata_json.get("logs")
                    )
                    logs.append(
                        self._operation_log_payload(
                            message=self._build_completed_source_message(summary),
                            level="success",
                        )
                    )
                except SourceIngestionError as exc:
                    affected_edition_days.update(exc.summary.affected_edition_days)
                    source_summaries.append(exc.summary)
                    logs = self._normalize_operation_log_payloads(
                        cycle_run.metadata_json.get("logs")
                    )
                    logs.append(
                        self._operation_log_payload(
                            message=f"Source failed: {source.name} ({exc.summary.error or 'Unknown error'}).",
                            level="error",
                        )
                    )
                finally:
                    self._clear_active_operation_log_context()
                cycle_run.metadata_json = self._build_cycle_progress_metadata(
                    trigger="ingest",
                    source_summaries=source_summaries,
                    source_count=source_count,
                    logs=logs,
                )
                self.db.add(cycle_run)
                self.db.commit()

            final_logs = self._normalize_operation_log_payloads(cycle_run.metadata_json.get("logs"))
            final_logs.append(
                self._operation_log_payload(
                    message=(
                        f"Ingest cycle finished with {ingested} title{'s' if ingested != 1 else ''} extracted."
                        if ingested
                        else "Ingest cycle finished with no new titles."
                    ),
                    level="success"
                    if not any(summary.status == RunStatus.FAILED for summary in source_summaries)
                    else "info",
                )
            )
            cycle_run.status = (
                RunStatus.FAILED
                if any(summary.status == RunStatus.FAILED for summary in source_summaries)
                else RunStatus.SUCCEEDED
            )
            cycle_run.metadata_json = {
                **self._build_cycle_metadata(trigger="ingest", source_summaries=source_summaries),
                "logs": final_logs[-OPERATION_LOG_LIMIT:],
            }
            cycle_run.error = "\n".join(self._build_cycle_errors(source_summaries)) or None
            cycle_run.finished_at = utcnow()
            self.db.add(cycle_run)
            self.db.commit()
            return ingested, affected_edition_days
        except Exception as exc:
            failure_logs = self._normalize_operation_log_payloads(
                cycle_run.metadata_json.get("logs")
            )
            failure_logs.append(
                self._operation_log_payload(message=f"Ingest cycle failed: {exc}", level="error")
            )
            cycle_run.status = RunStatus.FAILED
            cycle_run.error = str(exc)
            cycle_run.metadata_json = {
                **self._build_cycle_metadata(trigger="ingest", source_summaries=source_summaries),
                "logs": failure_logs[-OPERATION_LOG_LIMIT:],
            }
            cycle_run.finished_at = utcnow()
            self.db.add(cycle_run)
            self.db.commit()
            raise

    def run_source(self, source: Source) -> int:
        ingested, _ = self.run_source_with_affected_edition_days(source)
        return ingested

    def run_source_with_affected_edition_days(self, source: Source) -> tuple[int, set[date]]:
        self.reset_changed_item_ids()
        summary = self._run_source_with_summary(source)
        return summary.ingested_count, set(summary.affected_edition_days)

    def probe_source(self, source: Source) -> dict[str, Any]:
        if source.type in {SourceType.RSS, SourceType.ARXIV}:
            return self._probe_feed_source(source)
        if source.type == SourceType.GMAIL:
            return self._probe_gmail_source(source)
        raise SourceProbeError("This source type cannot be probed.")

    def _run_source_with_summary(self, source: Source) -> SourceRunSummary:
        run = IngestionRun(
            source=source, run_type=IngestionRunType.INGEST, status=RunStatus.RUNNING
        )
        self.db.add(run)
        self.db.commit()
        try:
            if source.type in {SourceType.RSS, SourceType.ARXIV}:
                summary = self._ingest_feed_source(source)
            elif source.type == SourceType.GMAIL:
                summary = self._ingest_gmail_source(source)
            else:
                summary = SourceRunSummary(
                    source_id=source.id, source_name=source.name, status=RunStatus.SUCCEEDED
                )
            run.status = RunStatus.SUCCEEDED
            run.metadata_json = summary.to_metadata()
            run.finished_at = utcnow()
            synced_at = utcnow()
            source.last_synced_at = synced_at
            self.db.add_all([run, source])
            if source.type == SourceType.GMAIL:
                connection = self.connection_service.get_connection(ConnectionProvider.GMAIL)
                if connection:
                    connection.last_synced_at = synced_at
                    self.db.add(connection)
            self.db.commit()
            return summary
        except Exception as exc:
            summary = SourceRunSummary.failed(source, str(exc))
            run.status = RunStatus.FAILED
            run.error = str(exc)
            run.metadata_json = summary.to_metadata()
            run.finished_at = utcnow()
            self.db.add(run)
            self.db.commit()
            raise SourceIngestionError(str(exc), summary) from exc

    def import_manual_url(self, url: str) -> Item:
        return self.import_manual_url_with_result(url).item

    def import_manual_url_with_result(self, url: str) -> IngestPayloadResult:
        source = self.db.scalar(
            select(Source).where(Source.type == SourceType.MANUAL, Source.name == "Manual import")
        )
        if not source:
            source = Source(
                type=SourceType.MANUAL,
                name="Manual import",
                priority=70,
                active=True,
                tags=["manual"],
            )
            self.db.add(source)
            self.db.commit()
            self.db.refresh(source)

        normalized_url = normalize_url(url)
        existing_item = self.db.scalar(
            select(Item)
            .options(selectinload(Item.content))
            .where(Item.canonical_url == normalized_url)
        )
        try:
            extracted = self.extractor.extract_from_url(url)
        except UnsafeOutboundUrlError:
            raise
        except Exception as exc:
            extracted = self._manual_import_fallback(
                url=url, existing_item=existing_item, error=exc
            )
        return self._ingest_payload_with_result(
            source=source,
            title=extracted.title,
            canonical_url=str(url),
            authors=existing_item.authors if existing_item else [],
            published_at=extracted.published_at
            or (existing_item.published_at if existing_item else None),
            cleaned_text=extracted.cleaned_text,
            raw_payload=extracted.raw_payload,
            outbound_links=extracted.outbound_links,
            extraction_confidence=extracted.extraction_confidence,
            metadata_json={"mime_type": extracted.mime_type},
        )

    def _manual_import_fallback(
        self,
        *,
        url: str,
        existing_item: Item | None,
        error: Exception,
    ) -> ExtractedContent:
        normalized_url = normalize_url(url)
        if existing_item:
            return ExtractedContent(
                title=existing_item.title,
                cleaned_text=(
                    existing_item.content.cleaned_text
                    if existing_item.content and existing_item.content.cleaned_text
                    else existing_item.title
                ),
                outbound_links=existing_item.content.outbound_links
                if existing_item.content
                else [],
                published_at=existing_item.published_at,
                mime_type=(
                    existing_item.metadata_json.get("mime_type")
                    if isinstance(existing_item.metadata_json.get("mime_type"), str)
                    else None
                ),
                extraction_confidence=max(existing_item.extraction_confidence, 0.05),
                raw_payload={
                    "requested_url": normalized_url,
                    "fetch_error": str(error),
                    "fallback": "existing_item",
                },
            )

        slug = urlparse(normalized_url).path.rstrip("/").split("/")[-1]
        title = slug.replace("-", " ").replace("_", " ").strip().title() or normalized_url
        return ExtractedContent(
            title=title,
            cleaned_text=(
                f"Manual import could not extract the full text for {normalized_url}. "
                "Open the source link to review the original content."
            ),
            outbound_links=[],
            published_at=None,
            mime_type=None,
            extraction_confidence=0.05,
            raw_payload={
                "requested_url": normalized_url,
                "fetch_error": str(error),
                "fallback": "placeholder",
            },
        )

    def ingest_payload(
        self,
        *,
        source: Source,
        title: str,
        canonical_url: str,
        authors: list[str],
        published_at: datetime | None,
        cleaned_text: str,
        raw_payload: dict,
        outbound_links: list[str],
        extraction_confidence: float,
        metadata_json: dict | None = None,
        content_type: ContentType | None = None,
        insight_payload: dict | None = None,
    ) -> Item:
        return self._ingest_payload_with_result(
            source=source,
            title=title,
            canonical_url=canonical_url,
            authors=authors,
            published_at=published_at,
            cleaned_text=cleaned_text,
            raw_payload=raw_payload,
            outbound_links=outbound_links,
            extraction_confidence=extraction_confidence,
            metadata_json=metadata_json,
            content_type=content_type,
            insight_payload=insight_payload,
        ).item

    def _ingest_payload_with_result(
        self,
        *,
        source: Source,
        title: str,
        canonical_url: str,
        authors: list[str],
        published_at: datetime | None,
        cleaned_text: str,
        raw_payload: dict,
        outbound_links: list[str],
        extraction_confidence: float,
        metadata_json: dict | None = None,
        content_type: ContentType | None = None,
        insight_payload: dict | None = None,
    ) -> IngestPayloadResult:
        timezone_name = self.profile_service.get_profile().timezone
        incoming_data_mode = DataMode.SEED if (metadata_json or {}).get("seeded") else DataMode.LIVE
        metadata_json = merge_metadata_for_data_mode(
            None,
            metadata_json or {},
            incoming_mode=incoming_data_mode,
        )
        canonical_url = normalize_url(canonical_url)
        content_type = content_type or infer_content_type(
            source.type, title, canonical_url, cleaned_text
        )
        title = normalize_item_title(title, content_type=content_type)
        content_hash = hash_content(title, cleaned_text)
        base_query = select(Item).options(
            selectinload(Item.content),
            selectinload(Item.score),
            selectinload(Item.insight),
            selectinload(Item.cluster).selectinload(ItemCluster.items),
            selectinload(Item.zotero_matches),
        )
        item = self.db.scalar(base_query.where(Item.canonical_url == canonical_url))
        matched_by_content_hash = False
        created = False
        duplicate_mention_recorded = False
        previous_edition_day: date | None = None
        if not item:
            item = self.db.scalar(
                base_query.where(Item.content_hash == content_hash).order_by(
                    Item.first_seen_at.asc()
                )
            )
            matched_by_content_hash = item is not None
        if item:
            previous_edition_day = edition_day_for_datetimes(
                published_at=item.published_at,
                first_seen_at=item.first_seen_at,
                timezone_name=timezone_name,
            )
        if not item:
            created = True
            item = Item(
                source=source,
                title=title[:500],
                source_name=source.name,
                authors=authors,
                published_at=published_at,
                canonical_url=canonical_url,
                content_type=content_type,
                content_hash=content_hash,
                extraction_confidence=extraction_confidence,
                metadata_json=metadata_json,
                first_seen_at=utcnow(),
            )
            self.db.add(item)
            self.db.flush()
        else:
            if matched_by_content_hash and item.canonical_url != canonical_url:
                self._record_duplicate_mention(item, source, canonical_url, title)
                duplicate_mention_recorded = True
            existing_title = normalize_item_title(item.title, content_type=item.content_type)
            preferred_title = title if len(title) >= len(existing_title) else existing_title
            if preferred_title and preferred_title != item.title:
                item.title = preferred_title[:500]
            item.authors = authors or item.authors
            item.published_at = choose_published_at(item.published_at, published_at)
            item.content_type = content_type
            item.content_hash = content_hash
            if not item.source_name:
                item.source = source
                item.source_name = source.name
            item.metadata_json = merge_metadata_for_data_mode(
                item.metadata_json,
                metadata_json,
                incoming_mode=incoming_data_mode,
            )
            item.extraction_confidence = max(item.extraction_confidence, extraction_confidence)

        if not item.content:
            item.content = ItemContent(item=item)
        if matched_by_content_hash and item.canonical_url != canonical_url:
            if not item.content.cleaned_text or len(cleaned_text) > len(
                item.content.cleaned_text or ""
            ):
                item.content.cleaned_text = cleaned_text[:40000]
                item.content.extracted_text = cleaned_text[:40000]
            if not item.content.raw_payload:
                item.content.raw_payload = raw_payload
            item.content.outbound_links = merge_unique_links(
                item.content.outbound_links, outbound_links
            )
            item.content.word_count = max(item.content.word_count, len(cleaned_text.split()))
            if source.type == SourceType.GMAIL and not item.content.raw_payload_retention_until:
                item.content.raw_payload_retention_until = utcnow() + timedelta(days=30)
        else:
            item.content.cleaned_text = cleaned_text[:40000]
            item.content.extracted_text = cleaned_text[:40000]
            item.content.raw_payload = raw_payload
            item.content.outbound_links = outbound_links[:50]
            item.content.word_count = len(cleaned_text.split())
            item.content.raw_payload_retention_until = (
                utcnow() + timedelta(days=30) if source.type == SourceType.GMAIL else None
            )
        item.metadata_json = merge_metadata_for_data_mode(
            item.metadata_json,
            metadata_json,
            incoming_mode=incoming_data_mode,
        )

        if item.content_type == ContentType.PAPER:
            paper_metadata = self.papers.enrich(
                title=item.title, text=cleaned_text, url=item.canonical_url
            )
            item.metadata_json = merge_metadata_for_data_mode(
                item.metadata_json,
                paper_metadata,
                incoming_mode=incoming_data_mode,
            )

        if insight_payload:
            self._apply_insight_payload(item, insight_payload)

        self.db.add(item)
        self.db.flush()
        self.cluster_service.assign_item(item)
        self.ranking_service.score_item(item)
        self.db.add(item)
        self.db.commit()
        self.db.refresh(item)
        self._record_changed_item_id(item.id)
        current_edition_day = edition_day_for_datetimes(
            published_at=item.published_at,
            first_seen_at=item.first_seen_at,
            timezone_name=timezone_name,
        )
        return IngestPayloadResult(
            item=item,
            created=created,
            duplicate_mention_recorded=duplicate_mention_recorded,
            affected_edition_days=sorted(
                {
                    affected_day
                    for affected_day in [previous_edition_day, current_edition_day]
                    if affected_day is not None
                }
            ),
        )

    def _record_duplicate_mention(
        self, item: Item, source: Source, canonical_url: str, title: str
    ) -> None:
        existing = item.metadata_json.get("duplicate_mentions", [])
        if any(mention.get("canonical_url") == canonical_url for mention in existing):
            return
        duplicate_mentions = [
            *existing,
            {
                "canonical_url": canonical_url,
                "source_name": source.name,
                "title": title[:500],
            },
        ]
        item.metadata_json = item.metadata_json | {"duplicate_mentions": duplicate_mentions}

    def _apply_insight_payload(self, item: Item, insight_payload: dict[str, Any]) -> ItemInsight:
        insight = item.insight or ItemInsight(item=item)
        for field_name in [
            "short_summary",
            "why_it_matters",
            "whats_new",
            "caveats",
            "follow_up_questions",
            "contribution",
            "method",
            "result",
            "limitation",
            "possible_extension",
            "deeper_summary",
            "experiment_ideas",
        ]:
            if field_name not in insight_payload:
                continue
            value = insight_payload[field_name]
            if value is None:
                continue
            setattr(insight, field_name, value)
        self.db.add(insight)
        return insight

    def ensure_insight(self, item: Item) -> ItemInsight:
        insight, _, _ = self.ensure_insight_with_usage(item)
        return insight

    def ensure_insight_with_usage(self, item: Item) -> tuple[ItemInsight, dict[str, int], bool]:
        if item.insight and item.insight.short_summary:
            return item.insight, self._empty_ai_usage(), False
        payload = self.llm.summarize_item(
            {
                "title": item.title,
                "source_name": item.source_name,
                "content_type": item.content_type.value,
            },
            self._analysis_text(item),
        )
        usage = self._normalize_ai_usage(payload.get("_usage"))
        insight = item.insight or ItemInsight(item=item)
        for field_name in [
            "short_summary",
            "why_it_matters",
            "whats_new",
            "caveats",
            "follow_up_questions",
            "contribution",
            "method",
            "result",
            "limitation",
            "possible_extension",
            "deeper_summary",
            "experiment_ideas",
        ]:
            if field_name in payload:
                setattr(insight, field_name, payload[field_name])
        self.db.add(insight)
        return insight, usage, True

    def generate_deeper_summary(self, item_id: str) -> None:
        item = self.db.scalar(
            select(Item)
            .options(selectinload(Item.content), selectinload(Item.insight))
            .where(Item.id == item_id)
        )
        if not item:
            return
        edition_day = edition_day_for_datetimes(
            published_at=item.published_at,
            first_seen_at=item.first_seen_at,
            timezone_name=self.settings.timezone,
        )
        operation_run = self.start_operation_run(
            run_type=IngestionRunType.DEEPER_SUMMARY,
            operation_kind="deeper_summary_generation",
            trigger="item_deeper_summary",
            metadata={
                "title": "Deeper summary generation",
                "summary": "Generating a deeper item summary.",
                "affected_edition_days": [edition_day.isoformat()] if edition_day else [],
                "basic_info": [
                    {"label": "Item", "value": item.title},
                    {"label": "Source", "value": item.source_name},
                    {"label": "Type", "value": item.content_type.value},
                ],
            },
        )
        ai_usage = self._empty_ai_usage()
        try:
            payload = self.llm.deepen_item(
                {
                    "title": item.title,
                    "source_name": item.source_name,
                    "content_type": item.content_type.value,
                },
                self._analysis_text(item),
            )
            ai_usage = self._normalize_ai_usage(payload.get("_usage"))
            insight = item.insight or ItemInsight(item=item)
            insight.deeper_summary = payload.get("deeper_summary")
            insight.experiment_ideas = payload.get("experiment_ideas", [])
            self.db.add(insight)
            self.db.commit()
            ai_cost_usd = self._estimate_ai_cost_usd(
                prompt_tokens=ai_usage["prompt_tokens"],
                completion_tokens=ai_usage["completion_tokens"],
                total_tokens=ai_usage["total_tokens"],
            )
            self.finalize_operation_run(
                operation_run,
                status=RunStatus.SUCCEEDED,
                metadata={
                    "operation_kind": "deeper_summary_generation",
                    "trigger": "item_deeper_summary",
                    "title": "Deeper summary generation",
                    "summary": (
                        f"{len(insight.experiment_ideas or [])} experiment "
                        f"idea{'s' if len(insight.experiment_ideas or []) != 1 else ''} generated."
                    ),
                    "affected_edition_days": [edition_day.isoformat()] if edition_day else [],
                    "ai_prompt_tokens": ai_usage["prompt_tokens"],
                    "ai_completion_tokens": ai_usage["completion_tokens"],
                    "ai_total_tokens": ai_usage["total_tokens"],
                    "ai_cost_usd": ai_cost_usd,
                    "tts_cost_usd": 0.0,
                    "total_cost_usd": ai_cost_usd,
                    "basic_info": [
                        {"label": "Item", "value": item.title},
                        {"label": "Source", "value": item.source_name},
                        {"label": "Type", "value": item.content_type.value},
                        {
                            "label": "Experiment ideas",
                            "value": str(len(insight.experiment_ideas or [])),
                        },
                    ],
                },
            )
        except Exception as exc:
            self.db.rollback()
            ai_cost_usd = self._estimate_ai_cost_usd(
                prompt_tokens=ai_usage["prompt_tokens"],
                completion_tokens=ai_usage["completion_tokens"],
                total_tokens=ai_usage["total_tokens"],
            )
            self.finalize_operation_run(
                operation_run,
                status=RunStatus.FAILED,
                metadata={
                    "operation_kind": "deeper_summary_generation",
                    "trigger": "item_deeper_summary",
                    "title": "Deeper summary generation",
                    "summary": "Deeper summary generation failed.",
                    "affected_edition_days": [edition_day.isoformat()] if edition_day else [],
                    "ai_prompt_tokens": ai_usage["prompt_tokens"],
                    "ai_completion_tokens": ai_usage["completion_tokens"],
                    "ai_total_tokens": ai_usage["total_tokens"],
                    "ai_cost_usd": ai_cost_usd,
                    "tts_cost_usd": 0.0,
                    "total_cost_usd": ai_cost_usd,
                    "basic_info": [
                        {"label": "Item", "value": item.title},
                        {"label": "Source", "value": item.source_name},
                        {"label": "Type", "value": item.content_type.value},
                    ],
                },
                error=str(exc),
            )
            raise

    def retry_failed_runs(self) -> int:
        rerun_count, _ = self.retry_failed_runs_with_affected_edition_days()
        return rerun_count

    def retry_failed_runs_with_affected_edition_days(self) -> tuple[int, set[date]]:
        failed_runs = list(
            self.db.scalars(
                select(IngestionRun)
                .options(selectinload(IngestionRun.source))
                .where(IngestionRun.status == RunStatus.FAILED, IngestionRun.source_id.is_not(None))
                .order_by(IngestionRun.started_at.desc())
                .limit(10)
            ).all()
        )
        cycle_run = IngestionRun(
            run_type=IngestionRunType.INGEST,
            status=RunStatus.RUNNING,
            metadata_json={"scope": "cycle", "trigger": "retry", "source_count": len(failed_runs)},
        )
        self.db.add(cycle_run)
        self.db.commit()

        rerun_count = 0
        affected_edition_days: set[date] = set()
        source_summaries: list[SourceRunSummary] = []
        try:
            for run in failed_runs:
                if not run.source:
                    continue
                try:
                    summary = self._run_source_with_summary(run.source)
                    rerun_count += summary.ingested_count
                    affected_edition_days.update(summary.affected_edition_days)
                    source_summaries.append(summary)
                except SourceIngestionError as exc:
                    affected_edition_days.update(exc.summary.affected_edition_days)
                    source_summaries.append(exc.summary)

            cycle_run.status = (
                RunStatus.FAILED
                if any(summary.status == RunStatus.FAILED for summary in source_summaries)
                else RunStatus.SUCCEEDED
            )
            cycle_run.metadata_json = self._build_cycle_metadata(
                trigger="retry", source_summaries=source_summaries
            )
            cycle_run.error = "\n".join(self._build_cycle_errors(source_summaries)) or None
            cycle_run.finished_at = utcnow()
            self.db.add(cycle_run)
            self.db.commit()
            return rerun_count, affected_edition_days
        except Exception as exc:
            cycle_run.status = RunStatus.FAILED
            cycle_run.error = str(exc)
            cycle_run.metadata_json = self._build_cycle_metadata(
                trigger="retry", source_summaries=source_summaries
            )
            cycle_run.finished_at = utcnow()
            self.db.add(cycle_run)
            self.db.commit()
            raise

    def list_recent_ingestion_cycles(self, limit: int = 12) -> list[dict[str, Any]]:
        runs = list(
            self.db.scalars(
                select(IngestionRun)
                .where(IngestionRun.source_id.is_(None))
                .order_by(IngestionRun.started_at.desc())
                .limit(limit)
            ).all()
        )
        return [self._history_entry_from_run(run) for run in runs]

    def purge_old_email_payloads(self) -> int:
        items = list(
            self.db.scalars(
                select(ItemContent).where(
                    ItemContent.raw_payload_retention_until.is_not(None),
                    ItemContent.raw_payload_retention_until < utcnow(),
                    ItemContent.raw_payload_purged_at.is_(None),
                )
            ).all()
        )
        for content in items:
            content.raw_payload = {}
            content.raw_payload_purged_at = utcnow()
            self.db.add(content)
        self.db.commit()
        return len(items)

    def _load_feed(self, locator: str):
        response = fetch_safe_response(
            locator,
            timeout=WEBSITE_INDEX_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        response_headers = dict(response.headers)
        response_headers["content-location"] = str(response.url)
        return feedparser.parse(response.content, response_headers=response_headers)

    def _feed_source_config(self, source: Source) -> dict[str, Any]:
        return source.config_json if isinstance(source.config_json, dict) else {}

    def _feed_source_discovery_mode(self, source: Source) -> str:
        return str(self._feed_source_config(source).get("discovery_mode") or "").strip().lower()

    def _website_index_source_url(self, source: Source) -> str:
        config = self._feed_source_config(source)
        for candidate in (
            config.get("website_index_url"),
            source.url,
            config.get("website_url"),
        ):
            if candidate and str(candidate).strip():
                return str(candidate).strip()
        return ""

    def _website_index_max_links(self, source: Source) -> int:
        config = self._feed_source_config(source)
        return max(
            1,
            self._read_int(
                config.get("max_links"), default=WEBSITE_INDEX_DEFAULT_MAX_LINKS
            ),
        )

    def _website_index_entry_title(self, anchor: Any, link: str) -> str:
        heading = anchor.find(["h1", "h2", "h3", "h4", "h5", "h6"])
        if heading:
            heading_text = re.sub(r"\s+", " ", heading.get_text(" ", strip=True))
            if heading_text:
                return heading_text

        title_candidates = [
            anchor.get_text(" ", strip=True),
            anchor.get("aria-label"),
            anchor.get("title"),
        ]
        for candidate in title_candidates:
            if str(candidate or "").strip():
                return re.sub(r"\s+", " ", str(candidate).strip())

        path_segment = urlparse(link).path.rstrip("/").split("/")[-1]
        fallback_title = path_segment.replace("-", " ").replace("_", " ").strip()
        if fallback_title:
            return fallback_title.title()
        return "Untitled item"

    def _discover_website_index_entries(self, source: Source) -> list[dict[str, Any]]:
        index_url = self._website_index_source_url(source)
        if not index_url:
            raise RuntimeError("Source is missing a website index URL.")

        response = fetch_safe_response(
            index_url,
            timeout=WEBSITE_INDEX_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()

        config = self._feed_source_config(source)
        base_url = str(response.url)
        allowed_hosts = {
            (urlparse(index_url).hostname or "").lower(),
            (urlparse(base_url).hostname or "").lower(),
        }
        configured_hosts = config.get("allowed_hosts") or []
        if isinstance(configured_hosts, list):
            for raw_host in configured_hosts:
                host = str(raw_host or "").strip().lower()
                if host:
                    allowed_hosts.add(host)
        allowed_hosts.discard("")

        raw_prefixes = config.get("article_path_prefixes") or []
        path_prefixes = tuple(
            prefix if prefix.startswith("/") else f"/{prefix}"
            for prefix in (
                str(raw_prefix or "").strip() for raw_prefix in raw_prefixes if raw_prefix
            )
            if prefix
        )

        exclude_patterns = tuple(
            str(pattern or "").strip()
            for pattern in (config.get("exclude_patterns") or [])
            if str(pattern or "").strip()
        )

        entries: list[dict[str, Any]] = []
        seen_links: set[str] = set()
        soup = BeautifulSoup(response.text, "html.parser")
        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href") or "").strip()
            if not href or href.startswith("#") or href.lower().startswith(
                ("javascript:", "mailto:", "tel:")
            ):
                continue

            normalized_link = normalize_url(urljoin(base_url, href))
            parsed_link = urlparse(normalized_link)
            hostname = (parsed_link.hostname or "").lower()
            if allowed_hosts and hostname not in allowed_hosts:
                continue

            path = parsed_link.path or "/"
            if path_prefixes and not any(path.startswith(prefix) for prefix in path_prefixes):
                continue

            link_text = path if not parsed_link.query else f"{path}?{parsed_link.query}"
            if exclude_patterns and any(
                re.search(pattern, link_text, flags=re.IGNORECASE)
                for pattern in exclude_patterns
            ):
                continue

            if normalized_link in seen_links:
                continue
            seen_links.add(normalized_link)

            entries.append(
                {
                    "link": normalized_link,
                    "title": self._website_index_entry_title(anchor, normalized_link),
                }
            )
        return entries

    def _probe_website_index_source(self, source: Source) -> dict[str, Any]:
        index_url = self._website_index_source_url(source)
        try:
            entries = self._discover_website_index_entries(source)
        except Exception as exc:
            raise SourceProbeError(f"Could not read the website index: {exc}") from exc

        sample_titles = [str(entry.get("title") or "Untitled item").strip() for entry in entries[:5]]
        detail = (
            f"Lightweight check found {len(entries)} item{'' if len(entries) == 1 else 's'} "
            f"in {source.name} from {index_url}."
        )
        return self._build_source_probe_result(
            source=source,
            total_found=len(entries),
            sample_titles=sample_titles,
            detail=detail,
        )

    def _probe_feed_source(self, source: Source) -> dict[str, Any]:
        if self._feed_source_discovery_mode(source) == "website_index":
            return self._probe_website_index_source(source)

        locator = self._resolve_feed_source_locator(source)
        if not locator:
            raise SourceProbeError("Source is missing a feed URL or query.")

        try:
            parsed = self._load_feed(locator)
        except Exception as exc:
            raise SourceProbeError(f"Could not read the feed: {exc}") from exc

        entries = list(getattr(parsed, "entries", []) or [])
        if getattr(parsed, "bozo", False) and not entries:
            bozo_exception = getattr(parsed, "bozo_exception", None)
            detail = (
                str(bozo_exception).strip() if bozo_exception else "The feed could not be parsed."
            )
            raise SourceProbeError(f"Could not read the feed: {detail}")

        sample_titles = [
            (getattr(entry, "title", None) or "Untitled item").strip() for entry in entries[:5]
        ]
        feed_title = (
            str((getattr(parsed, "feed", {}) or {}).get("title") or source.name).strip()
            or source.name
        )
        entry_label = "paper" if source.type == SourceType.ARXIV else "item"
        detail = f"Lightweight check found {len(entries)} {entry_label}{'' if len(entries) == 1 else 's'} in {feed_title}."
        return self._build_source_probe_result(
            source=source,
            total_found=len(entries),
            sample_titles=sample_titles,
            detail=detail,
        )

    def _ingest_website_index_source(self, source: Source) -> SourceRunSummary:
        index_url = self._website_index_source_url(source)
        source_started_at = perf_counter()
        self._log_active_source_step(
            f"Fetching website index from {self._clip_log_text(index_url, limit=120)}."
        )
        entries = self._discover_website_index_entries(source)
        processed_entries = entries[: self._website_index_max_links(source)]
        self._log_active_source_step(
            f'Parsed {len(entries)} article link{"s" if len(entries) != 1 else ""} from '
            f'"{self._clip_log_text(source.name, limit=72)}"; processing {len(processed_entries)}.'
        )

        items: list[IngestionRunItemSummary] = []
        affected_edition_days: set[date] = set()
        extractor_fallback_count = 0
        for entry_index, entry in enumerate(processed_entries, start=1):
            item_index = len(items)
            link = str(entry.get("link") or index_url).strip()
            title = str(entry.get("title") or "Untitled item").strip() or "Untitled item"
            cleaned_text = title
            outbound_links: list[str] = []
            extraction_confidence = 0.15
            published_at: datetime | None = None
            raw_payload = {
                "website_index_entry": {
                    "title": title,
                    "link": link,
                    "index_url": index_url,
                }
            }

            if link:
                try:
                    extracted = self.extractor.extract_from_url(link)
                    title = extracted.title or title
                    cleaned_text = extracted.cleaned_text or title
                    outbound_links = extracted.outbound_links
                    extraction_confidence = extracted.extraction_confidence
                    published_at = extracted.published_at
                    raw_payload = raw_payload | extracted.raw_payload
                except Exception:
                    cleaned_text = title
                    extractor_fallback_count += 1

            result = self._ingest_payload_with_result(
                source=source,
                title=title,
                canonical_url=link or f"{source.id}-{item_index}",
                authors=[],
                published_at=published_at or utcnow(),
                cleaned_text=cleaned_text,
                raw_payload=raw_payload,
                outbound_links=outbound_links,
                extraction_confidence=extraction_confidence,
                metadata_json={
                    "feed_title": source.name,
                    "source_type": source.type.value,
                    "discovery_mode": "website_index",
                    "website_index_url": index_url,
                },
                content_type=None,
            )
            items.append(
                IngestionRunItemSummary(
                    title=result.item.title,
                    outcome="created" if result.created else "updated",
                    content_type=result.item.content_type.value,
                    extraction_confidence=result.item.extraction_confidence,
                    duplicate_mention=result.duplicate_mention_recorded,
                )
            )
            affected_edition_days.update(result.affected_edition_days)
            if entry_index % 5 == 0 and entry_index < len(processed_entries):
                progress_message = (
                    f"Processed {entry_index}/{len(processed_entries)} website entr"
                    f"{'y' if entry_index == 1 else 'ies'} in "
                    f"{self._format_duration(perf_counter() - source_started_at)}"
                )
                if extractor_fallback_count:
                    progress_message += (
                        f" ({extractor_fallback_count} extractor fallback"
                        f"{'s' if extractor_fallback_count != 1 else ''})"
                    )
                self._log_active_source_step(progress_message + ".")

        return SourceRunSummary(
            source_id=source.id,
            source_name=source.name,
            status=RunStatus.SUCCEEDED,
            items=items,
            extractor_fallback_count=extractor_fallback_count,
            affected_edition_days=sorted(affected_edition_days),
        )

    def _ingest_feed_source(self, source: Source) -> SourceRunSummary:
        if self._feed_source_discovery_mode(source) == "website_index":
            return self._ingest_website_index_source(source)

        feed_locator = self._resolve_feed_source_locator(source)
        if not feed_locator:
            raise RuntimeError("Source is missing a feed URL or query.")

        source_started_at = perf_counter()
        self._log_active_source_step(
            f"Fetching feed from {self._clip_log_text(feed_locator, limit=120)}."
        )
        parsed = self._load_feed(feed_locator)
        entries = list(getattr(parsed, "entries", []) or [])
        processed_entries = entries[:20]
        feed_title = (
            str((getattr(parsed, "feed", {}) or {}).get("title") or source.name).strip()
            or source.name
        )
        self._log_active_source_step(
            f'Parsed {len(entries)} feed entr{"y" if len(entries) == 1 else "ies"} from "{self._clip_log_text(feed_title, limit=72)}"; '
            f"processing {len(processed_entries)}."
        )
        items: list[IngestionRunItemSummary] = []
        affected_edition_days: set[date] = set()
        extractor_fallback_count = 0
        for entry_index, entry in enumerate(processed_entries, start=1):
            item_index = len(items)
            link = getattr(entry, "link", None) or feed_locator or source.url or source.query or ""
            title = getattr(entry, "title", "Untitled item")
            fallback_author = entry.author if getattr(entry, "author", None) else None
            authors = [
                author.get("name") for author in getattr(entry, "authors", []) if author.get("name")
            ] or ([fallback_author] if fallback_author else [])
            published_at = parse_datetime(
                getattr(entry, "published", None)
                or getattr(entry, "updated", None)
                or getattr(entry, "created", None)
            )
            summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
            cleaned_text = summary
            outbound_links: list[str] = []
            extraction_confidence = 0.35 if summary else 0.15
            raw_payload = {
                "feed_entry": {
                    "title": title,
                    "link": link,
                    "published": getattr(entry, "published", None)
                    or getattr(entry, "updated", None),
                    "summary": summary[:4000],
                }
            }

            if link:
                try:
                    extracted = self.extractor.extract_from_url(link)
                    title = extracted.title or title
                    cleaned_text = extracted.cleaned_text or summary or title
                    outbound_links = extracted.outbound_links
                    extraction_confidence = extracted.extraction_confidence
                    raw_payload = raw_payload | extracted.raw_payload
                except Exception:
                    cleaned_text = summary or title
                    extractor_fallback_count += 1

            result = self._ingest_payload_with_result(
                source=source,
                title=title,
                canonical_url=link or f"{source.id}-{item_index}",
                authors=authors,
                published_at=published_at or utcnow(),
                cleaned_text=cleaned_text,
                raw_payload=raw_payload,
                outbound_links=outbound_links,
                extraction_confidence=extraction_confidence,
                metadata_json={
                    "feed_title": parsed.feed.get("title"),
                    "source_type": source.type.value,
                    **(
                        {"abstract_text": self._normalize_analysis_text(summary)}
                        if source.type == SourceType.ARXIV and summary
                        else {}
                    ),
                },
                content_type=ContentType.PAPER if source.type == SourceType.ARXIV else None,
            )
            items.append(
                IngestionRunItemSummary(
                    title=result.item.title,
                    outcome="created" if result.created else "updated",
                    content_type=result.item.content_type.value,
                    extraction_confidence=result.item.extraction_confidence,
                    duplicate_mention=result.duplicate_mention_recorded,
                )
            )
            affected_edition_days.update(result.affected_edition_days)
            if entry_index % 5 == 0 and entry_index < len(processed_entries):
                progress_message = (
                    f"Processed {entry_index}/{len(processed_entries)} feed entr"
                    f"{'y' if entry_index == 1 else 'ies'} in {self._format_duration(perf_counter() - source_started_at)}"
                )
                if extractor_fallback_count:
                    progress_message += (
                        f" ({extractor_fallback_count} extractor fallback"
                        f"{'s' if extractor_fallback_count != 1 else ''})"
                    )
                self._log_active_source_step(progress_message + ".")
        return SourceRunSummary(
            source_id=source.id,
            source_name=source.name,
            status=RunStatus.SUCCEEDED,
            items=items,
            extractor_fallback_count=extractor_fallback_count,
            affected_edition_days=sorted(affected_edition_days),
        )

    def _probe_gmail_source(self, source: Source) -> dict[str, Any]:
        payload = self.connection_service.get_valid_gmail_payload()
        if not payload:
            raise SourceProbeError("Gmail connection is missing credentials.")

        senders, labels, raw_query = self._resolve_gmail_source_filters(source)
        connector = self._build_gmail_connector(payload)
        try:
            messages = connector.list_newsletters(
                senders=senders,
                labels=labels,
                raw_query=raw_query,
                max_results=20,
                newer_than_days=SOURCE_PROBE_GMAIL_WINDOW_DAYS,
            )
        except Exception as exc:
            raise SourceProbeError(f"Could not read Gmail for this source: {exc}") from exc

        sample_titles = [
            (message.subject or "Untitled message").strip() for message in messages[:5]
        ]
        filter_description = self._describe_gmail_probe_filter(
            senders=senders,
            labels=labels,
            raw_query=raw_query,
        )
        detail = (
            f"Lightweight inbox check found {len(messages)} message{'' if len(messages) == 1 else 's'} "
            f"in the last {SOURCE_PROBE_GMAIL_WINDOW_DAYS} days using {filter_description}."
        )
        return self._build_source_probe_result(
            source=source,
            total_found=len(messages),
            sample_titles=sample_titles,
            detail=detail,
        )

    def _ingest_gmail_source(self, source: Source) -> SourceRunSummary:
        payload = self.connection_service.get_valid_gmail_payload()
        if not payload:
            raise RuntimeError("Gmail connection is missing credentials.")

        senders, labels, raw_query = self._resolve_gmail_source_filters(source)
        connector = self._build_gmail_connector(payload)
        filter_description = self._describe_gmail_probe_filter(
            senders=senders,
            labels=labels,
            raw_query=raw_query,
        )
        self._log_active_source_step(f"Fetching Gmail messages using {filter_description}.")
        message_fetch_started_at = perf_counter()
        messages = connector.list_newsletters(
            senders=senders,
            labels=labels,
            raw_query=raw_query,
            max_results=20,
        )
        self._log_active_source_step(
            f"Loaded {len(messages)} newsletter message{'s' if len(messages) != 1 else ''} from Gmail in "
            f"{self._format_duration(perf_counter() - message_fetch_started_at)}."
        )
        items: list[IngestionRunItemSummary] = []
        affected_edition_days: set[date] = set()
        ai_usage = self._empty_ai_usage()
        for message_index, message in enumerate(messages, start=1):
            message_items, message_days, message_ai_usage = self._ingest_gmail_message_facts(
                source,
                message,
                message_index=message_index,
                message_count=len(messages),
            )
            items.extend(message_items)
            affected_edition_days.update(message_days)
            ai_usage = self._merge_ai_usage(ai_usage, message_ai_usage)
        return SourceRunSummary(
            source_id=source.id,
            source_name=source.name,
            status=RunStatus.SUCCEEDED,
            items=items,
            ai_prompt_tokens=ai_usage["prompt_tokens"],
            ai_completion_tokens=ai_usage["completion_tokens"],
            ai_total_tokens=ai_usage["total_tokens"],
            affected_edition_days=sorted(affected_edition_days),
        )

    def _ingest_gmail_message_facts(
        self,
        source: Source,
        message,
        *,
        message_index: int | None = None,
        message_count: int | None = None,
    ) -> tuple[list[IngestionRunItemSummary], set[date], dict[str, int]]:
        message_label = self._build_gmail_message_log_label(
            message_subject=message.subject,
            message_index=message_index,
            message_count=message_count,
        )
        newsletter_context = {
            "source_name": source.name,
            "subject": message.subject,
            "sender": message.sender,
            "published_at": message.published_at.isoformat(),
            "text_body": message.text_body,
            "outbound_links": message.outbound_links,
        }
        self._log_active_source_step(f"{message_label}: splitting newsletter into facts.")
        split_started_at = perf_counter()
        payload = self.llm.split_newsletter_message(newsletter_context)
        usage = self._normalize_ai_usage(payload.get("_usage"))
        facts = payload.get("facts") or [
            {
                "headline": message.subject,
                "summary": message.text_body or message.subject,
                "why_it_matters": message.subject,
            }
        ]
        generation_mode = str(payload.get("generation_mode") or "heuristic")
        fact_count = max(1, len(facts))
        split_message = (
            f"{message_label}: identified {fact_count} fact{'s' if fact_count != 1 else ''} in "
            f"{self._format_duration(perf_counter() - split_started_at)}"
        )
        split_details = [f"mode={generation_mode}"]
        if usage["total_tokens"]:
            split_details.append(f"tokens={usage['total_tokens']}")
        self._log_active_source_step(f"{split_message} ({', '.join(split_details)}).")
        summaries: list[IngestionRunItemSummary] = []
        affected_edition_days: set[date] = set()
        ingest_started_at = perf_counter()
        created_count = 0
        updated_count = 0
        for fact_index, fact in enumerate(facts, start=1):
            normalized_fact = self.llm.expand_newsletter_fact(
                newsletter_context,
                fact if isinstance(fact, dict) else {},
            )
            fact_links = self._newsletter_fact_links(message, normalized_fact)
            canonical_url = (
                message.permalink
                if fact_index == 1
                else self._build_newsletter_fact_permalink(message.permalink, fact_index)
            )
            result = self._ingest_payload_with_result(
                source=source,
                title=str(normalized_fact.get("headline") or message.subject),
                canonical_url=canonical_url,
                authors=[message.sender],
                published_at=message.published_at,
                cleaned_text=self._build_newsletter_fact_text(
                    message, normalized_fact, fact_links=fact_links
                ),
                raw_payload=self._build_newsletter_fact_raw_payload(
                    message,
                    normalized_fact,
                    fact_index=fact_index,
                ),
                outbound_links=fact_links,
                extraction_confidence=0.96,
                metadata_json={
                    "thread_id": message.thread_id,
                    "sender": message.sender,
                    "source_type": source.type.value,
                    "newsletter_message_id": message.message_id,
                    "newsletter_message_subject": message.subject,
                    "newsletter_parent_permalink": message.permalink,
                    "newsletter_fact_index": fact_index,
                    "newsletter_fact_count": fact_count,
                    "newsletter_fact_generation_mode": generation_mode,
                    "newsletter_relevant_links": fact_links,
                },
                content_type=ContentType.NEWSLETTER,
                insight_payload={
                    "short_summary": str(normalized_fact.get("summary") or message.subject),
                    "why_it_matters": str(
                        normalized_fact.get("why_it_matters")
                        or normalized_fact.get("summary")
                        or message.subject
                    ),
                    "whats_new": str(
                        normalized_fact.get("whats_new")
                        or normalized_fact.get("summary")
                        or message.subject
                    ),
                    "caveats": str(
                        normalized_fact.get("caveats")
                        or "Derived from a newsletter email fact and should be checked against the linked source before acting."
                    ),
                    "follow_up_questions": normalized_fact.get("follow_up_questions") or [],
                },
            )
            summaries.append(
                IngestionRunItemSummary(
                    title=result.item.title,
                    outcome="created" if result.created else "updated",
                    content_type=result.item.content_type.value,
                    extraction_confidence=result.item.extraction_confidence,
                    duplicate_mention=result.duplicate_mention_recorded,
                )
            )
            affected_edition_days.update(result.affected_edition_days)
            if result.created:
                created_count += 1
            else:
                updated_count += 1
            if fact_index % 10 == 0 and fact_index < fact_count:
                self._log_active_source_step(
                    f"{message_label}: saved {fact_index}/{fact_count} fact{'s' if fact_count != 1 else ''} in "
                    f"{self._format_duration(perf_counter() - ingest_started_at)}."
                )

        archived_count = self._archive_stale_newsletter_fact_items(
            source=source,
            message_permalink=message.permalink,
            active_fact_count=fact_count,
        )
        final_details = [f"{created_count} new", f"{updated_count} refreshed"]
        if archived_count:
            final_details.append(f"{archived_count} archived as superseded")
        self._log_active_source_step(
            f"{message_label}: stored {fact_count} fact{'s' if fact_count != 1 else ''} in "
            f"{self._format_duration(perf_counter() - ingest_started_at)} ({', '.join(final_details)})."
        )
        return summaries, affected_edition_days, usage

    def _build_newsletter_fact_text(
        self, message, fact: dict[str, Any], *, fact_links: list[str]
    ) -> str:
        parts = [
            f"Newsletter: {message.subject}",
            f"From: {message.sender}",
            str(fact.get("summary") or message.subject),
            f"Why it matters: {str(fact.get('why_it_matters') or fact.get('summary') or message.subject)}",
            f"What's new: {str(fact.get('whats_new') or fact.get('summary') or message.subject)}",
            f"Caveats: {str(fact.get('caveats') or 'Check the linked source before acting.')}",
        ]
        if fact_links:
            parts.append("Relevant links:\n" + "\n".join(f"- {link}" for link in fact_links))
        return "\n\n".join(part.strip() for part in parts if str(part).strip())

    def _build_newsletter_fact_raw_payload(
        self,
        message,
        fact: dict[str, Any],
        *,
        fact_index: int,
    ) -> dict[str, Any]:
        payload = {
            "sender": message.sender,
            "message_id": message.message_id,
            "thread_id": message.thread_id,
            "subject": message.subject,
            "fact_index": fact_index,
            "fact": {
                "headline": fact.get("headline"),
                "summary": fact.get("summary"),
                "why_it_matters": fact.get("why_it_matters"),
                "whats_new": fact.get("whats_new"),
                "caveats": fact.get("caveats"),
                "follow_up_questions": fact.get("follow_up_questions") or [],
                "relevant_links": fact.get("relevant_links") or [],
            },
        }
        if fact_index == 1:
            payload["html"] = message.html_body
            payload["text_body"] = message.text_body
        return payload

    def _newsletter_fact_links(self, message, fact: dict[str, Any]) -> list[str]:
        relevant_links = fact.get("relevant_links")
        if isinstance(relevant_links, list):
            cleaned = [str(link).strip() for link in relevant_links if str(link).strip()]
            if cleaned:
                return cleaned[:50]
        return message.outbound_links[:50]

    def _build_newsletter_fact_permalink(self, permalink: str, fact_index: int) -> str:
        parsed = urlparse(permalink)
        query = urlencode(
            [
                (key, value)
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if key != NEWSLETTER_FACT_QUERY_KEY
            ]
            + [(NEWSLETTER_FACT_QUERY_KEY, str(fact_index))]
        )
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", query, parsed.fragment))

    def _archive_stale_newsletter_fact_items(
        self,
        *,
        source: Source,
        message_permalink: str,
        active_fact_count: int,
    ) -> int:
        archived_count = 0
        for fact_index in range(active_fact_count + 1, NEWSLETTER_FACT_URL_LIMIT + 1):
            stale_url = normalize_url(
                self._build_newsletter_fact_permalink(message_permalink, fact_index)
            )
            stale_item = self.db.scalar(select(Item).where(Item.canonical_url == stale_url))
            if not stale_item or stale_item.source_id != source.id:
                continue
            stale_item.triage_status = TriageStatus.ARCHIVED
            stale_item.metadata_json = stale_item.metadata_json | {
                "newsletter_fact_superseded": True,
            }
            self.db.add(stale_item)
            archived_count += 1
        return archived_count

    def _resolve_gmail_source_filters(
        self, source: Source
    ) -> tuple[list[str], list[str], str | None]:
        query = (source.query or "").strip()
        if query:
            query_parts = [part.strip() for part in re.split(r"[,\n;]+", query) if part.strip()]
            if query_parts and all(EMAIL_QUERY_RE.fullmatch(part) for part in query_parts):
                return query_parts, [], None
            return [], [], query

        rules = source.rules or []
        senders = [
            rule.value.strip()
            for rule in rules
            if rule.active and rule.rule_type in {"sender", "email"} and rule.value.strip()
        ]
        labels = [
            rule.value.strip()
            for rule in rules
            if rule.active and rule.rule_type == "label" and rule.value.strip()
        ]
        return senders, labels, None

    def _build_gmail_connector(
        self, payload: dict[str, Any]
    ) -> GmailConnector | GmailImapConnector:
        auth_mode = str(payload.get("auth_mode") or "").strip().lower()
        if auth_mode == "app_password":
            return GmailImapConnector(
                email_address=str(payload.get("email") or ""),
                app_password=str(payload.get("app_password") or ""),
            )
        if payload.get("access_token"):
            return GmailConnector(str(payload["access_token"]))
        raise RuntimeError("Gmail connection is missing usable credentials.")

    def _describe_gmail_probe_filter(
        self,
        *,
        senders: list[str],
        labels: list[str],
        raw_query: str | None,
    ) -> str:
        filter_parts: list[str] = []
        if raw_query:
            filter_parts.append(f'Gmail query "{raw_query}"')
        if senders:
            sender_label = "sender" if len(senders) == 1 else "senders"
            filter_parts.append(f"{sender_label} {', '.join(senders)}")
        if labels:
            label_name = "label" if len(labels) == 1 else "labels"
            filter_parts.append(f"{label_name} {', '.join(labels)}")
        if not filter_parts:
            return "the default Gmail source settings"
        return " and ".join(filter_parts)

    def _resolve_feed_source_locator(self, source: Source) -> str:
        if source.url and source.url.strip():
            return source.url.strip()

        query = (source.query or "").strip()
        if not query:
            return ""
        if source.type == SourceType.ARXIV and not query.lower().startswith(
            ("http://", "https://")
        ):
            return f"{ARXIV_API_BASE}?{urlencode({'search_query': query, 'start': 0, 'max_results': 20, 'sortBy': 'submittedDate', 'sortOrder': 'descending'})}"
        return query

    def _build_source_probe_result(
        self,
        *,
        source: Source,
        total_found: int,
        sample_titles: list[str],
        detail: str,
    ) -> dict[str, Any]:
        return {
            "source_id": source.id,
            "source_name": source.name,
            "source_type": source.type,
            "total_found": total_found,
            "sample_titles": sample_titles,
            "detail": detail,
            "checked_at": utcnow(),
        }

    def _build_cycle_errors(self, source_summaries: list[SourceRunSummary]) -> list[str]:
        return [
            f"{summary.source_name}: {summary.error}"
            for summary in source_summaries
            if summary.error
        ]

    def _build_ingest_summary(
        self, *, total_titles: int, source_count: int, failed_source_count: int
    ) -> str:
        summary = f"{total_titles} title{'s' if total_titles != 1 else ''} across {source_count} source{'s' if source_count != 1 else ''}"
        if failed_source_count:
            summary = f"{summary} · {failed_source_count} failure{'s' if failed_source_count != 1 else ''}"
        return summary

    def _build_ingest_basic_info(
        self,
        *,
        source_count: int,
        failed_source_count: int,
        total_titles: int,
        created_count: int,
        updated_count: int,
        extractor_fallback_count: int,
    ) -> list[dict[str, str]]:
        source_value = f"{source_count} processed"
        if failed_source_count:
            source_value = f"{source_value} · {failed_source_count} failed"
        return [
            {"label": "Sources", "value": source_value},
            {"label": "Titles", "value": f"{total_titles} extracted"},
            {
                "label": "Created vs updated",
                "value": f"{created_count} new · {updated_count} refreshed",
            },
            {"label": "Extractor fallbacks", "value": str(extractor_fallback_count)},
        ]

    def _build_cycle_metadata(
        self, *, trigger: str, source_summaries: list[SourceRunSummary]
    ) -> dict[str, Any]:
        all_items = [item for summary in source_summaries for item in summary.items]
        ai_usage = self._merge_ai_usage(
            *[
                {
                    "prompt_tokens": summary.ai_prompt_tokens,
                    "completion_tokens": summary.ai_completion_tokens,
                    "total_tokens": summary.ai_total_tokens,
                }
                for summary in source_summaries
            ]
        )
        source_count = len(source_summaries)
        failed_source_count = sum(
            1 for summary in source_summaries if summary.status == RunStatus.FAILED
        )
        total_titles = len(all_items)
        created_count = sum(1 for item in all_items if item.outcome == "created")
        updated_count = sum(1 for item in all_items if item.outcome == "updated")
        duplicate_mention_count = sum(1 for item in all_items if item.duplicate_mention)
        extractor_fallback_count = sum(
            summary.extractor_fallback_count for summary in source_summaries
        )
        operation_kind = "ingest_retry" if trigger == "retry" else "ingest_cycle"
        title = "Retry ingest cycle" if trigger == "retry" else "Ingest cycle"
        summary = self._build_ingest_summary(
            total_titles=total_titles,
            source_count=source_count,
            failed_source_count=failed_source_count,
        )
        average_extraction_confidence = (
            round(sum(item.extraction_confidence for item in all_items) / len(all_items), 3)
            if all_items
            else None
        )
        return {
            "scope": "cycle",
            "operation_kind": operation_kind,
            "trigger": trigger,
            "title": title,
            "summary": summary,
            "source_count": source_count,
            "failed_source_count": failed_source_count,
            "ingested_count": total_titles,
            "created_count": created_count,
            "updated_count": updated_count,
            "duplicate_mention_count": duplicate_mention_count,
            "extractor_fallback_count": extractor_fallback_count,
            "affected_edition_days": sorted(
                {
                    affected_day.isoformat()
                    for summary in source_summaries
                    for affected_day in summary.affected_edition_days
                }
            ),
            "ai_prompt_tokens": ai_usage["prompt_tokens"],
            "ai_completion_tokens": ai_usage["completion_tokens"],
            "ai_total_tokens": ai_usage["total_tokens"],
            "ai_cost_usd": self._estimate_ai_cost_usd(
                prompt_tokens=ai_usage["prompt_tokens"],
                completion_tokens=ai_usage["completion_tokens"],
                total_tokens=ai_usage["total_tokens"],
            ),
            "tts_cost_usd": 0.0,
            "total_cost_usd": self._estimate_ai_cost_usd(
                prompt_tokens=ai_usage["prompt_tokens"],
                completion_tokens=ai_usage["completion_tokens"],
                total_tokens=ai_usage["total_tokens"],
            ),
            "average_extraction_confidence": average_extraction_confidence,
            "basic_info": self._build_ingest_basic_info(
                source_count=source_count,
                failed_source_count=failed_source_count,
                total_titles=total_titles,
                created_count=created_count,
                updated_count=updated_count,
                extractor_fallback_count=extractor_fallback_count,
            ),
            "source_stats": [summary.to_metadata() for summary in source_summaries],
            "errors": self._build_cycle_errors(source_summaries),
        }

    def _default_operation_title(self, *, operation_kind: str, run_type: IngestionRunType) -> str:
        if operation_kind == "ingest_retry":
            return "Retry ingest cycle"
        if operation_kind == "ingest_cycle":
            return "Ingest cycle"
        if operation_kind == "database_backup":
            return "Database backup"
        if operation_kind == "brief_generation":
            return "Brief generation"
        if operation_kind == "audio_generation":
            return "Audio brief generation"
        if operation_kind == "item_insight_generation":
            return "Item insight generation"
        if operation_kind == "deeper_summary_generation":
            return "Deeper summary generation"
        if operation_kind == "zotero_export":
            return "Save to Zotero"
        if operation_kind == "post_ingest_enrichment":
            return "Post-ingest enrichment"
        if operation_kind == "corpus_enrichment_backfill":
            return "Corpus enrichment backfill"
        if run_type == IngestionRunType.DIGEST:
            return "Digest operation"
        return run_type.value.replace("_", " ").title()

    def _default_operation_kind(self, *, run_type: IngestionRunType, trigger: str) -> str:
        if run_type == IngestionRunType.INGEST:
            return "ingest_retry" if trigger == "retry" else "ingest_cycle"
        if run_type == IngestionRunType.DIGEST:
            return "brief_generation"
        if run_type == IngestionRunType.ZOTERO_SYNC:
            return "zotero_sync"
        if run_type == IngestionRunType.CLEANUP:
            return "cleanup"
        if run_type == IngestionRunType.DEEPER_SUMMARY:
            return "deeper_summary"
        return run_type.value

    def _default_operation_summary(
        self,
        *,
        operation_kind: str,
        total_titles: int,
        source_count: int,
        failed_source_count: int,
    ) -> str:
        if operation_kind in {"ingest_cycle", "ingest_retry"}:
            return self._build_ingest_summary(
                total_titles=total_titles,
                source_count=source_count,
                failed_source_count=failed_source_count,
            )
        if operation_kind == "database_backup":
            return "Created a database backup snapshot."
        if operation_kind == "brief_generation":
            return "Generated a daily brief."
        if operation_kind == "audio_generation":
            return "Generated an audio version of the brief."
        if operation_kind == "item_insight_generation":
            return "Generated insight for one item."
        if operation_kind == "deeper_summary_generation":
            return "Generated a deeper item summary."
        if operation_kind == "zotero_export":
            return "Saved one item to Zotero."
        if operation_kind == "post_ingest_enrichment":
            return "Enriched newly ingested items."
        if operation_kind == "corpus_enrichment_backfill":
            return "Ran a corpus-wide enrichment backfill."
        return "Operation completed."

    def _history_entry_from_run(self, run: IngestionRun) -> dict[str, Any]:
        metadata = run.metadata_json if isinstance(run.metadata_json, dict) else {}
        source_stats = self._normalize_history_source_stats(metadata.get("source_stats"))
        total_titles = self._read_int(
            metadata.get("ingested_count"),
            default=sum(stat["ingested_count"] for stat in source_stats),
        )
        source_count = self._read_int(metadata.get("source_count"), default=len(source_stats))
        failed_source_count = self._read_int(
            metadata.get("failed_source_count"),
            default=sum(1 for stat in source_stats if stat["status"] == RunStatus.FAILED.value),
        )
        created_count = self._read_int(
            metadata.get("created_count"),
            default=sum(stat["created_count"] for stat in source_stats),
        )
        updated_count = self._read_int(
            metadata.get("updated_count"),
            default=sum(stat["updated_count"] for stat in source_stats),
        )
        duplicate_mention_count = self._read_int(
            metadata.get("duplicate_mention_count"),
            default=sum(stat["duplicate_mention_count"] for stat in source_stats),
        )
        extractor_fallback_count = self._read_int(
            metadata.get("extractor_fallback_count"),
            default=sum(stat["extractor_fallback_count"] for stat in source_stats),
        )
        ai_prompt_tokens = self._read_int(
            metadata.get("ai_prompt_tokens"),
            default=sum(stat["ai_prompt_tokens"] for stat in source_stats),
        )
        ai_completion_tokens = self._read_int(
            metadata.get("ai_completion_tokens"),
            default=sum(stat["ai_completion_tokens"] for stat in source_stats),
        )
        ai_total_tokens = self._read_int(
            metadata.get("ai_total_tokens"),
            default=sum(stat["ai_total_tokens"] for stat in source_stats),
        )
        if ai_total_tokens == 0:
            ai_total_tokens = ai_prompt_tokens + ai_completion_tokens
        ai_cost_usd = (
            self._read_float(
                metadata.get("ai_cost_usd"),
                default=self._estimate_ai_cost_usd(
                    prompt_tokens=ai_prompt_tokens,
                    completion_tokens=ai_completion_tokens,
                    total_tokens=ai_total_tokens,
                ),
                places=6,
            )
            or 0.0
        )
        tts_cost_usd = (
            self._read_float(
                metadata.get("tts_cost_usd"),
                default=0.0,
                places=6,
            )
            or 0.0
        )
        total_cost_usd = (
            self._read_float(
                metadata.get("total_cost_usd"),
                default=round(ai_cost_usd + tts_cost_usd, 6),
                places=6,
            )
            or 0.0
        )
        affected_edition_days = self._normalize_history_date_list(
            metadata.get("affected_edition_days")
        )
        logs = self._normalize_history_logs(metadata.get("logs"))
        errors = self._normalize_history_errors(metadata.get("errors"))
        if not errors and run.error:
            errors = [error for error in str(run.error).splitlines() if error.strip()]
        trigger = str(
            metadata.get("trigger")
            or ("ingest" if run.run_type == IngestionRunType.INGEST else run.run_type.value)
        )
        operation_kind = str(
            metadata.get("operation_kind")
            or self._default_operation_kind(run_type=run.run_type, trigger=trigger)
        )
        title = str(
            metadata.get("title")
            or self._default_operation_title(operation_kind=operation_kind, run_type=run.run_type)
        )
        summary = str(
            metadata.get("summary")
            or self._default_operation_summary(
                operation_kind=operation_kind,
                total_titles=total_titles,
                source_count=source_count,
                failed_source_count=failed_source_count,
            )
        )
        basic_info = self._normalize_history_basic_info(metadata.get("basic_info"))
        if not basic_info and operation_kind in {"ingest_cycle", "ingest_retry"}:
            basic_info = self._build_ingest_basic_info(
                source_count=source_count,
                failed_source_count=failed_source_count,
                total_titles=total_titles,
                created_count=created_count,
                updated_count=updated_count,
                extractor_fallback_count=extractor_fallback_count,
            )
        return {
            "id": run.id,
            "run_type": run.run_type,
            "status": run.status,
            "operation_kind": operation_kind,
            "trigger": trigger,
            "title": title,
            "summary": summary,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "affected_edition_days": affected_edition_days,
            "total_titles": total_titles,
            "source_count": source_count,
            "failed_source_count": failed_source_count,
            "created_count": created_count,
            "updated_count": updated_count,
            "duplicate_mention_count": duplicate_mention_count,
            "extractor_fallback_count": extractor_fallback_count,
            "ai_prompt_tokens": ai_prompt_tokens,
            "ai_completion_tokens": ai_completion_tokens,
            "ai_total_tokens": ai_total_tokens,
            "ai_cost_usd": ai_cost_usd,
            "tts_cost_usd": tts_cost_usd,
            "total_cost_usd": total_cost_usd,
            "average_extraction_confidence": self._read_float(
                metadata.get("average_extraction_confidence")
            ),
            "basic_info": basic_info,
            "logs": logs,
            "source_stats": source_stats,
            "errors": errors,
        }

    def _normalize_history_source_stats(self, value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        source_stats: list[dict[str, Any]] = []
        for raw in value:
            if not isinstance(raw, dict):
                continue
            items = raw.get("items")
            normalized_items = []
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    title = str(item.get("title") or "").strip()
                    if not title:
                        continue
                    normalized_items.append(
                        {
                            "title": title,
                            "outcome": str(item.get("outcome") or "updated"),
                            "content_type": str(
                                item.get("content_type") or ContentType.ARTICLE.value
                            ),
                            "extraction_confidence": self._read_float(
                                item.get("extraction_confidence"), default=0.0
                            )
                            or 0.0,
                        }
                    )
            source_stats.append(
                {
                    "source_id": str(raw.get("source_id")) if raw.get("source_id") else None,
                    "source_name": str(raw.get("source_name") or "Unknown source"),
                    "status": str(raw.get("status") or RunStatus.SUCCEEDED.value),
                    "ingested_count": self._read_int(
                        raw.get("ingested_count"), default=len(normalized_items)
                    ),
                    "created_count": self._read_int(raw.get("created_count")),
                    "updated_count": self._read_int(raw.get("updated_count")),
                    "duplicate_mention_count": self._read_int(raw.get("duplicate_mention_count")),
                    "extractor_fallback_count": self._read_int(raw.get("extractor_fallback_count")),
                    "ai_prompt_tokens": self._read_int(raw.get("ai_prompt_tokens")),
                    "ai_completion_tokens": self._read_int(raw.get("ai_completion_tokens")),
                    "ai_total_tokens": self._read_int(raw.get("ai_total_tokens")),
                    "ai_cost_usd": self._read_float(
                        raw.get("ai_cost_usd"),
                        default=self._estimate_ai_cost_usd(
                            prompt_tokens=self._read_int(raw.get("ai_prompt_tokens")),
                            completion_tokens=self._read_int(raw.get("ai_completion_tokens")),
                            total_tokens=self._read_int(raw.get("ai_total_tokens")),
                        ),
                        places=6,
                    )
                    or 0.0,
                    "average_extraction_confidence": self._read_float(
                        raw.get("average_extraction_confidence")
                    ),
                    "items": normalized_items,
                    "error": str(raw.get("error")).strip() if raw.get("error") else None,
                }
            )
        return source_stats

    def _normalize_history_date_list(self, value: Any) -> list[date]:
        if not isinstance(value, list):
            return []
        normalized_dates: list[date] = []
        for raw in value:
            try:
                normalized_dates.append(
                    raw if isinstance(raw, date) else date.fromisoformat(str(raw))
                )
            except (TypeError, ValueError):
                continue
        return sorted(dict.fromkeys(normalized_dates))

    def _normalize_history_logs(self, value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        normalized_logs: list[dict[str, Any]] = []
        for raw in value:
            if not isinstance(raw, dict):
                continue
            message = str(raw.get("message") or "").strip()
            if not message:
                continue
            logged_at = parse_datetime(raw.get("logged_at")) or utcnow()
            level = str(raw.get("level") or "info").strip().lower() or "info"
            normalized_logs.append(
                {
                    "logged_at": logged_at,
                    "level": level,
                    "message": message,
                }
            )
        return normalized_logs

    def _normalize_history_basic_info(self, value: Any) -> list[dict[str, str]]:
        if not isinstance(value, list):
            return []
        normalized: list[dict[str, str]] = []
        for raw in value:
            if not isinstance(raw, dict):
                continue
            label = str(raw.get("label") or "").strip()
            value_text = str(raw.get("value") or "").strip()
            if not label or not value_text:
                continue
            normalized.append({"label": label, "value": value_text})
        return normalized

    def _normalize_history_errors(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [str(error).strip() for error in value if str(error).strip()]

    def _read_int(self, value: Any, *, default: int = 0) -> int:
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _read_float(
        self, value: Any, *, default: float | None = None, places: int = 3
    ) -> float | None:
        if value is None:
            return default
        try:
            return round(float(value), places)
        except (TypeError, ValueError):
            return default

    def _analysis_text(self, item: Item) -> str:
        if item.content_type == ContentType.PAPER:
            for candidate in [
                item.metadata_json.get("semantic_scholar_abstract"),
                item.metadata_json.get("crossref_abstract"),
                item.metadata_json.get("abstract_text"),
            ]:
                normalized = self._normalize_analysis_text(candidate)
                if normalized:
                    return normalized
        return (
            self._normalize_analysis_text(item.content.cleaned_text if item.content else None)
            or item.title
        )

    def _normalize_analysis_text(self, value: str | None) -> str:
        if not value:
            return ""
        text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
        return " ".join(text.split())
