from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from app.core.config import get_settings
from app.db.models import ContentType, DataMode, IngestionRunType, RunStatus
from app.schemas.briefs import (
    AudioBriefRead,
    BriefAvailabilityDayRead,
    BriefAvailabilityRead,
    BriefAvailabilityWeekRead,
    DigestEntryRead,
    DigestRead,
    PaperTableEntryRead,
)
from app.schemas.common import AlphaXivPaperRead
from app.schemas.items import ItemDetailRead, ItemInsightRead, ItemListEntry, ItemScoreBreakdownRead, ItemScoreRead
from app.schemas.ops import (
    IngestionRunHistoryRead,
    OperationBasicInfoRead,
    OperationLogRead,
    OperationStepRead,
)
from app.schemas.published import (
    PublishedItemDetailRead,
    PublishedItemInsightRead,
    PublishedItemListEntryRead,
    PublishedItemScoreRead,
)
from app.services.brief_dates import coverage_day_for_edition, iso_week_end, iso_week_start
from app.services.profile import load_profile_snapshot
from app.services.text import fallback_short_summary, normalize_whitespace
from app.vault.models import AIRunManifest, AITraceReference, VaultItemRecord

SLUG_RE = re.compile(r"[^a-z0-9]+")
URL_RE = re.compile(r"https?://\S+")
NEWS_HOST_HINTS = (
    "techcrunch.com",
    "theverge.com",
    "wired.com",
    "venturebeat.com",
    "arstechnica.com",
    "ft.com",
    "nytimes.com",
    "wsj.com",
    "bloomberg.com",
    "reuters.com",
)
BLOG_HOST_HINTS = (
    "openai.com",
    "anthropic.com",
    "medium.com",
    "substack.com",
)
logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(UTC)


def current_profile_date() -> date:
    settings = get_settings()
    timezone_name = settings.timezone
    try:
        timezone_name = load_profile_snapshot().timezone or settings.timezone
    except Exception:
        timezone_name = settings.timezone
    return utcnow().astimezone(ZoneInfo(timezone_name)).date()


def slugify(value: str, *, fallback: str = "item") -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = SLUG_RE.sub("-", normalized.lower()).strip("-")
    return slug[:80] or fallback


def stable_doc_id(*parts: str) -> str:
    digest = hashlib.sha256("::".join(parts).encode("utf-8")).hexdigest()
    return digest[:20]


def short_hash(*parts: str, length: int = 8) -> str:
    digest = hashlib.sha256("::".join(parts).encode("utf-8")).hexdigest()
    return digest[:length]


def readable_doc_id(
    *,
    stable_key: str,
    title: str,
    source_slug: str,
    published_at: datetime | None,
    fallback_date: date | None = None,
) -> str:
    reference_date = (
        (published_at if published_at and published_at.tzinfo else published_at.replace(tzinfo=UTC))
        if published_at is not None
        else None
    )
    if reference_date is not None:
        date_part = reference_date.date().isoformat()
    elif fallback_date is not None:
        date_part = fallback_date.isoformat()
    else:
        date_part = "undated"
    title_slug = slugify(title, fallback="untitled")[:48]
    source_part = slugify(source_slug, fallback="source")[:24]
    return f"{date_part}-{source_part}-{title_slug}-{short_hash(stable_key)}"


def extract_links(text: str) -> list[str]:
    return list(dict.fromkeys(match.group(0).rstrip(").,") for match in URL_RE.finditer(text)))[:50]


def _canonical_hash_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    return normalize_whitespace(normalized).casefold()


def _canonical_identity_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    return normalize_whitespace(normalized)


def identity_hash(*parts: object) -> str:
    digest = hashlib.sha256()
    candidate_parts = parts or ("",)
    for part in candidate_parts:
        digest.update(b"\x1f")
        digest.update(_canonical_identity_text(str(part or "")).encode("utf-8"))
    return digest.hexdigest()


def document_identity_hash(
    *,
    source_id: str | None,
    external_key: str | None = None,
    canonical_url: str | None = None,
    fallback_key: str | None = None,
) -> str:
    return identity_hash(
        source_id or "unknown-source",
        external_key or canonical_url or fallback_key or "undocumented",
    )


def revision_hash(title: str, body: str) -> str:
    digest = hashlib.sha256()
    digest.update(_canonical_hash_text(title).encode("utf-8"))
    digest.update(_canonical_hash_text(body).encode("utf-8"))
    return digest.hexdigest()


def content_hash(title: str, body: str) -> str:
    return revision_hash(title, body)


def infer_content_type(kind: str, title: str, source_url: str | None, text: str) -> ContentType:
    normalized_kind = kind.strip().lower()
    lowered = f"{normalized_kind}\n{title}\n{source_url or ''}\n{text}".lower()
    if normalized_kind in {"article", "written-article"}:
        return ContentType.ARTICLE
    if normalized_kind in {"blog-post", "blog_post", "post"}:
        return ContentType.POST
    if normalized_kind in {"news", "news-item"}:
        return ContentType.NEWS
    if normalized_kind in {"newsletter", "email-newsletter", "email_newsletter"}:
        return ContentType.NEWSLETTER
    if normalized_kind in {"paper", "research-paper", "research_paper"}:
        return ContentType.PAPER
    if normalized_kind in {"thread"}:
        return ContentType.THREAD
    if normalized_kind in {"signal"}:
        return ContentType.SIGNAL
    if "paper" in lowered or "pdf" in lowered or "arxiv" in lowered or "doi:" in lowered:
        return ContentType.PAPER
    if "newsletter" in lowered or "gmail" in lowered or "email" in lowered:
        return ContentType.NEWSLETTER
    if "newsroom" in lowered or "/news/" in lowered or "breaking" in lowered:
        return ContentType.NEWS
    if "thread" in lowered:
        return ContentType.THREAD
    if "signal" in lowered:
        return ContentType.SIGNAL
    if "blog" in lowered or "post" in lowered:
        return ContentType.POST
    return ContentType.ARTICLE


def classify_written_kind(
    *,
    source_url: str | None,
    title: str,
    source_name: str | None,
    source_id: str | None,
    default_kind: str = "article",
) -> str:
    lowered = " ".join(filter(None, [title, source_name or "", source_id or "", source_url or ""])).lower()
    parsed = urlparse(source_url or "")
    host = (parsed.hostname or "").lower()
    path = parsed.path.lower()

    if "alphaxiv" in host or "arxiv" in host or "/abs/" in path:
        return "paper"
    if any(hint in host for hint in NEWS_HOST_HINTS) or "/newsroom" in path or path.startswith("/news/") or "news" in lowered:
        return "news"
    if any(hint in host for hint in BLOG_HOST_HINTS) or "/blog" in path or "/research/" in path or "blog" in lowered:
        return "blog-post"
    return default_kind


def canonical_url(source_url: str | None, raw_path: str) -> str:
    return source_url or raw_path


def estimate_audio_duration_seconds(script: str | None) -> int | None:
    if not script or not script.strip():
        return None
    word_count = len(script.split())
    return max(30, int(round(word_count / 2.6)))


def brief_priority_key(item: VaultItemRecord) -> tuple[float, datetime, str]:
    reference = item.published_at or item.fetched_at or item.ingested_at
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=UTC)
    kind_bonus = {
        ContentType.PAPER: 1.4,
        ContentType.POST: 1.2,
        ContentType.ARTICLE: 1.0,
        ContentType.NEWS: 0.95,
        ContentType.NEWSLETTER: 0.8,
        ContentType.SIGNAL: 0.7,
        ContentType.THREAD: 0.65,
    }.get(item.content_type, 1.0)
    total_score = item.score.total_score if item.score else 0.0
    return (total_score, item.trend_score, kind_bonus, reference, item.title.lower())


def to_item_list_entry(
    item: VaultItemRecord,
    *,
    read: bool = False,
    starred: bool = False,
    summary_override: str | None = None,
) -> ItemListEntry:
    short_summary = summary_override or fallback_short_summary(
        summary=item.short_summary,
        text=item.cleaned_text,
        title=item.title,
    )
    return ItemListEntry(
        id=item.id,
        kind=item.kind,
        source_id=item.source_id,
        title=item.title,
        source_name=item.source_name,
        organization_name=item.organization_name,
        authors=item.authors,
        published_at=item.published_at,
        canonical_url=item.canonical_url,
        content_type=item.content_type,
        triage_status="archived" if item.status == "archived" else "unread",
        read=read,
        starred=starred,
        extraction_confidence=item.extraction_confidence,
        short_summary=short_summary,
        bucket=item.score.bucket,
        total_score=item.score.total_score,
        score_breakdown=ItemScoreBreakdownRead(
            relevance_score=item.score.relevance_score,
            novelty_score=item.score.novelty_score,
            source_quality_score=item.score.source_quality_score,
            author_match_score=item.score.author_match_score,
            topic_match_score=item.score.topic_match_score,
            zotero_affinity_score=item.score.zotero_affinity_score,
        ),
        reason_trace=dict(item.score.reason_trace),
        also_mentioned_in_count=0,
    )


def to_item_detail(
    item: VaultItemRecord,
    *,
    read: bool = False,
    starred: bool = False,
    alphaxiv: AlphaXivPaperRead | None = None,
) -> ItemDetailRead:
    short_summary = (
        alphaxiv.short_summary
        if alphaxiv and alphaxiv.short_summary
        else fallback_short_summary(
            summary=item.short_summary,
            text=item.cleaned_text,
            title=item.title,
        )
    )
    return ItemDetailRead(
        id=item.id,
        title=item.title,
        source_name=item.source_name,
        organization_name=item.organization_name,
        authors=item.authors,
        published_at=item.published_at,
        canonical_url=item.canonical_url,
        content_type=item.content_type,
        triage_status="archived" if item.status == "archived" else "unread",
        read=read,
        starred=starred,
        ingest_status=RunStatus.SUCCEEDED,
        extraction_confidence=item.extraction_confidence,
        cleaned_text=item.cleaned_text,
        outbound_links=item.outbound_links,
        raw_payload_retention_until=None,
        score=ItemScoreRead.model_validate(item.score.model_dump(mode="json")),
        insight=ItemInsightRead(short_summary=short_summary),
        also_mentioned_in=[],
        zotero_matches=[],
        kind=item.kind,
        source_id=item.source_id,
        doc_role=item.doc_role,
        parent_id=item.parent_id,
        asset_paths=item.asset_paths,
        raw_doc_path=item.raw_doc_path,
        lightweight_enrichment_status=item.lightweight_enrichment_status,
        lightweight_enriched_at=item.lightweight_enriched_at,
        alphaxiv=alphaxiv,
    )


def to_published_item_list_entry(
    item: VaultItemRecord,
    *,
    summary_override: str | None = None,
) -> PublishedItemListEntryRead:
    short_summary = summary_override or fallback_short_summary(
        summary=item.short_summary,
        text=item.cleaned_text,
        title=item.title,
    )
    return PublishedItemListEntryRead(
        id=item.id,
        kind=item.kind,
        source_id=item.source_id,
        title=item.title,
        source_name=item.source_name,
        organization_name=item.organization_name,
        authors=item.authors,
        published_at=item.published_at,
        canonical_url=item.canonical_url,
        content_type=item.content_type,
        extraction_confidence=item.extraction_confidence,
        short_summary=short_summary,
        bucket=item.score.bucket,
        total_score=item.score.total_score,
        reason_trace=dict(item.score.reason_trace),
        also_mentioned_in_count=0,
    )


def to_published_item_detail(
    item: VaultItemRecord,
    *,
    alphaxiv: AlphaXivPaperRead | None = None,
) -> PublishedItemDetailRead:
    short_summary = (
        alphaxiv.short_summary
        if alphaxiv and alphaxiv.short_summary
        else fallback_short_summary(
            summary=item.short_summary,
            text=item.cleaned_text,
            title=item.title,
        )
    )
    return PublishedItemDetailRead(
        id=item.id,
        title=item.title,
        source_name=item.source_name,
        organization_name=item.organization_name,
        authors=item.authors,
        published_at=item.published_at,
        canonical_url=item.canonical_url,
        content_type=item.content_type,
        extraction_confidence=item.extraction_confidence,
        cleaned_text=item.cleaned_text,
        outbound_links=item.outbound_links,
        score=PublishedItemScoreRead.model_validate(item.score.model_dump(mode="json")),
        insight=PublishedItemInsightRead(short_summary=short_summary),
        also_mentioned_in=[],
        kind=item.kind,
        source_id=item.source_id,
        doc_role=item.doc_role,
        parent_id=item.parent_id,
        asset_paths=item.asset_paths,
        raw_doc_path=item.raw_doc_path,
        lightweight_enrichment_status=item.lightweight_enrichment_status,
        lightweight_enriched_at=item.lightweight_enriched_at,
        alphaxiv=alphaxiv,
    )


def build_digest(
    *,
    brief_date: date,
    title: str,
    editorial_note: str | None,
    follow_ups: list[str],
    generated_at: datetime,
    audio_brief: AudioBriefRead | None,
    editorial_shortlist: list[VaultItemRecord],
    headlines: list[VaultItemRecord],
    interesting_side_signals: list[VaultItemRecord],
    remaining_reads: list[VaultItemRecord],
    papers_table: list[VaultItemRecord],
    read_ids: set[str] | None = None,
    starred_ids: set[str] | None = None,
) -> DigestRead:
    read_ids = read_ids or set()
    starred_ids = starred_ids or set()
    coverage_day = coverage_day_for_edition(brief_date)

    def _entry_list(items: list[VaultItemRecord]) -> list[DigestEntryRead]:
        return [
            DigestEntryRead(
                item=to_item_list_entry(item, read=item.id in read_ids, starred=item.id in starred_ids),
                note=None,
                rank=index + 1,
            )
            for index, item in enumerate(items)
        ]

    return DigestRead(
        id=f"day:{brief_date.isoformat()}",
        period_type="day",
        brief_date=brief_date,
        week_start=None,
        week_end=None,
        coverage_start=coverage_day,
        coverage_end=coverage_day,
        data_mode=DataMode.LIVE,
        title=title,
        editorial_note=editorial_note,
        suggested_follow_ups=follow_ups,
        audio_brief=audio_brief,
        generated_at=generated_at,
        editorial_shortlist=_entry_list(editorial_shortlist),
        headlines=_entry_list(headlines),
        interesting_side_signals=_entry_list(interesting_side_signals),
        remaining_reads=_entry_list(remaining_reads),
        papers_table=[
            PaperTableEntryRead(
                item=to_item_list_entry(item, read=item.id in read_ids, starred=item.id in starred_ids),
                rank=index + 1,
                zotero_tags=[],
                credibility_score=None,
            )
            for index, item in enumerate(papers_table)
        ],
    )


def hydrate_digest(
    digest: DigestRead,
    *,
    item_lookup: dict[str, VaultItemRecord],
    read_ids: set[str] | None = None,
    starred_ids: set[str] | None = None,
) -> DigestRead:
    read_ids = read_ids or set()
    starred_ids = starred_ids or set()

    def _entry_list(entries: list[DigestEntryRead]) -> list[DigestEntryRead]:
        hydrated: list[DigestEntryRead] = []
        for entry in entries:
            item = item_lookup.get(entry.item.id)
            if item is None or item.index_visibility == "hidden" or item.status == "archived":
                continue
            hydrated.append(
                entry.model_copy(
                    update={
                        "item": to_item_list_entry(item, read=item.id in read_ids, starred=item.id in starred_ids),
                    }
                )
            )
        return hydrated

    def _paper_entries(entries: list[PaperTableEntryRead]) -> list[PaperTableEntryRead]:
        hydrated: list[PaperTableEntryRead] = []
        for entry in entries:
            item = item_lookup.get(entry.item.id)
            if item is None or item.index_visibility == "hidden" or item.status == "archived":
                continue
            hydrated.append(
                entry.model_copy(
                    update={
                        "item": to_item_list_entry(item, read=item.id in read_ids, starred=item.id in starred_ids),
                    }
                )
            )
        return hydrated

    return digest.model_copy(
        update={
            "editorial_shortlist": _entry_list(digest.editorial_shortlist),
            "headlines": _entry_list(digest.headlines),
            "interesting_side_signals": _entry_list(digest.interesting_side_signals),
            "remaining_reads": _entry_list(digest.remaining_reads),
            "papers_table": _paper_entries(digest.papers_table),
        }
    )


def list_brief_availability(
    available_dates: list[date],
    *,
    default_day: date | None = None,
    exclude_week_start_on_or_after: date | None = None,
) -> BriefAvailabilityRead:
    sorted_days = sorted(set(available_dates), reverse=True)
    weeks: list[BriefAvailabilityWeekRead] = []
    for week_dates in _group_dates_by_week(sorted(set(available_dates))):
        week_start = iso_week_start(week_dates[0])
        if exclude_week_start_on_or_after is not None and week_start >= exclude_week_start_on_or_after:
            continue
        weeks.append(
            BriefAvailabilityWeekRead(
                week_start=week_start,
                week_end=iso_week_end(week_start),
                coverage_start=week_start,
                coverage_end=iso_week_end(week_start),
            )
        )
    weeks.sort(key=lambda entry: entry.week_start, reverse=True)
    return BriefAvailabilityRead(
        default_day=default_day or (sorted_days[0] if sorted_days else None),
        days=[
            BriefAvailabilityDayRead(
                brief_date=brief_date,
                coverage_start=coverage_day_for_edition(brief_date),
                coverage_end=coverage_day_for_edition(brief_date),
            )
            for brief_date in sorted_days
        ],
        weeks=weeks,
    )


def _group_dates_by_week(values: list[date]) -> list[list[date]]:
    grouped: dict[tuple[int, int], list[date]] = {}
    for value in values:
        iso_year, iso_week, _ = value.isocalendar()
        grouped.setdefault((iso_year, iso_week), []).append(value)
    return [sorted(grouped[key]) for key in sorted(grouped)]


@dataclass
class PendingRun:
    id: str
    run_type: IngestionRunType
    operation_kind: str
    trigger: str | None
    title: str
    summary: str
    started_at: datetime
    affected_edition_days: list[date] = field(default_factory=list)
    total_titles: int = 0
    source_count: int = 0
    failed_source_count: int = 0
    created_count: int = 0
    updated_count: int = 0
    ai_prompt_tokens: int = 0
    ai_completion_tokens: int = 0
    ai_total_tokens: int = 0
    ai_cost_usd: float = 0.0
    tts_cost_usd: float = 0.0
    basic_info: list[OperationBasicInfoRead] = field(default_factory=list)
    logs: list[OperationLogRead] = field(default_factory=list)
    steps: list[OperationStepRead] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    ai_traces: list[AITraceReference] = field(default_factory=list)
    codex_command: list[str] | None = None
    prompt_path: str | None = None
    manifest_path: str | None = None
    output_paths: list[str] = field(default_factory=list)
    changed_file_count: int = 0
    duration_seconds: float | None = None
    exit_code: int | None = None
    stderr_excerpt: str | None = None
    final_summary: dict[str, Any] | None = None


class RunRecorder:
    def __init__(self, store) -> None:
        self.store = store

    def _record(
        self,
        run: PendingRun,
        *,
        status: RunStatus,
        finished_at: datetime | None = None,
    ) -> IngestionRunHistoryRead:
        return IngestionRunHistoryRead(
            id=run.id,
            run_type=run.run_type,
            status=status,
            operation_kind=run.operation_kind,
            trigger=run.trigger,
            title=run.title,
            summary=run.summary,
            started_at=run.started_at,
            finished_at=finished_at,
            affected_edition_days=run.affected_edition_days,
            total_titles=run.total_titles,
            source_count=run.source_count,
            failed_source_count=run.failed_source_count,
            created_count=run.created_count,
            updated_count=run.updated_count,
            duplicate_mention_count=0,
            extractor_fallback_count=0,
            ai_prompt_tokens=run.ai_prompt_tokens,
            ai_completion_tokens=run.ai_completion_tokens,
            ai_total_tokens=run.ai_total_tokens,
            ai_cost_usd=run.ai_cost_usd,
            tts_cost_usd=run.tts_cost_usd,
            total_cost_usd=round(run.ai_cost_usd + run.tts_cost_usd, 6),
            average_extraction_confidence=None,
            basic_info=run.basic_info,
            logs=run.logs,
            steps=run.steps,
            source_stats=[],
            errors=run.errors,
            codex_command=run.codex_command,
            prompt_path=run.prompt_path,
            manifest_path=run.manifest_path,
            output_paths=run.output_paths,
            changed_file_count=run.changed_file_count,
            duration_seconds=run.duration_seconds,
            exit_code=run.exit_code,
            stderr_excerpt=run.stderr_excerpt,
            final_summary=run.final_summary,
        )

    def _persist(
        self,
        run: PendingRun,
        *,
        status: RunStatus,
        finished_at: datetime | None = None,
    ) -> IngestionRunHistoryRead:
        record = self._record(run, status=status, finished_at=finished_at)
        self.store.upsert_run_record(record.model_dump(mode="json"))
        return record

    def start(
        self,
        *,
        run_type: IngestionRunType,
        operation_kind: str,
        trigger: str | None,
        title: str,
        summary: str,
    ) -> PendingRun:
        run = PendingRun(
            id=stable_doc_id(operation_kind, title, utcnow().isoformat(), secrets_hint()),
            run_type=run_type,
            operation_kind=operation_kind,
            trigger=trigger,
            title=title,
            summary=summary,
            started_at=utcnow(),
        )
        self._persist(run, status=RunStatus.RUNNING)
        return run

    def log(self, run: PendingRun, message: str, *, level: str = "info") -> None:
        run.logs.append(
            OperationLogRead(
                logged_at=utcnow(),
                level=level,
                message=message,
            )
        )
        self._persist(run, status=RunStatus.RUNNING)

    def start_step(
        self,
        run: PendingRun,
        *,
        step_kind: str,
        source_id: str | None = None,
        doc_id: str | None = None,
    ) -> OperationStepRead:
        step = OperationStepRead(
            step_kind=step_kind,
            status=RunStatus.RUNNING,
            started_at=utcnow(),
            source_id=source_id,
            doc_id=doc_id,
        )
        run.steps.append(step)
        self._persist(run, status=RunStatus.RUNNING)
        return step

    def log_step(self, run: PendingRun, step: OperationStepRead, message: str, *, level: str = "info") -> None:
        step.logs.append(
            OperationLogRead(
                logged_at=utcnow(),
                level=level,
                message=message,
            )
        )
        self._persist(run, status=RunStatus.RUNNING)

    def finish_step(
        self,
        run: PendingRun,
        step: OperationStepRead,
        *,
        status: RunStatus,
        created_count: int | None = None,
        updated_count: int | None = None,
        skipped_count: int | None = None,
        counts_by_kind: dict[str, int] | None = None,
    ) -> OperationStepRead:
        step.status = status
        step.finished_at = utcnow()
        if created_count is not None:
            step.created_count = created_count
        if updated_count is not None:
            step.updated_count = updated_count
        if skipped_count is not None:
            step.skipped_count = skipped_count
        if counts_by_kind is not None:
            step.counts_by_kind = counts_by_kind
        self._persist(run, status=RunStatus.RUNNING)
        return step

    def record_ai_trace(self, run: PendingRun, trace: AITraceReference | dict[str, Any] | None) -> None:
        if trace is None:
            return
        reference = trace if isinstance(trace, AITraceReference) else AITraceReference.model_validate(trace)
        if any(existing.trace_id == reference.trace_id for existing in run.ai_traces):
            return
        run.ai_traces.append(reference)
        run.ai_prompt_tokens += reference.prompt_tokens
        run.ai_completion_tokens += reference.completion_tokens
        run.ai_total_tokens += reference.total_tokens
        run.ai_cost_usd = round(run.ai_cost_usd + reference.cost_usd, 6)
        if run.prompt_path is None and reference.prompt_path:
            run.prompt_path = reference.prompt_path
        self._upsert_basic_info(run, "AI traces", str(len(run.ai_traces)))
        providers = ", ".join(sorted({entry.provider for entry in run.ai_traces if entry.provider}))
        if providers:
            self._upsert_basic_info(run, "AI providers", providers)
        models = ", ".join(sorted({entry.model for entry in run.ai_traces if entry.model}))
        if models:
            self._upsert_basic_info(run, "AI models", models)
        self._upsert_basic_info(
            run,
            "AI tokens",
            f"{run.ai_total_tokens} total ({run.ai_prompt_tokens} prompt / {run.ai_completion_tokens} completion)",
        )
        self._upsert_basic_info(run, "AI cost (USD)", f"{run.ai_cost_usd:.6f}")
        self._refresh_ai_manifest(run)
        self._persist(run, status=RunStatus.RUNNING)

    def record_tts_cost(self, run: PendingRun, *, cost_usd: float) -> None:
        normalized_cost = round(max(0.0, cost_usd), 6)
        if normalized_cost == run.tts_cost_usd:
            return
        run.tts_cost_usd = normalized_cost
        self._upsert_basic_info(run, "TTS cost (USD)", f"{run.tts_cost_usd:.6f}")
        self._persist(run, status=RunStatus.RUNNING)

    def finish(
        self,
        run: PendingRun,
        *,
        status: RunStatus,
        summary: str | None = None,
    ) -> IngestionRunHistoryRead:
        if summary:
            run.summary = summary
        finished_at = utcnow()
        if run.duration_seconds is None:
            run.duration_seconds = round((finished_at - run.started_at).total_seconds(), 2)
        return self._persist(run, status=status, finished_at=finished_at)

    def _refresh_ai_manifest(self, run: PendingRun) -> None:
        if not run.ai_traces:
            return
        manifest = AIRunManifest(
            run_id=run.id,
            generated_at=utcnow(),
            trace_count=len(run.ai_traces),
            providers=sorted({entry.provider for entry in run.ai_traces if entry.provider}),
            models=sorted({entry.model for entry in run.ai_traces if entry.model}),
            operations=sorted({entry.operation for entry in run.ai_traces if entry.operation}),
            ai_prompt_tokens=run.ai_prompt_tokens,
            ai_completion_tokens=run.ai_completion_tokens,
            ai_total_tokens=run.ai_total_tokens,
            ai_cost_usd=run.ai_cost_usd,
            traces=run.ai_traces,
        )
        try:
            run.manifest_path = str(self.store.write_ai_run_manifest(manifest))
        except Exception as exc:
            logger.warning(
                "run.ai_manifest.persist_failed",
                extra={
                    "run_id": run.id,
                    "operation_kind": run.operation_kind,
                    "reason": str(exc),
                },
            )

    @staticmethod
    def _upsert_basic_info(run: PendingRun, label: str, value: str) -> None:
        for entry in run.basic_info:
            if entry.label == label:
                entry.value = value
                return
        run.basic_info.append(OperationBasicInfoRead(label=label, value=value))


def secrets_hint() -> str:
    return hashlib.sha1(str(utcnow().timestamp()).encode("utf-8")).hexdigest()[:8]
