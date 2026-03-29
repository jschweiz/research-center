from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.db.models import (
    ContentType,
    DataMode,
    Digest,
    DigestEntry,
    DigestSection,
    IngestionRunType,
    Item,
    ItemCluster,
    RunStatus,
    ScoreBucket,
    TriageStatus,
)
from app.integrations.voice import VoiceClient
from app.schemas.briefs import (
    BriefAvailabilityDayRead,
    BriefAvailabilityRead,
    BriefAvailabilityWeekRead,
    DigestEntryRead,
    DigestRead,
)
from app.services.brief_dates import (
    coverage_day_for_edition,
    edition_day_for_datetimes,
    iso_week_end,
    iso_week_start,
)
from app.services.data_mode import filter_items_for_data_mode
from app.services.ingestion import IngestionService
from app.services.presenters import (
    build_audio_brief_read,
    build_digest_read,
    build_item_list_entry,
    build_paper_table_entry,
    build_period_digest_read,
    build_related_mentions,
    compute_paper_credibility_score,
    estimate_audio_duration_seconds,
)
from app.services.profile import ProfileService
from app.services.scheduling import ScheduleService
from app.services.text import compact_signal_note, normalize_item_title, normalize_whitespace

EDITORIAL_EMPH_RE = re.compile(r"\\emph\{([^}]*)\}")
EDITORIAL_THEME_SPLIT_RE = re.compile(r"\s*(?:--|—|–|:|;|\||\(|\[)\s*")
EDITORIAL_PREFIX_RE = re.compile(
    r"^(?:we|this paper|the paper)\s+"
    r"(?:present|presents|study|studies|show|shows|introduce|introduces|evaluate|evaluates|"
    r"explore|explores|propose|proposes|describe|describes|analyze|analyzes|investigate|"
    r"investigates|report|reports)\s+"
    r"(?:(?:an?|the)\s+)?(?:(?:empirical|systematic|practical|new)\s+)?"
    r"(?:(?:study|analysis|framework|approach|evaluation)\s+(?:of|for)\s+)?",
    re.IGNORECASE,
)
EDITORIAL_LEADING_PHRASE_RE = re.compile(r"^(?:how far can|towards?|understanding|rethinking|revisiting)\s+", re.IGNORECASE)
EDITORIAL_CLAUSE_TRIM_RE = re.compile(
    r"\b(?:that|which|who|while|where|because|although|under|using|with|without|via)\b.*$",
    re.IGNORECASE,
)
EDITORIAL_GENERIC_RE = re.compile(r"^(?:issue|episode|update|newsletter|brief)(?:\s+\d+)?$", re.IGNORECASE)
HEADLINE_KEYWORDS = (
    "announce",
    "announcement",
    "launch",
    "release",
    "rollout",
    "roadmap",
    "partnership",
    "funding",
    "raises",
    "acquisition",
    "statement",
    "pricing",
    "availability",
    "preview",
    "update",
    "blog",
    "api",
    "open source",
)
SIDE_SIGNAL_KEYWORDS = (
    "trend",
    "shift",
    "ecosystem",
    "adoption",
    "tooling",
    "integration",
    "workflow",
    "benchmark",
    "community",
    "developer",
    "notable",
    "signal",
)
SECTION_LIMITS = {
    DigestSection.HEADLINES: 20,
    DigestSection.EDITORIAL_SHORTLIST: 3,
    DigestSection.INTERESTING_SIDE_SIGNALS: 6,
    DigestSection.REMAINING_READS: 10,
}
SECTION_PRIORITY = {
    DigestSection.EDITORIAL_SHORTLIST: 0,
    DigestSection.HEADLINES: 1,
    DigestSection.INTERESTING_SIDE_SIGNALS: 2,
    DigestSection.REMAINING_READS: 3,
}
AUDIO_SECTION_ORDER = [
    DigestSection.EDITORIAL_SHORTLIST,
    DigestSection.HEADLINES,
    DigestSection.INTERESTING_SIDE_SIGNALS,
    DigestSection.REMAINING_READS,
]
AUDIO_SECTION_LIMITS = {
    DigestSection.EDITORIAL_SHORTLIST: 3,
    DigestSection.HEADLINES: 2,
    DigestSection.INTERESTING_SIDE_SIGNALS: 1,
    DigestSection.REMAINING_READS: 1,
}
PAPER_TABLE_LIMIT = 5
PAPER_AUDIO_LIMIT = 2
EDITORIAL_NOTE_SECTION_LIMITS = {
    DigestSection.EDITORIAL_SHORTLIST: 3,
    DigestSection.HEADLINES: 4,
    DigestSection.INTERESTING_SIDE_SIGNALS: 2,
    DigestSection.REMAINING_READS: 1,
}


@dataclass
class WeeklyEntryAggregate:
    item: Item
    note: str | None
    section: DigestSection
    section_priority: int
    best_rank: int


@dataclass
class AudioDigestEntry:
    item: Item
    note: str | None
    section: str
    rank: int


class BriefService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.ingestion_service = IngestionService(db)
        self.voice_client = VoiceClient()
        self.profile_service = ProfileService(db)
        self.schedule_service = ScheduleService(db)

    def current_edition_date(self) -> date:
        return self.schedule_service.current_profile_date()

    def get_digest_by_date(self, brief_date: date, *, data_mode: DataMode | None = None) -> DigestRead | None:
        resolved_data_mode = data_mode or self.profile_service.get_profile().data_mode
        digest = self._get_digest_model_by_date(brief_date, data_mode=resolved_data_mode)
        if not digest:
            return None
        digest = self._ensure_digest_editorial_note(digest)
        representatives = self._representative_items_for_edition_day(brief_date, data_mode=resolved_data_mode)
        return build_digest_read(digest, papers_table=self._build_papers_table_entries(representatives))

    def get_or_generate_by_date(self, brief_date: date, *, data_mode: DataMode | None = None) -> DigestRead | None:
        resolved_data_mode = data_mode or self.profile_service.get_profile().data_mode
        digest = self.get_digest_by_date(brief_date, data_mode=resolved_data_mode)
        if digest:
            return digest
        self.generate_digest(
            brief_date,
            data_mode=resolved_data_mode,
            trigger="load_backfill",
            editorial_note_mode="generate",
        )
        return self.get_digest_by_date(brief_date, data_mode=resolved_data_mode)

    def get_or_generate_today(self, *, data_mode: DataMode | None = None) -> DigestRead | None:
        return self.get_or_generate_by_date(self.current_edition_date(), data_mode=data_mode)

    def list_availability(self, *, data_mode: DataMode | None = None) -> BriefAvailabilityRead:
        resolved_data_mode = data_mode or self.profile_service.get_profile().data_mode
        current_day = self.current_edition_date()
        available_days = self._available_edition_days(data_mode=resolved_data_mode)
        default_day = (
            current_day
            if current_day in available_days
            else max(available_days) if available_days else current_day
        )

        week_starts = sorted(
            {
                iso_week_start(brief_date)
                for brief_date in available_days
                if iso_week_end(iso_week_start(brief_date)) < current_day
            },
            reverse=True,
        )

        return BriefAvailabilityRead(
            default_day=default_day,
            days=[
                BriefAvailabilityDayRead(
                    brief_date=brief_date,
                    coverage_start=coverage_day_for_edition(brief_date),
                    coverage_end=coverage_day_for_edition(brief_date),
                )
                for brief_date in sorted(available_days, reverse=True)
            ],
            weeks=[
                BriefAvailabilityWeekRead(
                    week_start=week_start,
                    week_end=iso_week_end(week_start),
                    coverage_start=coverage_day_for_edition(week_start),
                    coverage_end=coverage_day_for_edition(iso_week_end(week_start)),
                )
                for week_start in week_starts
            ],
        )

    def get_weekly_digest(self, week_start: date, *, data_mode: DataMode | None = None) -> DigestRead | None:
        resolved_data_mode = data_mode or self.profile_service.get_profile().data_mode
        normalized_week_start = iso_week_start(week_start)
        week_end = iso_week_end(normalized_week_start)
        if week_end >= self.current_edition_date():
            return None

        available_days = [
            brief_date
            for brief_date in self._available_edition_days(data_mode=resolved_data_mode)
            if normalized_week_start <= brief_date <= week_end
        ]
        if not available_days:
            return None

        digest_models: list[Digest] = []
        for brief_date in sorted(available_days):
            digest = self._get_digest_model_by_date(brief_date, data_mode=resolved_data_mode)
            if not digest:
                self.generate_digest(
                    brief_date,
                    data_mode=resolved_data_mode,
                    trigger="weekly_backfill",
                    editorial_note_mode="preserve",
                )
                digest = self._get_digest_model_by_date(brief_date, data_mode=resolved_data_mode)
            if digest:
                digest_models.append(digest)
        if not digest_models:
            return None

        grouped = self._build_weekly_groups(digest_models)
        lead_items = [entry.item for entry in grouped["editorial_shortlist"]] or [
            entry.item
            for section in ("headlines", "interesting_side_signals", "remaining_reads")
            for entry in grouped[section]
        ][:3]
        paper_pool = self._representative_items_for_edition_days(available_days, data_mode=resolved_data_mode)
        generated_at = datetime.now(UTC)

        return build_period_digest_read(
            digest_id=f"week:{normalized_week_start.isoformat()}:{resolved_data_mode.value}",
            period_type="week",
            brief_date=None,
            week_start=normalized_week_start,
            week_end=week_end,
            coverage_start=coverage_day_for_edition(normalized_week_start),
            coverage_end=coverage_day_for_edition(week_end),
            data_mode=resolved_data_mode,
            title=f"Weekly Brief • {normalized_week_start.isoformat()}",
            editorial_note=self._build_editorial_note(lead_items),
            suggested_follow_ups=[],
            audio_brief=None,
            generated_at=generated_at,
            headlines=grouped["headlines"],
            editorial_shortlist=grouped["editorial_shortlist"],
            interesting_side_signals=grouped["interesting_side_signals"],
            remaining_reads=grouped["remaining_reads"],
            papers_table=self._build_papers_table_entries(paper_pool),
        )

    def generate_digest(
        self,
        brief_date: date,
        force: bool = False,
        *,
        data_mode: DataMode | None = None,
        trigger: str | None = None,
        editorial_note_mode: Literal["generate", "preserve"] = "generate",
    ) -> Digest:
        resolved_data_mode = data_mode or self.profile_service.get_profile().data_mode
        preserved_editorial_note: str | None = None
        existing = self.db.scalar(
            select(Digest).where(Digest.brief_date == brief_date, Digest.data_mode == resolved_data_mode)
        )
        if existing and force:
            if editorial_note_mode == "preserve" and self._editorial_note_present(existing.editorial_note):
                preserved_editorial_note = existing.editorial_note
            self.db.delete(existing)
            self.db.commit()
            existing = None
        if existing:
            if editorial_note_mode == "generate" and not self._editorial_note_present(existing.editorial_note):
                existing = self._ensure_digest_editorial_note(existing)
            return existing

        operation_trigger = trigger or ("regenerate" if force else "generate")
        operation_run = self.ingestion_service.start_operation_run(
            run_type=IngestionRunType.DIGEST,
            operation_kind="brief_generation",
            trigger=operation_trigger,
            metadata={
                "brief_date": brief_date.isoformat(),
                "affected_edition_days": [brief_date.isoformat()],
                "data_mode": resolved_data_mode.value,
            },
        )
        representatives = self._representative_items_for_edition_day(brief_date, data_mode=resolved_data_mode)
        headlines, editorial_shortlist, interesting_side_signals, remaining_reads = self._partition_items(representatives)

        ai_usage = self.ingestion_service._empty_ai_usage()
        insights_generated = 0
        for item in self._unique_items(headlines + editorial_shortlist + interesting_side_signals + remaining_reads):
            _, usage, generated = self.ingestion_service.ensure_insight_with_usage(item)
            ai_usage = self.ingestion_service._merge_ai_usage(ai_usage, usage)
            if generated:
                insights_generated += 1

        brief_title = f"Morning Brief • {brief_date.isoformat()}"
        editorial_note = preserved_editorial_note
        if editorial_note_mode == "generate":
            editorial_note, editorial_note_usage = self._compose_editorial_note(
                brief_date=brief_date,
                title=brief_title,
                editorial_shortlist=self._build_editorial_note_item_payloads(
                    editorial_shortlist,
                    section=DigestSection.EDITORIAL_SHORTLIST,
                ),
                headlines=self._build_editorial_note_item_payloads(
                    headlines,
                    section=DigestSection.HEADLINES,
                ),
                interesting_side_signals=self._build_editorial_note_item_payloads(
                    interesting_side_signals,
                    section=DigestSection.INTERESTING_SIDE_SIGNALS,
                ),
                remaining_reads=self._build_editorial_note_item_payloads(
                    remaining_reads,
                    section=DigestSection.REMAINING_READS,
                ),
                fallback_note=self._build_editorial_note(editorial_shortlist or representatives[:3]),
            )
            ai_usage = self.ingestion_service._merge_ai_usage(ai_usage, editorial_note_usage)

        digest = Digest(
            brief_date=brief_date,
            data_mode=resolved_data_mode,
            status=RunStatus.SUCCEEDED,
            title=brief_title,
            editorial_note=editorial_note,
            suggested_follow_ups=self._collect_follow_ups(editorial_shortlist + remaining_reads),
        )
        try:
            self.db.add(digest)
            self.db.flush()

            self._attach_entries(digest, DigestSection.HEADLINES, headlines)
            self._attach_entries(digest, DigestSection.EDITORIAL_SHORTLIST, editorial_shortlist)
            self._attach_entries(digest, DigestSection.INTERESTING_SIDE_SIGNALS, interesting_side_signals)
            self._attach_entries(digest, DigestSection.REMAINING_READS, remaining_reads)
            self.db.commit()
            self.db.refresh(digest)
            self.ingestion_service.finalize_operation_run(
                operation_run,
                status=RunStatus.SUCCEEDED,
                metadata=self._build_digest_operation_metadata(
                    digest=digest,
                    trigger=operation_trigger,
                    insights_generated=insights_generated,
                    ai_usage=ai_usage,
                ),
            )
            return digest
        except IntegrityError:
            self.db.rollback()
            existing = self.db.scalar(
                select(Digest).where(Digest.brief_date == brief_date, Digest.data_mode == resolved_data_mode)
            )
            if existing:
                self.ingestion_service.finalize_operation_run(
                    operation_run,
                    status=RunStatus.SUCCEEDED,
                    metadata=self._build_digest_operation_metadata(
                        digest=existing,
                        trigger=operation_trigger,
                        insights_generated=insights_generated,
                        ai_usage=ai_usage,
                    ),
                )
                return existing
            self.ingestion_service.finalize_operation_run(
                operation_run,
                status=RunStatus.FAILED,
                error="Digest generation conflicted with another write.",
            )
            raise
        except Exception as exc:
            self.db.rollback()
            llm_cost_usd = self._estimate_llm_cost_usd(ai_usage)
            self.ingestion_service.finalize_operation_run(
                operation_run,
                status=RunStatus.FAILED,
                metadata={
                    "title": "Brief generation",
                    "summary": "Brief generation failed.",
                    "brief_date": brief_date.isoformat(),
                    "affected_edition_days": [brief_date.isoformat()],
                    "data_mode": resolved_data_mode.value,
                    "ai_prompt_tokens": ai_usage["prompt_tokens"],
                    "ai_completion_tokens": ai_usage["completion_tokens"],
                    "ai_total_tokens": ai_usage["total_tokens"],
                    "ai_cost_usd": llm_cost_usd,
                    "tts_cost_usd": 0.0,
                    "total_cost_usd": llm_cost_usd,
                    "basic_info": [
                        {"label": "Data mode", "value": resolved_data_mode.value},
                        {"label": "Insights generated", "value": str(insights_generated)},
                    ],
                },
                error=str(exc),
            )
            raise

    def refresh_edition_days(self, edition_days: set[date] | list[date], *, data_mode: DataMode | None = None) -> None:
        resolved_data_mode = data_mode or self.profile_service.get_profile().data_mode
        for brief_date in sorted(set(edition_days)):
            self.generate_digest(
                brief_date,
                force=True,
                data_mode=resolved_data_mode,
                trigger="ingest_refresh",
                editorial_note_mode="preserve",
            )

    def refresh_current_edition_day(
        self,
        *,
        data_mode: DataMode | None = None,
        trigger: str = "ingest_refresh",
    ) -> date:
        resolved_data_mode = data_mode or self.profile_service.get_profile().data_mode
        brief_date = self.current_edition_date()
        self.generate_digest(
            brief_date,
            force=True,
            data_mode=resolved_data_mode,
            trigger=trigger,
            editorial_note_mode="preserve",
        )
        return brief_date

    def purge_cached_digests(self) -> None:
        self.db.execute(delete(DigestEntry))
        self.db.execute(delete(Digest))
        self.db.commit()

    def generate_audio_brief(
        self,
        brief_date: date,
        *,
        data_mode: DataMode | None = None,
        force: bool = True,
    ):
        resolved_data_mode = data_mode or self.profile_service.get_profile().data_mode
        digest = self._get_digest_model_by_date(brief_date, data_mode=resolved_data_mode)
        if not digest:
            self.generate_digest(
                brief_date,
                data_mode=resolved_data_mode,
                trigger="audio_prepare",
                editorial_note_mode="preserve",
            )
            digest = self._get_digest_model_by_date(brief_date, data_mode=resolved_data_mode)
        if not digest:
            return None
        if digest.audio_brief_status == RunStatus.SUCCEEDED.value and digest.audio_brief_script and not force:
            self._ensure_audio_artifact(digest)
            return build_audio_brief_read(digest)

        operation_run = self.ingestion_service.start_operation_run(
            run_type=IngestionRunType.DIGEST,
            operation_kind="audio_generation",
            trigger="audio",
            metadata={
                "brief_date": brief_date.isoformat(),
                "affected_edition_days": [brief_date.isoformat()],
                "data_mode": resolved_data_mode.value,
            },
        )
        digest.audio_brief_status = RunStatus.RUNNING.value
        digest.audio_brief_script = None
        digest.audio_brief_chapters = []
        digest.audio_brief_error = None
        digest.audio_brief_generated_at = None
        digest.audio_artifact_url = None
        digest.audio_artifact_provider = None
        digest.audio_artifact_voice = None
        digest.audio_duration_seconds = None
        digest.audio_metadata_json = {}
        self.voice_client.clear_cached_audio(digest.id)
        self.db.add(digest)

        script: str | None = None
        chapters: list[dict] = []
        generation_mode = "heuristic"
        ai_usage = self.ingestion_service._empty_ai_usage()
        try:
            audio_context, context_usage = self._build_audio_brief_context(digest)
            ai_usage = self.ingestion_service._merge_ai_usage(ai_usage, context_usage)
            payload = self.ingestion_service.llm.compose_audio_brief(audio_context)
            ai_usage = self.ingestion_service._merge_ai_usage(ai_usage, self.ingestion_service._normalize_ai_usage(payload.get("_usage")))
            script, chapters = self._compile_audio_brief(digest, payload)
            generation_mode = payload.get("generation_mode", "heuristic")
            audio_path = self.voice_client.ensure_cached_audio(digest.id, script)
            estimated_duration_seconds = estimate_audio_duration_seconds(script)
            digest.audio_brief_status = RunStatus.SUCCEEDED.value
            digest.audio_brief_script = script
            digest.audio_brief_chapters = chapters
            digest.audio_brief_error = None
            digest.audio_brief_generated_at = datetime.now(UTC)
            digest.audio_artifact_provider = self.voice_client.provider_name
            digest.audio_artifact_voice = self.voice_client.voice_name
            digest.audio_artifact_url = None
            digest.audio_duration_seconds = estimated_duration_seconds
            llm_cost_usd = self._estimate_llm_cost_usd(ai_usage)
            tts_cost_usd = self.voice_client.estimate_synthesis_cost_usd(script)
            digest.audio_metadata_json = {
                "chapter_count": len(chapters),
                "script_word_count": len(script.split()),
                "tts_character_count": self.voice_client.estimate_character_count(script),
                "estimated_duration_seconds": estimated_duration_seconds,
                "generation_mode": generation_mode,
                "voice_pricing_tier": self.voice_client.pricing_tier,
                "ai_prompt_tokens": ai_usage["prompt_tokens"],
                "ai_completion_tokens": ai_usage["completion_tokens"],
                "ai_total_tokens": ai_usage["total_tokens"],
                "estimated_ai_cost_usd": llm_cost_usd,
                "estimated_tts_cost_usd": tts_cost_usd,
                "estimated_total_cost_usd": round(llm_cost_usd + tts_cost_usd, 6),
                "audio_cache_key": self.voice_client.cache_key_for_digest(digest.id),
                "audio_file_format": self.voice_client.output_format,
                "audio_file_size_bytes": audio_path.stat().st_size,
            }
            self.db.add(digest)
            self.db.commit()
            self.ingestion_service.finalize_operation_run(
                operation_run,
                status=RunStatus.SUCCEEDED,
                metadata=self._build_audio_operation_metadata(
                    digest=digest,
                    generation_mode=generation_mode,
                    ai_usage=ai_usage,
                ),
            )
        except Exception as exc:
            self.db.rollback()
            estimated_duration_seconds = estimate_audio_duration_seconds(script)
            digest.audio_brief_status = RunStatus.FAILED.value
            digest.audio_brief_script = script
            digest.audio_brief_chapters = chapters
            digest.audio_brief_error = str(exc)
            digest.audio_brief_generated_at = datetime.now(UTC)
            digest.audio_artifact_provider = self.voice_client.provider_name if self.voice_client.configured else None
            digest.audio_artifact_voice = self.voice_client.voice_name if self.voice_client.configured else None
            digest.audio_duration_seconds = estimated_duration_seconds
            llm_cost_usd = self._estimate_llm_cost_usd(ai_usage)
            tts_cost_usd = self.voice_client.estimate_synthesis_cost_usd(script)
            digest.audio_metadata_json = {
                "chapter_count": len(chapters),
                "script_word_count": len(script.split()) if script else 0,
                "tts_character_count": self.voice_client.estimate_character_count(script),
                "estimated_duration_seconds": estimated_duration_seconds,
                "generation_mode": generation_mode,
                "voice_pricing_tier": self.voice_client.pricing_tier,
                "ai_prompt_tokens": ai_usage["prompt_tokens"],
                "ai_completion_tokens": ai_usage["completion_tokens"],
                "ai_total_tokens": ai_usage["total_tokens"],
                "estimated_ai_cost_usd": llm_cost_usd,
                "estimated_tts_cost_usd": tts_cost_usd,
                "estimated_total_cost_usd": round(llm_cost_usd + tts_cost_usd, 6),
            }
            self.db.add(digest)
            self.db.commit()
            self.ingestion_service.finalize_operation_run(
                operation_run,
                status=RunStatus.FAILED,
                metadata=self._build_audio_operation_metadata(
                    digest=digest,
                    generation_mode=generation_mode,
                    ai_usage=ai_usage,
                ),
                error=str(exc),
            )

        self.db.refresh(digest)
        return build_audio_brief_read(digest)

    def get_audio_artifact_path(
        self,
        brief_date: date,
        *,
        data_mode: DataMode | None = None,
    ) -> Path | None:
        resolved_data_mode = data_mode or self.profile_service.get_profile().data_mode
        digest = self._get_digest_model_by_date(brief_date, data_mode=resolved_data_mode)
        if not digest:
            return None
        self._ensure_audio_artifact(digest)
        if digest.audio_brief_status != RunStatus.SUCCEEDED.value:
            return None
        return self.voice_client.cache_path_for_digest(digest.id)

    def _build_digest_operation_metadata(
        self,
        *,
        digest: Digest,
        trigger: str,
        insights_generated: int,
        ai_usage: dict[str, int],
    ) -> dict[str, Any]:
        section_counts = {
            DigestSection.HEADLINES.value: sum(1 for entry in digest.entries if entry.section == DigestSection.HEADLINES),
            DigestSection.EDITORIAL_SHORTLIST.value: sum(
                1 for entry in digest.entries if entry.section == DigestSection.EDITORIAL_SHORTLIST
            ),
            DigestSection.INTERESTING_SIDE_SIGNALS.value: sum(
                1 for entry in digest.entries if entry.section == DigestSection.INTERESTING_SIDE_SIGNALS
            ),
            DigestSection.REMAINING_READS.value: sum(
                1 for entry in digest.entries if entry.section == DigestSection.REMAINING_READS
            ),
        }
        entry_count = sum(section_counts.values())
        non_empty_section_count = sum(1 for count in section_counts.values() if count)
        summary = (
            f"{entry_count} ranked entr{'ies' if entry_count != 1 else 'y'} "
            f"across {non_empty_section_count} section{'s' if non_empty_section_count != 1 else ''}"
        )
        if insights_generated:
            summary = f"{summary} · {insights_generated} new insight{'s' if insights_generated != 1 else ''}"
        llm_cost_usd = self._estimate_llm_cost_usd(ai_usage)
        return {
            "operation_kind": "brief_generation",
            "trigger": trigger,
            "title": "Brief generation",
            "summary": summary,
            "brief_date": digest.brief_date.isoformat(),
            "affected_edition_days": [digest.brief_date.isoformat()],
            "data_mode": digest.data_mode.value,
            "section_counts": section_counts,
            "ingested_count": entry_count,
            "ai_prompt_tokens": ai_usage["prompt_tokens"],
            "ai_completion_tokens": ai_usage["completion_tokens"],
            "ai_total_tokens": ai_usage["total_tokens"],
            "ai_cost_usd": llm_cost_usd,
            "tts_cost_usd": 0.0,
            "total_cost_usd": llm_cost_usd,
            "basic_info": [
                {"label": "Entries", "value": str(entry_count)},
                {"label": "Sections", "value": str(non_empty_section_count)},
                {"label": "Insights generated", "value": str(insights_generated)},
                {"label": "Data mode", "value": digest.data_mode.value},
            ],
        }

    def _build_audio_operation_metadata(
        self,
        *,
        digest: Digest,
        generation_mode: str,
        ai_usage: dict[str, int],
    ) -> dict[str, Any]:
        chapter_count = len(digest.audio_brief_chapters or [])
        script_word_count = len((digest.audio_brief_script or "").split())
        tts_character_count = self.voice_client.estimate_character_count(digest.audio_brief_script)
        estimated_duration_seconds = digest.audio_duration_seconds or estimate_audio_duration_seconds(digest.audio_brief_script)
        duration_label = self._format_duration_label(estimated_duration_seconds)
        summary = f"{chapter_count} chapter{'s' if chapter_count != 1 else ''} · {script_word_count} words"
        if duration_label:
            summary = f"{summary} · {duration_label}"
        llm_cost_usd = self._estimate_llm_cost_usd(ai_usage)
        tts_cost_usd = self.voice_client.estimate_synthesis_cost_usd(digest.audio_brief_script)
        return {
            "operation_kind": "audio_generation",
            "trigger": "audio",
            "title": "Audio brief generation",
            "summary": summary,
            "brief_date": digest.brief_date.isoformat(),
            "affected_edition_days": [digest.brief_date.isoformat()],
            "data_mode": digest.data_mode.value,
            "ingested_count": chapter_count,
            "ai_prompt_tokens": ai_usage["prompt_tokens"],
            "ai_completion_tokens": ai_usage["completion_tokens"],
            "ai_total_tokens": ai_usage["total_tokens"],
            "ai_cost_usd": llm_cost_usd,
            "tts_cost_usd": tts_cost_usd,
            "total_cost_usd": round(llm_cost_usd + tts_cost_usd, 6),
            "basic_info": [
                {"label": "Chapters", "value": str(chapter_count)},
                {"label": "Words", "value": str(script_word_count)},
                {"label": "Characters", "value": str(tts_character_count)},
                {"label": "Estimated duration", "value": duration_label or "n/a"},
                {"label": "Generation mode", "value": generation_mode},
                {"label": "Voice tier", "value": self.voice_client.pricing_tier},
            ],
        }

    def _format_duration_label(self, duration_seconds: int | None) -> str | None:
        if not duration_seconds or duration_seconds <= 0:
            return None
        minutes, seconds = divmod(duration_seconds, 60)
        if minutes == 0:
            return f"{seconds}s"
        return f"{minutes}m {seconds:02d}s"

    def _estimate_llm_cost_usd(self, ai_usage: dict[str, int]) -> float:
        return self.ingestion_service._estimate_ai_cost_usd(
            prompt_tokens=ai_usage["prompt_tokens"],
            completion_tokens=ai_usage["completion_tokens"],
            total_tokens=ai_usage["total_tokens"],
        )

    def _digest_query(self):
        return select(Digest).options(
            selectinload(Digest.entries)
            .selectinload(DigestEntry.item)
            .selectinload(Item.score),
            selectinload(Digest.entries)
            .selectinload(DigestEntry.item)
            .selectinload(Item.insight),
            selectinload(Digest.entries)
            .selectinload(DigestEntry.item)
            .selectinload(Item.content),
            selectinload(Digest.entries)
            .selectinload(DigestEntry.item)
            .selectinload(Item.cluster)
            .selectinload(ItemCluster.items),
            selectinload(Digest.entries)
            .selectinload(DigestEntry.item)
            .selectinload(Item.zotero_matches),
        )

    def _get_digest_model_by_date(self, brief_date: date, *, data_mode: DataMode):
        return self.db.scalar(
            self._digest_query().where(Digest.brief_date == brief_date, Digest.data_mode == data_mode)
        )

    def _all_active_items(self, *, data_mode: DataMode) -> list[Item]:
        return filter_items_for_data_mode(
            list(
                self.db.scalars(
                    select(Item)
                    .options(
                        selectinload(Item.score),
                        selectinload(Item.insight),
                        selectinload(Item.content),
                        selectinload(Item.cluster).selectinload(ItemCluster.items),
                        selectinload(Item.zotero_matches),
                    )
                    .where(Item.triage_status != TriageStatus.ARCHIVED)
                ).all()
            ),
            data_mode,
        )

    def _available_edition_days(self, *, data_mode: DataMode) -> set[date]:
        timezone_name = self.profile_service.get_profile().timezone
        return {
            edition_day
            for item in self._all_active_items(data_mode=data_mode)
            if (edition_day := self._edition_day_for_item(item, timezone_name))
        }

    def _representative_items_for_edition_day(self, brief_date: date, *, data_mode: DataMode) -> list[Item]:
        return self._representative_items_for_edition_days([brief_date], data_mode=data_mode)

    def _representative_items_for_edition_days(self, brief_dates: list[date], *, data_mode: DataMode) -> list[Item]:
        timezone_name = self.profile_service.get_profile().timezone
        target_dates = set(brief_dates)
        items = [
            item
            for item in self._all_active_items(data_mode=data_mode)
            if self._edition_day_for_item(item, timezone_name) in target_dates
        ]
        representatives = [
            item
            for item in items
            if not item.cluster or item.cluster.representative_item_id in {None, item.id}
        ]
        representatives.sort(
            key=lambda item: (
                item.score.total_score if item.score else 0.0,
                self._sort_timestamp(item),
            ),
            reverse=True,
        )
        return representatives

    def _partition_items(self, representatives: list[Item]) -> tuple[list[Item], list[Item], list[Item], list[Item]]:
        headlines = [
            item
            for item in representatives
            if self._is_headline_candidate(item)
        ][: SECTION_LIMITS[DigestSection.HEADLINES]]

        selected_ids = {item.id for item in headlines}
        remaining = [item for item in representatives if item.id not in selected_ids]

        shortlist_limit = SECTION_LIMITS[DigestSection.EDITORIAL_SHORTLIST]
        editorial_shortlist = [
            item
            for item in remaining
            if item.score and (item.score.bucket == ScoreBucket.MUST_READ or item.score.total_score >= 0.72)
        ][:shortlist_limit]
        if len(editorial_shortlist) < shortlist_limit:
            shortlisted_ids = {item.id for item in editorial_shortlist}
            for item in remaining:
                if item.id in shortlisted_ids:
                    continue
                editorial_shortlist.append(item)
                shortlisted_ids.add(item.id)
                if len(editorial_shortlist) >= shortlist_limit:
                    break

        selected_ids.update(item.id for item in editorial_shortlist)
        interesting_side_signals = [
            item
            for item in representatives
            if item.id not in selected_ids and self._is_side_signal_candidate(item)
        ][: SECTION_LIMITS[DigestSection.INTERESTING_SIDE_SIGNALS]]
        selected_ids.update(item.id for item in interesting_side_signals)

        remaining_reads = [
            item
            for item in representatives
            if item.id not in selected_ids
        ][: SECTION_LIMITS[DigestSection.REMAINING_READS]]

        return headlines, editorial_shortlist, interesting_side_signals, remaining_reads

    def _build_papers_table_entries(self, items: list[Item]):
        return [
            build_paper_table_entry(item, rank=index)
            for index, item in enumerate(self._paper_table_items(items), start=1)
        ]

    def _paper_table_items(self, items: list[Item]) -> list[Item]:
        papers = self._unique_items([item for item in items if item.content_type == ContentType.PAPER])
        papers.sort(
            key=lambda item: (
                -(item.score.total_score if item.score else 0.0),
                -compute_paper_credibility_score(item),
                -self._sort_timestamp(item),
            )
        )
        return papers[:PAPER_TABLE_LIMIT]

    def _item_text_haystack(self, item: Item) -> str:
        return normalize_whitespace(
            " ".join(
                value
                for value in (
                    item.title,
                    item.source_name,
                    item.insight.short_summary if item.insight else "",
                    item.insight.why_it_matters if item.insight else "",
                    item.content.cleaned_text if item.content else "",
                )
                if value
            )
        ).lower()

    def _contains_any_keyword(self, haystack: str, keywords: tuple[str, ...]) -> bool:
        return any(keyword in haystack for keyword in keywords)

    def _is_headline_candidate(self, item: Item) -> bool:
        if item.content_type == ContentType.PAPER:
            return False
        if item.content_type in {ContentType.POST, ContentType.THREAD, ContentType.SIGNAL}:
            return True
        if item.content and item.content.word_count > 1600:
            return False
        return self._contains_any_keyword(self._item_text_haystack(item), HEADLINE_KEYWORDS)

    def _is_side_signal_candidate(self, item: Item) -> bool:
        if build_related_mentions(item):
            return True
        if item.content_type in {ContentType.NEWSLETTER, ContentType.POST, ContentType.THREAD, ContentType.SIGNAL}:
            return True
        return self._contains_any_keyword(self._item_text_haystack(item), SIDE_SIGNAL_KEYWORDS)

    def _build_weekly_groups(self, digest_models: list[Digest]) -> dict[str, list[DigestEntryRead]]:
        aggregates: dict[str, WeeklyEntryAggregate] = {}
        for digest in digest_models:
            for entry in sorted(digest.entries, key=lambda current: (SECTION_PRIORITY[current.section], current.rank)):
                if entry.item.triage_status == TriageStatus.ARCHIVED:
                    continue
                current = aggregates.get(entry.item.id)
                candidate = WeeklyEntryAggregate(
                    item=entry.item,
                    note=entry.note,
                    section=entry.section,
                    section_priority=SECTION_PRIORITY[entry.section],
                    best_rank=entry.rank,
                )
                if current is None:
                    aggregates[entry.item.id] = candidate
                    continue
                if candidate.section_priority < current.section_priority:
                    aggregates[entry.item.id] = candidate
                    continue
                if candidate.section_priority == current.section_priority:
                    if candidate.best_rank < current.best_rank:
                        aggregates[entry.item.id] = candidate
                        continue
                    if (
                        candidate.best_rank == current.best_rank
                        and self._sort_timestamp(candidate.item) > self._sort_timestamp(current.item)
                    ):
                        aggregates[entry.item.id] = candidate

        grouped: dict[DigestSection, list[WeeklyEntryAggregate]] = {
            DigestSection.HEADLINES: [],
            DigestSection.EDITORIAL_SHORTLIST: [],
            DigestSection.INTERESTING_SIDE_SIGNALS: [],
            DigestSection.REMAINING_READS: [],
        }
        for aggregate in aggregates.values():
            grouped[aggregate.section].append(aggregate)

        section_keys = {
            DigestSection.HEADLINES: "headlines",
            DigestSection.EDITORIAL_SHORTLIST: "editorial_shortlist",
            DigestSection.INTERESTING_SIDE_SIGNALS: "interesting_side_signals",
            DigestSection.REMAINING_READS: "remaining_reads",
        }
        result: dict[str, list[DigestEntryRead]] = {
            "headlines": [],
            "editorial_shortlist": [],
            "interesting_side_signals": [],
            "remaining_reads": [],
        }
        for section, entries in grouped.items():
            entries.sort(
                key=lambda aggregate: (
                    -(aggregate.item.score.total_score if aggregate.item.score else 0.0),
                    aggregate.best_rank,
                    -self._sort_timestamp(aggregate.item),
                )
            )
            for rank, aggregate in enumerate(entries[: SECTION_LIMITS[section]], start=1):
                result[section_keys[section]].append(
                    DigestEntryRead(
                        item=build_item_list_entry(aggregate.item),
                        note=self._entry_note(aggregate.item, aggregate.note),
                        rank=rank,
                    )
                )
        return result

    def _edition_day_for_item(self, item: Item, timezone_name: str) -> date | None:
        return edition_day_for_datetimes(
            published_at=item.published_at,
            first_seen_at=item.first_seen_at,
            timezone_name=timezone_name,
        )

    def _sort_timestamp(self, item: Item) -> float:
        value = item.published_at or item.first_seen_at
        if value is None:
            return 0.0
        normalized = value if value.tzinfo else value.replace(tzinfo=UTC)
        return normalized.timestamp()

    def _entry_note(self, item: Item, note: str | None = None) -> str | None:
        return compact_signal_note(
            note or (item.insight.why_it_matters if item.insight else None),
            title=normalize_item_title(item.title, content_type=item.content_type),
            summary=item.insight.short_summary if item.insight else "",
            fallback_text=item.content.cleaned_text if item.content else "",
        )

    def _attach_entries(self, digest: Digest, section: DigestSection, items: list[Item]) -> None:
        for index, item in enumerate(items, start=1):
            digest.entries.append(
                DigestEntry(
                    item=item,
                    section=section,
                    rank=index,
                    note=self._entry_note(item),
                )
            )

    def _editorial_note_present(self, value: str | None) -> bool:
        return bool(normalize_whitespace(value))

    def _ensure_digest_editorial_note(self, digest: Digest) -> Digest:
        if self._editorial_note_present(digest.editorial_note) or digest.brief_date is None:
            return digest

        editorial_shortlist_entries = self._sorted_digest_entries(
            digest,
            DigestSection.EDITORIAL_SHORTLIST,
        )
        headline_entries = self._sorted_digest_entries(digest, DigestSection.HEADLINES)
        side_signal_entries = self._sorted_digest_entries(
            digest,
            DigestSection.INTERESTING_SIDE_SIGNALS,
        )
        remaining_entries = self._sorted_digest_entries(digest, DigestSection.REMAINING_READS)
        fallback_items = [entry.item for entry in editorial_shortlist_entries] or [
            entry.item
            for entries in (headline_entries, side_signal_entries, remaining_entries)
            for entry in entries
        ][:3]

        note, _ = self._compose_editorial_note(
            brief_date=digest.brief_date,
            title=digest.title,
            editorial_shortlist=[
                self._editorial_note_item_payload(
                    entry.item,
                    section=DigestSection.EDITORIAL_SHORTLIST,
                    rank=entry.rank,
                    note=entry.note,
                )
                for entry in editorial_shortlist_entries
            ],
            headlines=[
                self._editorial_note_item_payload(
                    entry.item,
                    section=DigestSection.HEADLINES,
                    rank=entry.rank,
                    note=entry.note,
                )
                for entry in headline_entries
            ],
            interesting_side_signals=[
                self._editorial_note_item_payload(
                    entry.item,
                    section=DigestSection.INTERESTING_SIDE_SIGNALS,
                    rank=entry.rank,
                    note=entry.note,
                )
                for entry in side_signal_entries
            ],
            remaining_reads=[
                self._editorial_note_item_payload(
                    entry.item,
                    section=DigestSection.REMAINING_READS,
                    rank=entry.rank,
                    note=entry.note,
                )
                for entry in remaining_entries
            ],
            fallback_note=self._build_editorial_note(fallback_items),
            audio_script=digest.audio_brief_script,
        )
        if not self._editorial_note_present(note):
            return digest

        digest.editorial_note = note
        self.db.add(digest)
        self.db.commit()
        refreshed = self._get_digest_model_by_date(digest.brief_date, data_mode=digest.data_mode)
        return refreshed or digest

    def _sorted_digest_entries(self, digest: Digest, section: DigestSection) -> list[DigestEntry]:
        return sorted(
            [entry for entry in digest.entries if entry.section == section],
            key=lambda entry: entry.rank,
        )[: EDITORIAL_NOTE_SECTION_LIMITS[section]]

    def _build_editorial_note_item_payloads(self, items: list[Item], *, section: DigestSection) -> list[dict[str, Any]]:
        return [
            self._editorial_note_item_payload(item, section=section, rank=index)
            for index, item in enumerate(items[: EDITORIAL_NOTE_SECTION_LIMITS[section]], start=1)
        ]

    def _editorial_note_item_payload(
        self,
        item: Item,
        *,
        section: DigestSection,
        rank: int,
        note: str | None = None,
    ) -> dict[str, Any]:
        return {
            "item_id": item.id,
            "section": section.value,
            "rank": rank,
            "title": normalize_item_title(item.title, content_type=item.content_type),
            "source_name": item.source_name,
            "content_type": item.content_type.value,
            "note": self._entry_note(item, note),
            "short_summary": (
                normalize_whitespace(item.insight.short_summary)
                if item.insight and item.insight.short_summary
                else None
            ),
            "why_it_matters": (
                normalize_whitespace(item.insight.why_it_matters)
                if item.insight and item.insight.why_it_matters
                else None
            ),
            "whats_new": (
                normalize_whitespace(item.insight.whats_new)
                if item.insight and item.insight.whats_new
                else None
            ),
            "caveats": (
                normalize_whitespace(item.insight.caveats)
                if item.insight and item.insight.caveats
                else None
            ),
        }

    def _compose_editorial_note(
        self,
        *,
        brief_date: date,
        title: str,
        editorial_shortlist: list[dict[str, Any]],
        headlines: list[dict[str, Any]],
        interesting_side_signals: list[dict[str, Any]],
        remaining_reads: list[dict[str, Any]],
        fallback_note: str,
        audio_script: str | None = None,
    ) -> tuple[str, dict[str, int]]:
        normalized_fallback = normalize_whitespace(fallback_note) or self._build_editorial_note([])
        if not any((editorial_shortlist, headlines, interesting_side_signals, remaining_reads, normalize_whitespace(audio_script))):
            return normalized_fallback, self.ingestion_service._empty_ai_usage()

        payload = self.ingestion_service.llm.compose_editorial_note(
            {
                "title": title,
                "brief_date": brief_date.isoformat(),
                "editorial_shortlist": editorial_shortlist,
                "headlines": headlines,
                "interesting_side_signals": interesting_side_signals,
                "remaining_reads": remaining_reads,
                "audio_script": audio_script,
                "fallback_note": normalized_fallback,
            }
        )
        note = normalize_whitespace(str(payload.get("note") or normalized_fallback))
        if note and note[-1] not in ".!?":
            note = f"{note}."
        return note or normalized_fallback, self.ingestion_service._normalize_ai_usage(payload.get("_usage"))

    def _build_editorial_note(self, items: list[Any]) -> str:
        themes = self._editorial_themes(items)
        if not themes:
            return (
                "This edition surfaced a small set of research signals. "
                "The lead set is grouped below for a closer read."
            )

        return (
            f"This edition highlights {self._join_editorial_themes(themes)}. "
            f"{self._build_editorial_mix_sentence(items)}"
        )

    def _editorial_themes(self, items: list[Any], *, limit: int = 3) -> list[str]:
        themes: list[str] = []
        seen: set[str] = set()
        for item in items:
            theme = self._editorial_theme_for_item(item)
            if not theme:
                continue
            normalized = theme.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            themes.append(theme)
            if len(themes) >= limit:
                break
        return themes

    def _editorial_theme_for_item(self, item: Any) -> str | None:
        candidates = [
            normalize_item_title(self._item_title(item), content_type=self._item_content_type(item)),
            self._item_why_it_matters(item),
            self._item_short_summary(item),
            self._item_cleaned_text(item),
        ]
        for candidate in candidates:
            theme = self._condense_editorial_theme(candidate)
            if theme:
                return theme
        return None

    def _condense_editorial_theme(self, value: str | None) -> str | None:
        normalized = normalize_whitespace(value)
        if not normalized:
            return None
        normalized = EDITORIAL_EMPH_RE.sub(r"\1", normalized)
        normalized = normalized.replace("{", " ").replace("}", " ").replace("\\", "")
        normalized = normalize_whitespace(normalized)

        candidates = [normalized]
        candidates.extend(
            segment.strip(" -,:;.!?")
            for segment in EDITORIAL_THEME_SPLIT_RE.split(normalized)
            if segment.strip(" -,:;.!?")
        )
        for candidate in candidates:
            stripped = self._strip_editorial_prefixes(candidate)
            stripped = EDITORIAL_CLAUSE_TRIM_RE.sub("", stripped).strip(" -,:;.!?")
            stripped = normalize_whitespace(stripped)
            if not stripped:
                continue
            words = stripped.split()
            if len(words) > 8:
                stripped = " ".join(words[:8]).rstrip(" -,:;")
            stripped = self._trim_editorial_theme(stripped)
            if not stripped or self._is_generic_editorial_theme(stripped):
                continue
            return self._sentence_case_editorial_theme(stripped)
        return None

    def _join_editorial_themes(self, themes: list[str]) -> str:
        if len(themes) == 1:
            return themes[0]
        if len(themes) == 2:
            return f"{themes[0]} and {themes[1]}"
        return f"{', '.join(themes[:-1])}, and {themes[-1]}"

    def _strip_editorial_prefixes(self, value: str) -> str:
        stripped = EDITORIAL_PREFIX_RE.sub("", value).strip()
        stripped = EDITORIAL_LEADING_PHRASE_RE.sub("", stripped).strip()
        return stripped

    def _trim_editorial_theme(self, value: str) -> str:
        trimmed = value.rstrip(" -,:;.!?")
        while trimmed:
            words = trimmed.split()
            if not words:
                return ""
            if words[-1].lower() not in {"a", "an", "and", "for", "from", "in", "of", "on", "or", "the", "to", "with"}:
                return trimmed
            trimmed = " ".join(words[:-1]).rstrip(" -,:;.!?")
        return ""

    def _is_generic_editorial_theme(self, value: str) -> bool:
        if EDITORIAL_GENERIC_RE.fullmatch(value):
            return True
        return len(re.findall(r"[A-Za-z][A-Za-z0-9\-]*", value)) < 2

    def _sentence_case_editorial_theme(self, value: str) -> str:
        if len(value) >= 2 and value[0].isupper() and not value[:2].isupper():
            return f"{value[0].lower()}{value[1:]}"
        return value

    def _build_editorial_mix_sentence(self, items: list[Any]) -> str:
        source_count = len(
            {
                source_name.strip().lower()
                for item in items
                if (source_name := self._item_source_name(item)).strip()
            }
        )
        type_labels = [
            self._editorial_content_type_label(content_type)
            for item in items
            if (content_type := self._item_content_type(item)) is not None
        ]
        if not type_labels:
            return "It centers on the strongest signal surfaced in this edition."

        counts: dict[str, int] = {}
        for label in type_labels:
            counts[label] = counts.get(label, 0) + 1
        mix = ", ".join(
            f"{count} {label if count == 1 else self._pluralize_editorial_label(label)}"
            for label, count in counts.items()
        )
        distinct_source_count = source_count or 1
        source_label = "source" if distinct_source_count == 1 else "sources"
        return f"The lead set combines {mix} from {distinct_source_count} distinct {source_label}."

    def _editorial_content_type_label(self, content_type: ContentType) -> str:
        return {
            ContentType.ARTICLE: "article",
            ContentType.PAPER: "paper",
            ContentType.NEWSLETTER: "newsletter",
            ContentType.POST: "post",
            ContentType.THREAD: "thread",
            ContentType.SIGNAL: "signal",
        }.get(content_type, "item")

    def _pluralize_editorial_label(self, label: str) -> str:
        if label == "analysis":
            return "analyses"
        if label.endswith("y"):
            return f"{label[:-1]}ies"
        return f"{label}s"

    def _item_short_summary(self, item: Any) -> str | None:
        insight = getattr(item, "insight", None)
        if insight and getattr(insight, "short_summary", None):
            return insight.short_summary
        return getattr(item, "short_summary", None)

    def _item_why_it_matters(self, item: Any) -> str | None:
        insight = getattr(item, "insight", None)
        if insight and getattr(insight, "why_it_matters", None):
            return insight.why_it_matters
        return None

    def _item_cleaned_text(self, item: Any) -> str | None:
        content = getattr(item, "content", None)
        if content and getattr(content, "cleaned_text", None):
            return content.cleaned_text
        return None

    def _item_title(self, item: Any) -> str:
        return str(getattr(item, "title", "") or "")

    def _item_content_type(self, item: Any) -> ContentType | None:
        return getattr(item, "content_type", None)

    def _item_source_name(self, item: Any) -> str:
        return str(getattr(item, "source_name", "") or "")

    def _collect_follow_ups(self, items: list[Item]) -> list[str]:
        follow_ups: list[str] = []
        for item in items:
            if item.insight:
                follow_ups.extend(item.insight.follow_up_questions)
        deduped = list(dict.fromkeys(follow_ups))
        return deduped[:5]

    def _unique_items(self, items: list[Item]) -> list[Item]:
        unique: list[Item] = []
        seen: set[str] = set()
        for item in items:
            if item.id in seen:
                continue
            seen.add(item.id)
            unique.append(item)
        return unique

    def _build_audio_brief_context(self, digest: Digest) -> tuple[dict[str, Any], dict[str, int]]:
        shortlisted_entries = self._shortlist_audio_entries(digest)
        ai_usage = self.ingestion_service._empty_ai_usage()
        for entry in shortlisted_entries:
            if not entry.item.insight or not entry.item.insight.short_summary:
                _, usage, _ = self.ingestion_service.ensure_insight_with_usage(entry.item)
                ai_usage = self.ingestion_service._merge_ai_usage(ai_usage, usage)
        self.db.flush()
        return (
            {
                "title": digest.title,
                "brief_date": digest.brief_date.isoformat(),
                "target_duration_minutes": 5,
                "editorial_note": digest.editorial_note or "",
                "suggested_follow_ups": digest.suggested_follow_ups,
                "shortlisted_items": [
                    {
                        "item_id": entry.item.id,
                        "title": normalize_item_title(entry.item.title, content_type=entry.item.content_type),
                        "source_name": entry.item.source_name,
                        "content_type": entry.item.content_type.value,
                        "section": entry.section,
                        "rank": entry.rank,
                        "note": self._entry_note(entry.item, entry.note),
                        "short_summary": entry.item.insight.short_summary if entry.item.insight else None,
                        "why_it_matters": (
                            normalize_whitespace(entry.item.insight.why_it_matters or "")
                            if entry.item.insight and entry.item.insight.why_it_matters
                            else None
                        ),
                        "whats_new": (
                            normalize_whitespace(entry.item.insight.whats_new or "")
                            if entry.item.insight and entry.item.insight.whats_new
                            else None
                        ),
                        "caveats": (
                            normalize_whitespace(entry.item.insight.caveats or "")
                            if entry.item.insight and entry.item.insight.caveats
                            else None
                        ),
                        "follow_up_questions": (
                            [
                                normalize_whitespace(question)
                                for question in entry.item.insight.follow_up_questions[:2]
                                if isinstance(question, str) and normalize_whitespace(question)
                            ]
                            if entry.item.insight
                            else []
                        ),
                        "source_excerpt": (
                            normalize_whitespace(entry.item.content.cleaned_text)[:600]
                            if entry.item.content and entry.item.content.cleaned_text
                            else None
                        ),
                    }
                    for entry in shortlisted_entries
                ],
            },
            ai_usage,
        )

    def _compile_audio_brief(self, digest: Digest, payload: dict) -> tuple[str, list[dict]]:
        shortlisted_entries = self._shortlist_audio_entries(digest)
        intro = self._normalize_audio_block(
            payload.get("intro"),
            fallback=digest.editorial_note or f"Here is your research voice summary for {digest.brief_date.isoformat()}.",
        )
        outro = self._normalize_audio_block(
            payload.get("outro"),
            fallback=(
                f"Carry forward this question: {digest.suggested_follow_ups[0]}"
                if digest.suggested_follow_ups
                else "That is the shortlist for today."
            ),
        )
        raw_chapters = payload.get("chapters") if isinstance(payload.get("chapters"), list) else []
        chapters_by_item_id = {
            str(chapter.get("item_id") or ""): chapter
            for chapter in raw_chapters
            if isinstance(chapter, dict) and chapter.get("item_id")
        }

        script_parts = [intro]
        chapters: list[dict] = []
        running_offset = self._estimate_speech_seconds(intro)
        for entry in shortlisted_entries:
            chapter = chapters_by_item_id.get(entry.item.id, {})
            normalized_title = normalize_item_title(entry.item.title, content_type=entry.item.content_type)
            compact_note = self._entry_note(entry.item, entry.note)
            headline = self._normalize_audio_block(
                chapter.get("headline"),
                fallback=normalized_title,
            )
            narration = self._normalize_audio_block(
                chapter.get("narration"),
                fallback=(entry.item.insight.short_summary if entry.item.insight else compact_note or normalized_title),
            )
            chapter_script = narration or headline
            chapters.append(
                {
                    "item_id": entry.item.id,
                    "item_title": normalized_title,
                    "section": entry.section,
                    "rank": entry.rank,
                    "headline": headline,
                    "narration": narration,
                    "offset_seconds": running_offset,
                }
            )
            script_parts.append(chapter_script)
            running_offset += self._estimate_speech_seconds(chapter_script)
        script_parts.append(outro)
        script = "\n\n".join(part for part in script_parts if part)
        return script, chapters

    def _shortlist_audio_entries(self, digest: Digest) -> list[AudioDigestEntry]:
        selected: list[AudioDigestEntry] = []
        seen_item_ids: set[str] = set()

        def push(item: Item, *, note: str | None, section: str, rank: int) -> None:
            if item.triage_status == TriageStatus.ARCHIVED or item.id in seen_item_ids:
                return
            seen_item_ids.add(item.id)
            selected.append(AudioDigestEntry(item=item, note=note, section=section, rank=rank))

        shortlist_entries = sorted(
            [
                entry
                for entry in digest.entries
                if entry.section == DigestSection.EDITORIAL_SHORTLIST and entry.item.triage_status != TriageStatus.ARCHIVED
            ],
            key=lambda entry: entry.rank,
        )
        for entry in shortlist_entries[: AUDIO_SECTION_LIMITS[DigestSection.EDITORIAL_SHORTLIST]]:
            push(entry.item, note=entry.note, section=entry.section.value, rank=entry.rank)

        if digest.brief_date:
            paper_pool = self._paper_table_items(
                self._representative_items_for_edition_day(digest.brief_date, data_mode=digest.data_mode)
            )
            added_papers = 0
            for paper_rank, item in enumerate(paper_pool, start=1):
                if item.id in seen_item_ids:
                    continue
                push(item, note=self._entry_note(item), section="papers_table", rank=paper_rank)
                added_papers += 1
                if added_papers >= PAPER_AUDIO_LIMIT:
                    break

        for section in (
            DigestSection.HEADLINES,
            DigestSection.INTERESTING_SIDE_SIGNALS,
            DigestSection.REMAINING_READS,
        ):
            section_entries = sorted(
                [
                    entry
                    for entry in digest.entries
                    if entry.section == section and entry.item.triage_status != TriageStatus.ARCHIVED
                ],
                key=lambda entry: entry.rank,
            )
            for entry in section_entries[: AUDIO_SECTION_LIMITS[section]]:
                push(entry.item, note=entry.note, section=entry.section.value, rank=entry.rank)
        return selected

    def _normalize_audio_block(self, value: object, *, fallback: str) -> str:
        if isinstance(value, str):
            cleaned = re.sub(r"\s+", " ", value).strip()
            if cleaned:
                return cleaned if cleaned.endswith((".", "!", "?")) else f"{cleaned}."
        fallback_cleaned = re.sub(r"\s+", " ", fallback).strip()
        if not fallback_cleaned:
            return ""
        return fallback_cleaned if fallback_cleaned.endswith((".", "!", "?")) else f"{fallback_cleaned}."

    def _estimate_speech_seconds(self, text: str | None) -> int:
        if not text:
            return 0
        word_count = len(re.findall(r"\b\w+\b", text))
        if not word_count:
            return 0
        return max(1, round(word_count / 2.35))

    def _ensure_audio_artifact(self, digest: Digest) -> None:
        if not digest.audio_brief_script:
            raise RuntimeError("Audio summary script is missing.")
        path = self.voice_client.ensure_cached_audio(digest.id, digest.audio_brief_script)
        estimated_ai_cost_usd = float((digest.audio_metadata_json or {}).get("estimated_ai_cost_usd") or 0.0)
        estimated_tts_cost_usd = self.voice_client.estimate_synthesis_cost_usd(digest.audio_brief_script)
        digest.audio_brief_status = RunStatus.SUCCEEDED.value
        digest.audio_brief_error = None
        digest.audio_artifact_provider = self.voice_client.provider_name
        digest.audio_artifact_voice = self.voice_client.voice_name
        digest.audio_duration_seconds = digest.audio_duration_seconds or estimate_audio_duration_seconds(
            digest.audio_brief_script
        )
        digest.audio_metadata_json = {
            **(digest.audio_metadata_json or {}),
            "tts_character_count": self.voice_client.estimate_character_count(digest.audio_brief_script),
            "voice_pricing_tier": self.voice_client.pricing_tier,
            "estimated_tts_cost_usd": estimated_tts_cost_usd,
            "estimated_total_cost_usd": round(estimated_ai_cost_usd + estimated_tts_cost_usd, 6),
            "audio_cache_key": self.voice_client.cache_key_for_digest(digest.id),
            "audio_file_format": self.voice_client.output_format,
            "audio_file_size_bytes": path.stat().st_size,
        }
        self.db.add(digest)
        self.db.commit()
        self.db.refresh(digest)
