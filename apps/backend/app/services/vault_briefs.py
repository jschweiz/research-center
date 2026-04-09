from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from app.core.logging import bind_log_context, reset_log_context
from app.db.models import ContentType, IngestionRunType, RunStatus
from app.integrations.llm import LLMClient
from app.integrations.voice import VoiceClient
from app.schemas.briefs import (
    AudioBriefChapterRead,
    AudioBriefRead,
    BriefAvailabilityRead,
    DigestEntryRead,
    DigestRead,
    PaperTableEntryRead,
)
from app.schemas.ops import OperationBasicInfoRead
from app.services.brief_dates import coverage_day_for_edition, iso_week_end, iso_week_start, local_date_for_timestamp
from app.services.profile import load_profile_snapshot
from app.services.vault_runtime import (
    RunRecorder,
    brief_priority_key,
    build_digest,
    current_profile_date,
    estimate_audio_duration_seconds,
    hydrate_digest,
    list_brief_availability,
    to_item_list_entry,
    utcnow,
)
from app.vault.store import VaultStore

BRIEF_JSON_FILENAME = "brief.json"
BRIEF_MARKDOWN_FILENAME = "brief.md"
SLIDES_FILENAME = "slides.md"
AUDIO_SCRIPT_FILENAME = "audio-script.md"
WEEKLY_SECTION_WEIGHTS = {
    "editorial_shortlist": 500,
    "papers_table": 450,
    "headlines": 300,
    "interesting_side_signals": 200,
    "remaining_reads": 100,
}


@dataclass
class WeeklyAggregate:
    item_id: str
    section: str
    best_rank: int
    note: str | None
    total_weight: int
    appearances: int
    latest_seen_at: datetime
    zotero_tags: list[str]
    credibility_score: int | None


class VaultBriefService:
    def __init__(self) -> None:
        self.store = VaultStore()
        self.llm = LLMClient()
        self.voice_client = VoiceClient()
        self.runs = RunRecorder(self.store)
        self.store.ensure_layout()

    def current_edition_date(self) -> date:
        return current_profile_date()

    def purge_cached_digests(self) -> None:
        if not self.store.briefs_dir.exists():
            return
        for path in self.store.briefs_dir.iterdir():
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)

    @staticmethod
    def _profile():
        return load_profile_snapshot()

    def get_digest_by_date(self, brief_date: date) -> DigestRead | None:
        path = self._brief_json_path(brief_date)
        if not path.exists():
            return None
        digest = DigestRead.model_validate_json(path.read_bytes())
        if not self._digest_matches_current_coverage(digest):
            return None
        return self._hydrate_digest(digest)

    def get_or_generate_by_date(self, brief_date: date) -> DigestRead | None:
        digest = self.get_digest_by_date(brief_date)
        if digest is not None:
            return digest
        return self._hydrate_digest(self.generate_digest(brief_date, force=True, trigger="load_backfill"))

    def get_or_generate_today(self) -> DigestRead | None:
        return self.get_or_generate_by_date(self.current_edition_date())

    def list_availability(self) -> BriefAvailabilityRead:
        dates = sorted(
            [
                date.fromisoformat(path.name)
                for path in self.store.briefs_dir.iterdir()
                if path.is_dir() and self._brief_json_path(date.fromisoformat(path.name)).exists()
            ]
        )
        default_day = self.current_edition_date() if self.current_edition_date() in dates else (dates[-1] if dates else None)
        return list_brief_availability(
            dates,
            default_day=default_day,
            exclude_week_start_on_or_after=iso_week_start(self.current_edition_date()),
        )

    def get_weekly_digest(self, week_start: date) -> DigestRead | None:
        if week_start >= iso_week_start(self.current_edition_date()):
            return None
        week_end = iso_week_end(week_start)
        days = [week_start + timedelta(days=offset) for offset in range(7)]
        digests = [self.get_digest_by_date(day) for day in days]
        digests = [digest for digest in digests if digest is not None]
        if not digests:
            return None
        latest = max(digests, key=lambda digest: digest.generated_at)
        sections = self._aggregate_weekly_sections(digests)
        return DigestRead(
            id=f"week:{week_start.isoformat()}",
            period_type="week",
            brief_date=None,
            week_start=week_start,
            week_end=week_end,
            coverage_start=week_start,
            coverage_end=week_end,
            data_mode=latest.data_mode,
            title=f"Research Brief · Week of {week_start.isoformat()}",
            editorial_note=f"Weekly digest aggregated from {len(digests)} persisted daily briefs.",
            suggested_follow_ups=[],
            audio_brief=None,
            generated_at=latest.generated_at,
            editorial_shortlist=sections["editorial_shortlist"],
            headlines=sections["headlines"],
            interesting_side_signals=sections["interesting_side_signals"],
            remaining_reads=sections["remaining_reads"],
            papers_table=sections["papers_table"],
        )

    def generate_digest(
        self,
        brief_date: date,
        *,
        force: bool = False,
        trigger: str = "manual_digest",
        editorial_note_mode: str | None = None,
    ) -> DigestRead:
        if not force:
            existing = self.get_digest_by_date(brief_date)
            if existing is not None:
                return existing

        run = self.runs.start(
            run_type=IngestionRunType.DIGEST,
            operation_kind="brief_generation",
            trigger=trigger,
            title="Brief generation",
            summary=f"Generating the brief for {brief_date.isoformat()}.",
        )
        lease = None
        try:
            profile = self._profile()
            lease = self.store.acquire_lease(name="generate-brief", owner="mac", ttl_seconds=600)
            items = self._eligible_items(brief_date)
            editorial_shortlist, headlines, side_signals, remaining, papers = self._partition_items(
                items,
                profile.brief_sections.model_dump(),
            )
            existing_audio_brief = self._existing_audio_brief(brief_date)
            digest_payload = {
                "title": f"Research Brief · {brief_date.isoformat()}",
                "brief_date": brief_date.isoformat(),
                "summary_depth": profile.summary_depth,
                "editorial_note_guidance": profile.prompt_guidance.editorial_note,
                "editorial_shortlist": self._prompt_section_payload("editorial_shortlist", editorial_shortlist),
                "headlines": self._prompt_section_payload("headlines", headlines),
                "interesting_side_signals": self._prompt_section_payload("interesting_side_signals", side_signals),
                "remaining_reads": self._prompt_section_payload("remaining_reads", remaining),
                "audio_script": existing_audio_brief.script if existing_audio_brief else None,
                "fallback_note": "The vault has been re-ranked into a concise working brief.",
            }
            log_context_token = bind_log_context(
                operation_run_id=run.id,
                operation_kind=run.operation_kind,
                brief_date=brief_date.isoformat(),
                artifact="editorial_note",
            )
            try:
                editorial_payload = self.llm.compose_editorial_note(digest_payload)
            finally:
                reset_log_context(log_context_token)
            trace_payload = editorial_payload.get("_trace")
            if isinstance(trace_payload, dict):
                self.runs.record_ai_trace(run, trace_payload)
            editorial_note = editorial_payload.get("note")
            follow_ups = self._follow_up_questions(
                editorial_shortlist,
                headlines,
                side_signals,
                remaining,
                limit=profile.brief_sections.follow_up_questions_count,
            )
            digest = build_digest(
                brief_date=brief_date,
                title=f"Research Brief · {brief_date.isoformat()}",
                editorial_note=editorial_note,
                follow_ups=follow_ups,
                generated_at=utcnow(),
                audio_brief=existing_audio_brief,
                editorial_shortlist=editorial_shortlist,
                headlines=headlines,
                interesting_side_signals=side_signals,
                remaining_reads=remaining,
                papers_table=papers,
            )

            brief_dir = self.store.brief_dir_for_date(brief_date)
            brief_dir.mkdir(parents=True, exist_ok=True)
            self.store.write_bytes(self._brief_json_path(brief_date), digest.model_dump_json(indent=2).encode("utf-8"))
            self.store.write_text(self._brief_markdown_path(brief_date), self._render_brief_markdown(digest))
            self.store.write_text(self._slides_path(brief_date), self._render_slides_markdown(digest))
            run.affected_edition_days = [brief_date]
            run.basic_info.extend(
                [
                    OperationBasicInfoRead(label="Brief date", value=brief_date.isoformat()),
                    OperationBasicInfoRead(label="Items", value=str(len(items))),
                    OperationBasicInfoRead(label="Brief dir", value=str(brief_dir)),
                ]
            )
            self.runs.finish(run, status=RunStatus.SUCCEEDED, summary=f"Generated the brief for {brief_date.isoformat()}.")
            return self._hydrate_digest(digest)
        except Exception as exc:
            run.errors.append(str(exc))
            self.runs.finish(run, status=RunStatus.FAILED, summary=f"Brief generation failed for {brief_date.isoformat()}.")
            raise
        finally:
            if lease is not None:
                self.store.release_lease(lease)

    def generate_audio_brief(self, brief_date: date) -> AudioBriefRead | None:
        digest = self.get_or_generate_by_date(brief_date)
        if digest is None:
            return None

        run = self.runs.start(
            run_type=IngestionRunType.DIGEST,
            operation_kind="audio_generation",
            trigger="manual_audio",
            title="Audio generation",
            summary=f"Generating audio for {brief_date.isoformat()}.",
        )
        lease = None
        try:
            profile = self._profile()
            lease = self.store.acquire_lease(name="generate-audio", owner="mac", ttl_seconds=900)
            items_index = self.store.load_items_index()
            item_lookup = {item.id: item for item in items_index.items}
            shortlisted_items = []
            for section_name, entries in (
                ("editorial_shortlist", digest.editorial_shortlist),
                ("papers_table", digest.papers_table),
                ("headlines", digest.headlines),
                ("interesting_side_signals", digest.interesting_side_signals),
                ("remaining_reads", digest.remaining_reads),
            ):
                for entry in entries[: profile.audio_brief_settings.max_items_per_section]:
                    record = item_lookup.get(entry.item.id)
                    shortlisted_items.append(
                        {
                            "item_id": entry.item.id,
                            "title": entry.item.title,
                            "source_name": entry.item.source_name,
                            "content_type": entry.item.content_type.value,
                            "short_summary": entry.item.short_summary,
                            "why_it_matters": None,
                            "whats_new": None,
                            "caveats": None,
                            "follow_up_questions": [],
                            "note": entry.note,
                            "section": section_name,
                            "rank": entry.rank,
                            "source_excerpt": self._source_excerpt(record.cleaned_text if record else None),
                        }
                    )
            log_context_token = bind_log_context(
                operation_run_id=run.id,
                operation_kind=run.operation_kind,
                brief_date=brief_date.isoformat(),
                artifact="audio_brief",
            )
            try:
                audio_payload = self.llm.compose_audio_brief(
                    {
                        "brief_date": brief_date.isoformat(),
                        "title": digest.title,
                        "summary_depth": profile.summary_depth,
                        "target_duration_minutes": profile.audio_brief_settings.target_duration_minutes,
                        "audio_prompt_guidance": profile.prompt_guidance.audio_brief,
                        "editorial_note": digest.editorial_note,
                        "suggested_follow_ups": digest.suggested_follow_ups,
                        "shortlisted_items": shortlisted_items,
                    }
                )
            finally:
                reset_log_context(log_context_token)
            trace_payload = audio_payload.get("_trace")
            if isinstance(trace_payload, dict):
                self.runs.record_ai_trace(run, trace_payload)
            chapters = [
                AudioBriefChapterRead(
                    item_id=chapter["item_id"],
                    item_title=item_lookup.get(chapter["item_id"]).title if item_lookup.get(chapter["item_id"]) else chapter["item_id"],
                    section=next((item["section"] for item in shortlisted_items if item["item_id"] == chapter["item_id"]), "editorial_shortlist"),
                    rank=next((item["rank"] for item in shortlisted_items if item["item_id"] == chapter["item_id"]), 1),
                    headline=chapter["headline"],
                    narration=chapter["narration"],
                    offset_seconds=index * 50,
                )
                for index, chapter in enumerate(audio_payload.get("chapters", []))
            ]
            script_parts = [
                str(audio_payload.get("intro") or "").strip(),
                *[chapter.headline for chapter in chapters],
                *[chapter.narration for chapter in chapters],
                str(audio_payload.get("outro") or "").strip(),
            ]
            script = "\n\n".join(part for part in script_parts if part).strip()
            brief_dir = self.store.brief_dir_for_date(brief_date)
            brief_dir.mkdir(parents=True, exist_ok=True)
            self.store.write_text(brief_dir / AUDIO_SCRIPT_FILENAME, script + "\n")

            audio_path: Path | None = None
            error: str | None = None
            status = RunStatus.SUCCEEDED.value
            if self.voice_client.configured:
                audio_path = brief_dir / f"audio.{self.voice_client.output_format}"
                audio_bytes = self.voice_client.synthesize_to_bytes(script)
                self.store.write_bytes(audio_path, audio_bytes)
                self.runs.record_tts_cost(
                    run,
                    cost_usd=self.voice_client.estimate_synthesis_cost_usd(script),
                )
            else:
                error = (
                    "Google Cloud TTS is not configured. The audio script was written, "
                    "but no audio file was generated."
                )
                status = RunStatus.FAILED.value

            audio_brief = AudioBriefRead(
                status=status,
                script=script,
                chapters=chapters,
                estimated_duration_seconds=estimate_audio_duration_seconds(script),
                audio_url=audio_path.name if audio_path is not None else None,
                audio_duration_seconds=estimate_audio_duration_seconds(script),
                provider=self.voice_client.provider_name if audio_path is not None else None,
                voice=self.voice_client.voice_name if audio_path is not None else None,
                error=error,
                generated_at=utcnow(),
                metadata={"generation_mode": audio_payload.get("generation_mode")},
            )
            updated_digest = digest.model_copy(update={"audio_brief": audio_brief})
            self.store.write_bytes(
                self._brief_json_path(brief_date),
                updated_digest.model_dump_json(indent=2).encode("utf-8"),
            )
            self.store.write_text(self._brief_markdown_path(brief_date), self._render_brief_markdown(updated_digest))
            self.store.write_text(self._slides_path(brief_date), self._render_slides_markdown(updated_digest))
            run.affected_edition_days = [brief_date]
            run.basic_info.extend(
                [
                    OperationBasicInfoRead(label="Brief date", value=brief_date.isoformat()),
                    OperationBasicInfoRead(label="Script", value=str(brief_dir / AUDIO_SCRIPT_FILENAME)),
                    OperationBasicInfoRead(label="Audio", value=str(audio_path) if audio_path else "not generated"),
                ]
            )
            result_status = RunStatus.SUCCEEDED if audio_path is not None else RunStatus.FAILED
            self.runs.finish(
                run,
                status=result_status,
                summary=(
                    f"Generated audio for {brief_date.isoformat()}."
                    if audio_path is not None
                    else f"Wrote the audio script for {brief_date.isoformat()}, but audio generation is not configured."
                ),
            )
            return audio_brief
        except Exception as exc:
            run.errors.append(str(exc))
            self.runs.finish(run, status=RunStatus.FAILED, summary=f"Audio generation failed for {brief_date.isoformat()}.")
            raise
        finally:
            if lease is not None:
                self.store.release_lease(lease)

    def get_audio_artifact_path(self, brief_date: date) -> Path:
        brief_dir = self.store.brief_dir_for_date(brief_date)
        mp3_path = brief_dir / "audio.mp3"
        if mp3_path.exists():
            return mp3_path
        digest = self.get_digest_by_date(brief_date)
        if digest and digest.audio_brief and digest.audio_brief.audio_url:
            candidate = brief_dir / digest.audio_brief.audio_url
            if candidate.exists():
                return candidate
        raise RuntimeError(f"Audio artifact not found for {brief_date.isoformat()}.")

    def _eligible_items(self, brief_date: date):
        items = []
        coverage_day = coverage_day_for_edition(brief_date)
        timezone_name = self._profile().timezone
        for item in self.store.load_items_index().items:
            if item.index_visibility == "hidden" or item.status == "archived":
                continue
            reference = item.published_at or item.ingested_at
            local_day = local_date_for_timestamp(reference, timezone_name)
            if local_day == coverage_day:
                items.append(item)
        items.sort(key=brief_priority_key, reverse=True)
        return items

    @staticmethod
    def _digest_matches_current_coverage(digest: DigestRead) -> bool:
        if digest.period_type != "day" or digest.brief_date is None:
            return True
        coverage_day = coverage_day_for_edition(digest.brief_date)
        return digest.coverage_start == coverage_day and digest.coverage_end == coverage_day

    def _hydrate_digest(self, digest: DigestRead) -> DigestRead:
        items_index = self.store.load_items_index()
        item_lookup = {item.id: item for item in items_index.items}
        starred_ids = self._starred_ids()
        return hydrate_digest(digest, item_lookup=item_lookup, starred_ids=starred_ids)

    def _starred_ids(self) -> set[str]:
        return set(self.store.load_starred_items().item_ids)

    def _aggregate_weekly_sections(
        self,
        digests: list[DigestRead],
    ) -> dict[str, list[DigestEntryRead] | list[PaperTableEntryRead]]:
        items_index = self.store.load_items_index()
        item_lookup = {item.id: item for item in items_index.items}
        starred_ids = self._starred_ids()
        aggregates: dict[str, WeeklyAggregate] = {}

        for digest in digests:
            for section_name, entries in (
                ("editorial_shortlist", digest.editorial_shortlist),
                ("papers_table", digest.papers_table),
                ("headlines", digest.headlines),
                ("interesting_side_signals", digest.interesting_side_signals),
                ("remaining_reads", digest.remaining_reads),
            ):
                for entry in entries:
                    item = item_lookup.get(entry.item.id)
                    if item is None or item.index_visibility == "hidden" or item.status == "archived":
                        continue

                    weight = WEEKLY_SECTION_WEIGHTS[section_name] + max(0, 20 - entry.rank)
                    current = aggregates.get(entry.item.id)
                    note = getattr(entry, "note", None)
                    zotero_tags = list(getattr(entry, "zotero_tags", []))
                    credibility_score = getattr(entry, "credibility_score", None)
                    if current is None:
                        aggregates[entry.item.id] = WeeklyAggregate(
                            item_id=entry.item.id,
                            section=section_name,
                            best_rank=entry.rank,
                            note=note,
                            total_weight=weight,
                            appearances=1,
                            latest_seen_at=digest.generated_at,
                            zotero_tags=zotero_tags,
                            credibility_score=credibility_score,
                        )
                        continue

                    replace_primary = self._weekly_candidate_better(
                        section_name=section_name,
                        rank=entry.rank,
                        seen_at=digest.generated_at,
                        current=current,
                    )
                    current.total_weight += weight
                    current.appearances += 1
                    if digest.generated_at > current.latest_seen_at:
                        current.latest_seen_at = digest.generated_at
                    if replace_primary:
                        current.section = section_name
                        current.best_rank = entry.rank
                        current.note = note
                        current.zotero_tags = zotero_tags
                        current.credibility_score = credibility_score

        grouped: dict[str, list] = {
            "editorial_shortlist": [],
            "papers_table": [],
            "headlines": [],
            "interesting_side_signals": [],
            "remaining_reads": [],
        }
        profile = self._profile()

        def sort_key(entry: WeeklyAggregate) -> tuple[int, int, int, float, str]:
            item = item_lookup[entry.item_id]
            return (
                -entry.total_weight,
                -entry.appearances,
                entry.best_rank,
                -entry.latest_seen_at.timestamp(),
                item.title.lower(),
            )

        for aggregate in sorted(aggregates.values(), key=sort_key):
            item = item_lookup.get(aggregate.item_id)
            if item is None:
                continue
            starred = item.id in starred_ids
            if aggregate.section == "papers_table":
                grouped["papers_table"].append(
                    {
                        "item": aggregate.item_id,
                        "entry": item,
                        "starred": starred,
                        "zotero_tags": aggregate.zotero_tags,
                        "credibility_score": aggregate.credibility_score,
                    }
                )
                continue
            grouped[aggregate.section].append(
                {
                    "item": aggregate.item_id,
                    "entry": item,
                    "starred": starred,
                    "note": aggregate.note,
                }
            )

        editorial_shortlist = [
            DigestEntryRead(
                item=to_item_list_entry(item["entry"], starred=item["starred"]),
                note=item["note"],
                rank=index + 1,
            )
            for index, item in enumerate(
                grouped["editorial_shortlist"][: profile.brief_sections.editorial_shortlist_count]
            )
        ]
        headlines = [
            DigestEntryRead(
                item=to_item_list_entry(item["entry"], starred=item["starred"]),
                note=item["note"],
                rank=index + 1,
            )
            for index, item in enumerate(grouped["headlines"][: profile.brief_sections.headlines_count])
        ]
        side_signals = [
            DigestEntryRead(
                item=to_item_list_entry(item["entry"], starred=item["starred"]),
                note=item["note"],
                rank=index + 1,
            )
            for index, item in enumerate(
                grouped["interesting_side_signals"][: profile.brief_sections.side_signals_count]
            )
        ]
        remaining = [
            DigestEntryRead(
                item=to_item_list_entry(item["entry"], starred=item["starred"]),
                note=item["note"],
                rank=index + 1,
            )
            for index, item in enumerate(
                grouped["remaining_reads"][: profile.brief_sections.remaining_reads_count]
            )
        ]
        papers = [
            PaperTableEntryRead(
                item=to_item_list_entry(item["entry"], starred=item["starred"]),
                rank=index + 1,
                zotero_tags=item["zotero_tags"],
                credibility_score=item["credibility_score"],
            )
            for index, item in enumerate(grouped["papers_table"][: profile.brief_sections.papers_count])
        ]

        return {
            "editorial_shortlist": editorial_shortlist,
            "headlines": headlines,
            "interesting_side_signals": side_signals,
            "remaining_reads": remaining,
            "papers_table": papers,
        }

    @staticmethod
    def _weekly_candidate_better(
        *,
        section_name: str,
        rank: int,
        seen_at: datetime,
        current: WeeklyAggregate,
    ) -> bool:
        next_priority = WEEKLY_SECTION_WEIGHTS[section_name]
        current_priority = WEEKLY_SECTION_WEIGHTS[current.section]
        if next_priority != current_priority:
            return next_priority > current_priority
        if rank != current.best_rank:
            return rank < current.best_rank
        return seen_at > current.latest_seen_at

    @staticmethod
    def _partition_items(items, settings: dict[str, int]):
        papers = [
            item for item in items if item.content_type == ContentType.PAPER
        ][: settings["papers_count"]]
        shortlist = items[: settings["editorial_shortlist_count"]]
        used = {item.id for item in shortlist}
        headlines = [item for item in items if item.id not in used][: settings["headlines_count"]]
        used.update(item.id for item in headlines)
        side_signals = [
            item
            for item in items
            if item.id not in used and item.content_type in {ContentType.SIGNAL, ContentType.POST, ContentType.THREAD}
        ][: settings["side_signals_count"]]
        used.update(item.id for item in side_signals)
        remaining = [item for item in items if item.id not in used][: settings["remaining_reads_count"]]
        return shortlist, headlines, side_signals, remaining, papers

    @staticmethod
    def _follow_up_questions(*groups, limit: int) -> list[str]:
        if limit <= 0:
            return []
        questions: list[str] = []
        for group in groups:
            for item in group:
                title = item.title.strip().rstrip(".")
                if not title:
                    continue
                if item.content_type == ContentType.PAPER:
                    question = f"Which baseline or replication would most quickly validate the claim in {title}?"
                elif item.content_type == ContentType.NEWSLETTER:
                    question = f"Which original source should be checked before relying on the newsletter take about {title}?"
                elif item.content_type in {ContentType.SIGNAL, ContentType.THREAD, ContentType.POST}:
                    question = f"What follow-on signal would show whether {title} is material rather than just momentum?"
                else:
                    question = f"What concrete follow-on signal would show whether {title} matters beyond the announcement?"
                if question not in questions:
                    questions.append(question)
                if len(questions) >= limit:
                    return questions
        return questions

    @staticmethod
    def _prompt_section_payload(section_name: str, items) -> list[dict[str, str | int | None]]:
        payload: list[dict[str, str | int | None]] = []
        for index, item in enumerate(items, start=1):
            payload.append(
                {
                    "section": section_name,
                    "rank": index,
                    "item_id": item.id,
                    "title": item.title,
                    "source_name": item.source_name,
                    "content_type": item.content_type.value,
                    "note": item.short_summary,
                    "short_summary": item.short_summary,
                    "why_it_matters": None,
                    "whats_new": None,
                    "caveats": None,
                }
            )
        return payload

    @staticmethod
    def _source_excerpt(value: str | None) -> str | None:
        if not value:
            return None
        normalized = " ".join(value.split())
        return normalized[:360].strip() or None

    def _existing_audio_brief(self, brief_date: date) -> AudioBriefRead | None:
        existing = self.get_digest_by_date(brief_date)
        return existing.audio_brief if existing else None

    def _brief_json_path(self, brief_date: date) -> Path:
        return self.store.brief_dir_for_date(brief_date) / BRIEF_JSON_FILENAME

    def _brief_markdown_path(self, brief_date: date) -> Path:
        return self.store.brief_dir_for_date(brief_date) / BRIEF_MARKDOWN_FILENAME

    def _slides_path(self, brief_date: date) -> Path:
        return self.store.brief_dir_for_date(brief_date) / SLIDES_FILENAME

    @staticmethod
    def _render_brief_markdown(digest: DigestRead) -> str:
        lines = [
            f"# {digest.title}",
            "",
            f"Coverage: {digest.coverage_start.isoformat()}",
        ]
        if digest.editorial_note:
            lines.extend(["", digest.editorial_note])
        for title, entries in (
            ("Editorial shortlist", digest.editorial_shortlist),
            ("Headlines", digest.headlines),
            ("Interesting side signals", digest.interesting_side_signals),
            ("Remaining reads", digest.remaining_reads),
        ):
            if not entries:
                continue
            lines.extend(["", f"## {title}", ""])
            for entry in entries:
                lines.append(f"- [{entry.item.title}]({entry.item.canonical_url})")
                if entry.note:
                    lines.append(f"  - {entry.note}")
        if digest.audio_brief:
            lines.extend(["", "## Audio", "", digest.audio_brief.script or "Audio script generated."])
        return "\n".join(lines).strip() + "\n"

    @staticmethod
    def _render_slides_markdown(digest: DigestRead) -> str:
        lines = [
            f"# Slides · {digest.title}",
            "",
            "## Lead",
            "",
            digest.editorial_note or "Daily research brief.",
        ]
        for entry in digest.editorial_shortlist:
            lines.extend(
                [
                    "",
                    f"## {entry.item.title}",
                    "",
                    entry.note or entry.item.short_summary or "Add speaker notes.",
                ]
            )
        return "\n".join(lines).strip() + "\n"
