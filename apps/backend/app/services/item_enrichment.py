from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.db.models import IngestionRun, IngestionRunType, Item, ItemCluster, RunStatus
from app.services.brief_dates import edition_day_for_datetimes
from app.services.ingestion import IngestionService
from app.services.presenters import resolve_item_organization_name
from app.services.profile import ProfileService
from app.services.ranking import RankingService
from app.services.text import normalize_whitespace

ENRICHMENT_METADATA_KEY = "llm_enrichment"
ENRICHMENT_VERSION = 1
ENRICHMENT_BATCH_SIZE = 10
ENRICHMENT_TAG_LIMIT = 8
ENRICHMENT_AUTHOR_LIMIT = 5
ENRICHMENT_REASON_LIMIT = 280


def _empty_usage() -> dict[str, int]:
    return {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }


@dataclass
class EnrichmentRunResult:
    operation_run_id: str | None = None
    status: RunStatus = RunStatus.SUCCEEDED
    processed_count: int = 0
    updated_count: int = 0
    batch_count: int = 0
    failed_batch_count: int = 0
    author_fill_count: int = 0
    ai_usage: dict[str, int] = field(default_factory=_empty_usage)
    affected_edition_days: list[date] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    skipped_reason: str | None = None


class ItemEnrichmentService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.ingestion_service = IngestionService(db)
        self.profile_service = ProfileService(db)
        self.ranking_service = RankingService(db)

    def create_backfill_operation_run(self) -> IngestionRun:
        target_count = self.db.scalar(select(func.count()).select_from(Item)) or 0
        return self.ingestion_service.start_operation_run(
            run_type=IngestionRunType.INGEST,
            operation_kind="corpus_enrichment_backfill",
            trigger="manual_backfill",
            metadata={
                "title": "Corpus enrichment backfill",
                "summary": (
                    f"Queued enrichment for {target_count} stored item"
                    f"{'' if target_count == 1 else 's'}."
                ),
                "ingested_count": target_count,
                "updated_count": 0,
                "source_count": self._batch_count(target_count),
                "failed_source_count": 0,
                "ai_prompt_tokens": 0,
                "ai_completion_tokens": 0,
                "ai_total_tokens": 0,
                "ai_cost_usd": 0.0,
                "tts_cost_usd": 0.0,
                "total_cost_usd": 0.0,
                "basic_info": [
                    {"label": "Items targeted", "value": str(target_count)},
                    {"label": "Batch size", "value": str(ENRICHMENT_BATCH_SIZE)},
                ],
                "affected_edition_days": [],
                "errors": [],
            },
        )

    def enrich_item_ids(
        self,
        item_ids: Iterable[str],
        *,
        trigger: str = "post_ingest",
        operation_run_id: str | None = None,
    ) -> EnrichmentRunResult:
        unique_ids = self._dedupe_item_ids(item_ids)
        if not unique_ids and not operation_run_id:
            return EnrichmentRunResult(skipped_reason="No items were eligible for enrichment.")
        items = self._items_for_ids(unique_ids)
        return self._run_enrichment(
            items,
            operation_kind="post_ingest_enrichment",
            trigger=trigger,
            title="Post-ingest enrichment",
            operation_run_id=operation_run_id,
        )

    def enrich_all_items(
        self,
        *,
        trigger: str = "manual_backfill",
        operation_run_id: str | None = None,
    ) -> EnrichmentRunResult:
        items = self._all_items()
        return self._run_enrichment(
            items,
            operation_kind="corpus_enrichment_backfill",
            trigger=trigger,
            title="Corpus enrichment backfill",
            operation_run_id=operation_run_id,
        )

    def _run_enrichment(
        self,
        items: list[Item],
        *,
        operation_kind: str,
        trigger: str,
        title: str,
        operation_run_id: str | None,
    ) -> EnrichmentRunResult:
        result = EnrichmentRunResult(
            operation_run_id=operation_run_id,
            processed_count=len(items),
            batch_count=self._batch_count(len(items)),
            affected_edition_days=self._affected_edition_days(items),
        )
        if not items:
            if operation_run_id:
                self._finalize_existing_run(
                    operation_run_id=operation_run_id,
                    title=title,
                    trigger=trigger,
                    operation_kind=operation_kind,
                    result=result,
                    summary="No stored items were eligible for enrichment.",
                    status=RunStatus.SUCCEEDED,
                )
            else:
                result.skipped_reason = "No items were eligible for enrichment."
            return result

        if not self._gemini_configured():
            result.skipped_reason = "Skipped enrichment because Gemini is not configured."
            if operation_run_id:
                self._finalize_existing_run(
                    operation_run_id=operation_run_id,
                    title=title,
                    trigger=trigger,
                    operation_kind=operation_kind,
                    result=result,
                    summary=result.skipped_reason,
                    status=RunStatus.SUCCEEDED,
                )
            return result

        run = self._prepare_operation_run(
            operation_run_id=operation_run_id,
            operation_kind=operation_kind,
            trigger=trigger,
            title=title,
            result=result,
        )
        result.operation_run_id = run.id
        self.ingestion_service.append_operation_log(
            run.id,
            message=(
                f"Starting enrichment for {result.processed_count} item"
                f"{'' if result.processed_count == 1 else 's'} across {result.batch_count} batch"
                f"{'' if result.batch_count == 1 else 'es'}."
            ),
        )

        profile = self.profile_service.get_profile()
        for batch_index, batch in enumerate(self._batch_items(items), start=1):
            self.ingestion_service.append_operation_log(
                run.id,
                message=(
                    f"Batch {batch_index}/{result.batch_count}: enriching "
                    f"{len(batch)} item{'' if len(batch) == 1 else 's'}."
                ),
            )
            try:
                payload = self.ingestion_service.llm.batch_enrich_items(
                    [self._build_prompt_item(item) for item in batch],
                    self._build_profile_context(profile),
                )
                usage = self.ingestion_service._normalize_ai_usage(payload.get("_usage"))
                result.ai_usage = self.ingestion_service._merge_ai_usage(result.ai_usage, usage)
                raw_results = self._normalize_batch_results(payload.get("items"))

                updated_in_batch = 0
                author_fills_in_batch = 0
                for item in batch:
                    raw_result = raw_results.get(item.id)
                    if raw_result is None:
                        continue
                    author_applied = self._apply_enrichment(
                        item,
                        raw_result,
                        trigger=trigger,
                        operation_run_id=run.id,
                    )
                    self.ranking_service.score_item(item)
                    self.db.add(item)
                    updated_in_batch += 1
                    if author_applied:
                        author_fills_in_batch += 1
                self.db.commit()
                result.updated_count += updated_in_batch
                result.author_fill_count += author_fills_in_batch
                self.ingestion_service.append_operation_log(
                    run.id,
                    message=(
                        f"Batch {batch_index}/{result.batch_count}: updated {updated_in_batch}/"
                        f"{len(batch)} item{'' if len(batch) == 1 else 's'}"
                        f" ({usage['total_tokens']} token{'' if usage['total_tokens'] == 1 else 's'}, "
                        f"{author_fills_in_batch} author fill"
                        f"{'' if author_fills_in_batch == 1 else 's'})."
                    ),
                    level="success",
                )
            except Exception as exc:
                self.db.rollback()
                result.failed_batch_count += 1
                result.errors.append(
                    f"Batch {batch_index}/{result.batch_count} failed: {exc}"
                )
                self.ingestion_service.append_operation_log(
                    run.id,
                    message=f"Batch {batch_index}/{result.batch_count} failed: {exc}",
                    level="error",
                )

        result.status = (
            RunStatus.FAILED if result.failed_batch_count else RunStatus.SUCCEEDED
        )
        summary = self._build_summary(result)
        self.ingestion_service.finalize_operation_run(
            run,
            status=result.status,
            metadata=self._build_operation_metadata(
                title=title,
                trigger=trigger,
                operation_kind=operation_kind,
                result=result,
                summary=summary,
            ),
            error="\n".join(result.errors) or None,
        )
        return result

    def _prepare_operation_run(
        self,
        *,
        operation_run_id: str | None,
        operation_kind: str,
        trigger: str,
        title: str,
        result: EnrichmentRunResult,
    ) -> IngestionRun:
        if operation_run_id:
            run = self.db.get(IngestionRun, operation_run_id)
            if run:
                return run
        return self.ingestion_service.start_operation_run(
            run_type=IngestionRunType.INGEST,
            operation_kind=operation_kind,
            trigger=trigger,
            metadata=self._build_operation_metadata(
                title=title,
                trigger=trigger,
                operation_kind=operation_kind,
                result=result,
                summary=(
                    f"Queued enrichment for {result.processed_count} item"
                    f"{'' if result.processed_count == 1 else 's'}."
                ),
            ),
        )

    def _finalize_existing_run(
        self,
        *,
        operation_run_id: str,
        title: str,
        trigger: str,
        operation_kind: str,
        result: EnrichmentRunResult,
        summary: str,
        status: RunStatus,
    ) -> None:
        run = self.db.get(IngestionRun, operation_run_id)
        if not run:
            return
        self.ingestion_service.finalize_operation_run(
            run,
            status=status,
            metadata=self._build_operation_metadata(
                title=title,
                trigger=trigger,
                operation_kind=operation_kind,
                result=result,
                summary=summary,
            ),
            error="\n".join(result.errors) or None,
        )

    def _build_profile_context(self, profile) -> dict[str, Any]:
        return {
            "favorite_topics": profile.favorite_topics,
            "favorite_authors": profile.favorite_authors,
            "favorite_sources": profile.favorite_sources,
            "ignored_topics": profile.ignored_topics,
        }

    def _build_prompt_item(self, item: Item) -> dict[str, Any]:
        return {
            "item_id": item.id,
            "title": item.title,
            "source_name": item.source_name,
            "content_type": item.content_type.value,
            "published_at": item.published_at.isoformat() if item.published_at else None,
            "authors": item.authors,
            "organization_name": resolve_item_organization_name(item),
            "short_summary": item.insight.short_summary if item.insight else None,
            "why_it_matters": item.insight.why_it_matters if item.insight else None,
            "whats_new": item.insight.whats_new if item.insight else None,
            "analysis_text": self._analysis_text(item),
        }

    def _apply_enrichment(
        self,
        item: Item,
        raw_result: dict[str, Any],
        *,
        trigger: str,
        operation_run_id: str,
    ) -> bool:
        suggested_authors = self._normalize_authors(raw_result.get("authors"))
        author_applied = False
        if not item.authors and suggested_authors:
            item.authors = suggested_authors
            author_applied = True

        existing_metadata = (
            dict(item.metadata_json) if isinstance(item.metadata_json, dict) else {}
        )
        item.metadata_json = existing_metadata | {
            ENRICHMENT_METADATA_KEY: {
            "version": ENRICHMENT_VERSION,
            "generated_at": datetime.now(UTC).isoformat(),
            "trigger": trigger,
            "model": self.ingestion_service.llm.settings.gemini_model,
            "relevance_score": self._normalize_score(raw_result.get("relevance_score")),
            "reason": self._normalize_reason(raw_result.get("reason")),
            "tags": self._normalize_tags(raw_result.get("tags")),
            "suggested_authors": suggested_authors,
            "author_applied": author_applied,
            "operation_run_id": operation_run_id,
            }
        }
        return author_applied

    def _normalize_batch_results(self, value: Any) -> dict[str, dict[str, Any]]:
        if not isinstance(value, list):
            return {}
        normalized: dict[str, dict[str, Any]] = {}
        for raw in value:
            if not isinstance(raw, dict):
                continue
            item_id = normalize_whitespace(raw.get("item_id"))
            if not item_id or item_id in normalized:
                continue
            normalized[item_id] = raw
        return normalized

    def _normalize_score(self, value: Any) -> float:
        try:
            return round(min(max(float(value), 0.0), 1.0), 4)
        except (TypeError, ValueError):
            return 0.0

    def _normalize_reason(self, value: Any) -> str:
        cleaned = normalize_whitespace(value)
        if len(cleaned) <= ENRICHMENT_REASON_LIMIT:
            return cleaned
        return cleaned[: ENRICHMENT_REASON_LIMIT - 3].rstrip() + "..."

    def _normalize_tags(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        tags: list[str] = []
        seen: set[str] = set()
        for raw in value:
            cleaned = normalize_whitespace(raw).strip(".,;:-").lower()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            tags.append(cleaned)
            if len(tags) >= ENRICHMENT_TAG_LIMIT:
                break
        return tags

    def _normalize_authors(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        authors: list[str] = []
        seen: set[str] = set()
        for raw in value:
            cleaned = normalize_whitespace(raw).strip(".,;:-")
            normalized = cleaned.lower()
            if not cleaned or normalized in seen:
                continue
            seen.add(normalized)
            authors.append(cleaned)
            if len(authors) >= ENRICHMENT_AUTHOR_LIMIT:
                break
        return authors

    def _analysis_text(self, item: Item) -> str:
        text = self.ingestion_service._analysis_text(item)
        if len(text) <= 2200:
            return text
        return text[:2197].rstrip() + "..."

    def _batch_items(self, items: list[Item]) -> list[list[Item]]:
        return [
            items[index : index + ENRICHMENT_BATCH_SIZE]
            for index in range(0, len(items), ENRICHMENT_BATCH_SIZE)
        ]

    def _items_for_ids(self, item_ids: list[str]) -> list[Item]:
        if not item_ids:
            return []
        statement = (
            select(Item)
            .options(
                selectinload(Item.source),
                selectinload(Item.content),
                selectinload(Item.insight),
                selectinload(Item.score),
                selectinload(Item.zotero_matches),
                selectinload(Item.cluster).selectinload(ItemCluster.items),
            )
            .where(Item.id.in_(item_ids))
        )
        items = list(self.db.scalars(statement).all())
        ordering = {item_id: index for index, item_id in enumerate(item_ids)}
        items.sort(key=lambda item: ordering.get(item.id, len(ordering)))
        return items

    def _all_items(self) -> list[Item]:
        statement = (
            select(Item)
            .options(
                selectinload(Item.source),
                selectinload(Item.content),
                selectinload(Item.insight),
                selectinload(Item.score),
                selectinload(Item.zotero_matches),
                selectinload(Item.cluster).selectinload(ItemCluster.items),
            )
            .order_by(Item.first_seen_at.desc())
        )
        return list(self.db.scalars(statement).all())

    def _affected_edition_days(self, items: list[Item]) -> list[date]:
        timezone_name = self.profile_service.get_profile().timezone
        days = [
            edition_day_for_datetimes(
                published_at=item.published_at,
                first_seen_at=item.first_seen_at,
                timezone_name=timezone_name,
            )
            for item in items
        ]
        return sorted({day for day in days if day is not None})

    def _build_operation_metadata(
        self,
        *,
        title: str,
        trigger: str,
        operation_kind: str,
        result: EnrichmentRunResult,
        summary: str,
    ) -> dict[str, Any]:
        ai_cost_usd = self.ingestion_service._estimate_ai_cost_usd(
            prompt_tokens=result.ai_usage["prompt_tokens"],
            completion_tokens=result.ai_usage["completion_tokens"],
            total_tokens=result.ai_usage["total_tokens"],
        )
        return {
            "operation_kind": operation_kind,
            "trigger": trigger,
            "title": title,
            "summary": summary,
            "affected_edition_days": [
                affected_day.isoformat() for affected_day in result.affected_edition_days
            ],
            "ingested_count": result.processed_count,
            "created_count": 0,
            "updated_count": result.updated_count,
            "source_count": result.batch_count,
            "failed_source_count": result.failed_batch_count,
            "ai_prompt_tokens": result.ai_usage["prompt_tokens"],
            "ai_completion_tokens": result.ai_usage["completion_tokens"],
            "ai_total_tokens": result.ai_usage["total_tokens"],
            "ai_cost_usd": ai_cost_usd,
            "tts_cost_usd": 0.0,
            "total_cost_usd": ai_cost_usd,
            "basic_info": [
                {"label": "Items targeted", "value": str(result.processed_count)},
                {"label": "Items updated", "value": str(result.updated_count)},
                {"label": "Batches", "value": str(result.batch_count)},
                {"label": "Failed batches", "value": str(result.failed_batch_count)},
                {"label": "Author fills", "value": str(result.author_fill_count)},
            ],
            "errors": result.errors,
        }

    def _build_summary(self, result: EnrichmentRunResult) -> str:
        summary = (
            f"{result.updated_count} item{'s' if result.updated_count != 1 else ''} updated "
            f"across {result.batch_count} batch{'es' if result.batch_count != 1 else ''}"
        )
        if result.author_fill_count:
            summary += (
                f" · {result.author_fill_count} author fill"
                f"{'' if result.author_fill_count == 1 else 's'}"
            )
        if result.failed_batch_count:
            summary += (
                f" · {result.failed_batch_count} failed batch"
                f"{'' if result.failed_batch_count == 1 else 'es'}"
            )
        return summary

    def _dedupe_item_ids(self, item_ids: Iterable[str]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for item_id in item_ids:
            normalized = normalize_whitespace(item_id)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    def _batch_count(self, item_count: int) -> int:
        if item_count <= 0:
            return 0
        return ((item_count - 1) // ENRICHMENT_BATCH_SIZE) + 1

    def _gemini_configured(self) -> bool:
        return bool(self.ingestion_service.llm.settings.gemini_api_key)
