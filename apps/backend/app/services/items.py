from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.core.config import get_settings
from app.db.models import (
    ActionType,
    ConnectionProvider,
    ConnectionStatus,
    ContentType,
    IngestionRunType,
    Item,
    ItemCluster,
    RunStatus,
    TriageStatus,
    UserAction,
    ZoteroExport,
    ZoteroMatch,
)
from app.integrations.zotero import ZoteroClient
from app.services.brief_dates import edition_day_for_datetimes
from app.services.briefs import BriefService
from app.services.connections import ConnectionService
from app.services.data_mode import filter_items_for_data_mode
from app.services.ingestion import IngestionService
from app.services.presenters import build_item_detail, build_item_list_entry
from app.services.profile import ProfileService
from app.services.ranking import RankingService
from app.services.scheduling import ensure_utc
from app.services.zotero_auto_tags import merge_zotero_tags, resolve_zotero_auto_tag_vocabulary

ARXIV_PREFIX_RE = re.compile(r"^\[[^\]]+\]\s*")
ARXIV_ID_RE = re.compile(r"^(?:arxiv:)?\d{4}\.\d{4,5}(?:v\d+)?$", re.IGNORECASE)


def build_topic_hint(title: str, *, max_terms: int = 4) -> str:
    normalized = ARXIV_PREFIX_RE.sub("", title.lower()).strip()
    terms: list[str] = []
    for raw_token in normalized.split():
        token = re.sub(r"^[^\w]+|[^\w]+$", "", raw_token)
        if not token:
            continue
        if ARXIV_ID_RE.fullmatch(token):
            continue
        if not any(char.isalpha() for char in token):
            continue
        terms.append(token)
        if len(terms) >= max_terms:
            break
    return " ".join(terms)


class ItemService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.settings = get_settings()
        self.ingestion_service = IngestionService(db)
        self.connection_service = ConnectionService(db)
        self.ranking_service = RankingService(db)

    def _base_query(self):
        return (
            select(Item)
            .options(
                selectinload(Item.score),
                selectinload(Item.insight),
                selectinload(Item.content),
                selectinload(Item.zotero_matches),
                selectinload(Item.cluster).selectinload(ItemCluster.items),
            )
        )

    def list_items(
        self,
        *,
        query: str | None = None,
        status_filter: str | None = None,
        content_type: str | None = None,
        source_id: str | None = None,
        sort: str = "importance",
    ):
        statement = self._base_query()
        if query:
            statement = statement.where(Item.title.ilike(f"%{query}%"))
        if status_filter:
            statement = statement.where(Item.triage_status == TriageStatus(status_filter))
        else:
            statement = statement.where(Item.triage_status != TriageStatus.ARCHIVED)
        if content_type:
            statement = statement.where(Item.content_type == ContentType(content_type))
        if source_id:
            statement = statement.where(Item.source_id == source_id)

        profile = ProfileService(self.db).get_profile()
        items = filter_items_for_data_mode(list(self.db.scalars(statement).all()), profile.data_mode)
        if sort == "newest":
            items.sort(
                key=lambda item: ensure_utc(item.published_at).timestamp() if item.published_at else 0.0,
                reverse=True,
            )
        elif sort == "source":
            items.sort(key=lambda item: (item.source_name.lower(), item.title.lower()))
        else:
            items.sort(key=lambda item: item.score.total_score if item.score else 0.0, reverse=True)
        return [build_item_list_entry(item) for item in items]

    def get_item_detail(self, item_id: str):
        item = self.db.scalar(self._base_query().where(Item.id == item_id))
        if not item:
            return None
        if not item.insight or not item.insight.short_summary:
            operation_run = self.ingestion_service.start_operation_run(
                run_type=IngestionRunType.DEEPER_SUMMARY,
                operation_kind="item_insight_generation",
                trigger="item_open",
                metadata=self._build_item_operation_metadata(
                    operation_kind="item_insight_generation",
                    trigger="item_open",
                    title="Item insight generation",
                    summary="Generating item insight.",
                    item=item,
                ),
            )
            usage = self.ingestion_service._empty_ai_usage()
            try:
                _, usage, _ = self.ingestion_service.ensure_insight_with_usage(item)
                self.ingestion_service.finalize_operation_run(
                    operation_run,
                    status=RunStatus.SUCCEEDED,
                    metadata=self._build_item_operation_metadata(
                        operation_kind="item_insight_generation",
                        trigger="item_open",
                        title="Item insight generation",
                        summary="Generated insight for one item.",
                        item=item,
                        ai_usage=usage,
                        basic_info=[
                            {"label": "Generated fields", "value": "summary, why it matters, caveats"},
                        ],
                    ),
                )
            except Exception as exc:
                self.db.rollback()
                self.ingestion_service.finalize_operation_run(
                    operation_run,
                    status=RunStatus.FAILED,
                    metadata=self._build_item_operation_metadata(
                        operation_kind="item_insight_generation",
                        trigger="item_open",
                        title="Item insight generation",
                        summary="Item insight generation failed.",
                        item=item,
                        ai_usage=usage,
                    ),
                    error=str(exc),
                )
                raise
        self._record_action(item, ActionType.OPENED)
        self.db.add(item)
        self.db.commit()
        item = self.db.scalar(self._base_query().where(Item.id == item_id))
        return build_item_detail(item) if item else None

    def import_url(self, url: str):
        result = self.ingestion_service.import_manual_url_with_result(url)
        BriefService(self.db).refresh_edition_days(result.affected_edition_days)
        self.db.refresh(result.item)
        return build_item_detail(result.item)

    def archive_item(self, item_id: str):
        item = self.db.get(Item, item_id)
        if not item:
            return None
        item.triage_status = TriageStatus.ARCHIVED
        self._record_action(item, ActionType.ARCHIVED)
        self.db.add(item)
        self.db.commit()
        return {"item_id": item.id, "triage_status": item.triage_status, "detail": "Item archived."}

    def toggle_star(self, item_id: str):
        item = self.db.get(Item, item_id)
        if not item:
            return None
        item.starred = not item.starred
        item.manual_priority_boost = 0.12 if item.starred else 0.0
        self.ranking_service.score_item(item)
        self._record_action(item, ActionType.STARRED, {"starred": item.starred})
        self.db.add(item)
        self.db.commit()
        return {
            "item_id": item.id,
            "triage_status": item.triage_status,
            "detail": "Item starred." if item.starred else "Item unstarred.",
        }

    def ignore_similar(self, item_id: str):
        item = self.db.scalar(self._base_query().where(Item.id == item_id))
        if not item:
            return None
        if item.cluster:
            for related in item.cluster.items:
                related.triage_status = TriageStatus.ARCHIVED
                self.db.add(related)
        profile = ProfileService(self.db).get_profile()
        topic_hint = build_topic_hint(item.title)
        if topic_hint and topic_hint not in profile.ignored_topics:
            profile.ignored_topics = [*profile.ignored_topics, topic_hint]
        self._record_action(item, ActionType.IGNORED_SIMILAR, {"cluster_id": item.cluster_id, "topic_hint": topic_hint})
        self.db.add(profile)
        self.db.commit()
        return {
            "item_id": item.id,
            "triage_status": item.triage_status,
            "detail": "Similar items will be deprioritized.",
        }

    def save_to_zotero(self, item_id: str, tags: list[str], note_prefix: str | None):
        item = self.db.scalar(self._base_query().where(Item.id == item_id))
        if not item:
            return None

        connection = self.connection_service.get_zotero_connection(refresh_if_needed=True)
        payload = self.connection_service.get_payload(ConnectionProvider.ZOTERO)
        export = ZoteroExport(item=item, status=RunStatus.PENDING)
        self.db.add(export)
        if (
            not connection
            or connection.status != ConnectionStatus.CONNECTED
            or not payload
            or not payload.get("api_key")
            or not payload.get("library_id")
        ):
            export.status = RunStatus.FAILED
            export.confidence_score = 0.0
            connection_error = (
                connection.metadata_json.get("last_error")
                if connection and isinstance(connection.metadata_json.get("last_error"), str)
                else None
            )
            export.error = connection_error or "Zotero connection is missing."
            item.triage_status = TriageStatus.REVIEW
            self._record_action(item, ActionType.SAVED_TO_ZOTERO, {"status": "missing_connection"})
            self.db.commit()
            return {
                "item_id": item.id,
                "triage_status": item.triage_status,
                "detail": (
                    f"Zotero connection needs attention: {connection_error}. Item moved to Needs Review."
                    if connection_error
                    else "Zotero connection missing. Item moved to Needs Review."
                ),
            }

        client = ZoteroClient(
            api_key=payload["api_key"],
            library_id=payload["library_id"],
            library_type=payload.get("library_type", "users"),
        )
        collection_name = (
            connection.metadata_json.get("collection_name")
            if isinstance(connection.metadata_json.get("collection_name"), str)
            else None
        )
        auto_tag_vocabulary = resolve_zotero_auto_tag_vocabulary(connection.metadata_json)
        operation_run = self.ingestion_service.start_operation_run(
            run_type=IngestionRunType.ZOTERO_SYNC,
            operation_kind="zotero_export",
            trigger="save_to_zotero",
            metadata=self._build_item_operation_metadata(
                operation_kind="zotero_export",
                trigger="save_to_zotero",
                title="Save to Zotero",
                summary="Preparing Zotero export.",
                item=item,
                basic_info=[
                    {"label": "Manual tags", "value": str(len(tags))},
                    {"label": "Collection", "value": collection_name or "Default library"},
                ],
            ),
        )
        ai_usage = self.ingestion_service._empty_ai_usage()
        auto_tags: list[str] = []
        applied_tags = list(tags)
        try:
            if not item.insight or not item.insight.short_summary:
                _, insight_usage, _ = self.ingestion_service.ensure_insight_with_usage(item)
                ai_usage = self.ingestion_service._merge_ai_usage(ai_usage, insight_usage)
                self.db.flush()
            auto_tags, auto_tag_usage = self.ingestion_service.llm.suggest_zotero_tags_with_usage(
                {
                    "title": item.title,
                    "source_name": item.source_name,
                    "content_type": item.content_type.value,
                },
                self.ingestion_service._analysis_text(item),
                auto_tag_vocabulary,
                insight={
                    "short_summary": item.insight.short_summary if item.insight else None,
                    "why_it_matters": item.insight.why_it_matters if item.insight else None,
                    "method": item.insight.method if item.insight else None,
                    "result": item.insight.result if item.insight else None,
                },
            )
            ai_usage = self.ingestion_service._merge_ai_usage(
                ai_usage,
                self.ingestion_service._normalize_ai_usage(auto_tag_usage),
            )
            applied_tags = merge_zotero_tags(tags, auto_tags)
        except Exception as exc:
            self.db.rollback()
            self.ingestion_service.finalize_operation_run(
                operation_run,
                status=RunStatus.FAILED,
                metadata=self._build_item_operation_metadata(
                    operation_kind="zotero_export",
                    trigger="save_to_zotero",
                    title="Save to Zotero",
                    summary="Zotero export preparation failed.",
                    item=item,
                    ai_usage=ai_usage,
                    basic_info=[
                        {"label": "Manual tags", "value": str(len(tags))},
                        {"label": "Auto tags", "value": str(len(auto_tags))},
                        {"label": "Applied tags", "value": str(len(applied_tags))},
                        {"label": "Collection", "value": collection_name or "Default library"},
                    ],
                ),
                error=str(exc),
            )
            raise
        try:
            result = client.save_item(
                item={
                    "title": item.title,
                    "canonical_url": item.canonical_url,
                    "authors": item.authors,
                    "content_type": item.content_type.value,
                    "published_at": item.published_at.isoformat() if item.published_at else None,
                },
                insight={
                    "short_summary": item.insight.short_summary if item.insight else None,
                    "why_it_matters": item.insight.why_it_matters if item.insight else None,
                    "follow_up_questions": item.insight.follow_up_questions if item.insight else [],
                },
                tags=applied_tags,
                note_prefix=note_prefix,
                collection_name=collection_name,
            )
            if isinstance(result.response_payload, dict):
                result.response_payload = result.response_payload | {
                    "auto_tags": auto_tags,
                    "applied_tags": applied_tags,
                }
        except Exception as exc:
            export.status = RunStatus.FAILED
            export.confidence_score = 0.0
            export.response_payload = {}
            export.error = f"Zotero export error: {exc}"
            item.triage_status = TriageStatus.REVIEW
            self._record_action(item, ActionType.SAVED_TO_ZOTERO, {"status": "error"})
            self.db.commit()
            self.ingestion_service.finalize_operation_run(
                operation_run,
                status=RunStatus.FAILED,
                metadata=self._build_item_operation_metadata(
                    operation_kind="zotero_export",
                    trigger="save_to_zotero",
                    title="Save to Zotero",
                    summary="Zotero export failed.",
                    item=item,
                    ai_usage=ai_usage,
                    basic_info=[
                        {"label": "Manual tags", "value": str(len(tags))},
                        {"label": "Auto tags", "value": str(len(auto_tags))},
                        {"label": "Applied tags", "value": str(len(applied_tags))},
                        {"label": "Collection", "value": collection_name or "Default library"},
                    ],
                ),
                error=f"Zotero export error: {exc}",
            )
            return {
                "item_id": item.id,
                "triage_status": item.triage_status,
                "detail": "Zotero export failed. Item moved to Needs Review.",
            }
        export.status = RunStatus.SUCCEEDED if result.success else RunStatus.FAILED
        export.confidence_score = result.confidence_score
        export.response_payload = result.response_payload
        export.exported_at = datetime.now(UTC) if result.success else None
        export.error = None if result.success else result.detail
        item.triage_status = TriageStatus.SAVED if result.success else TriageStatus.REVIEW
        self._record_action(item, ActionType.SAVED_TO_ZOTERO, {"status": export.status.value})
        self.db.commit()
        self.ingestion_service.finalize_operation_run(
            operation_run,
            status=RunStatus.SUCCEEDED if result.success else RunStatus.FAILED,
            metadata=self._build_item_operation_metadata(
                operation_kind="zotero_export",
                trigger="save_to_zotero",
                title="Save to Zotero",
                summary=f"{len(applied_tags)} tag{'s' if len(applied_tags) != 1 else ''} applied.",
                item=item,
                ai_usage=ai_usage,
                basic_info=[
                    {"label": "Manual tags", "value": str(len(tags))},
                    {"label": "Auto tags", "value": str(len(auto_tags))},
                    {"label": "Applied tags", "value": str(len(applied_tags))},
                    {"label": "Collection", "value": collection_name or "Default library"},
                ],
            ),
            error=None if result.success else result.detail,
        )
        return {
            "item_id": item.id,
            "triage_status": item.triage_status,
            "detail": result.detail,
        }

    def enqueue_deeper_summary(self, item_id: str) -> bool:
        item = self.db.get(Item, item_id)
        if not item:
            return False
        if self.settings.app_env == "production":
            from app.tasks.jobs import generate_deeper_summary_task

            generate_deeper_summary_task.delay(item_id)
        else:
            self.ingestion_service.generate_deeper_summary(item_id)
        self._record_action(item, ActionType.ASKED_DEEPER)
        self.db.commit()
        return True

    def sync_zotero_matches(self) -> int:
        connection = self.connection_service.get_zotero_connection(refresh_if_needed=True)
        payload = self.connection_service.get_payload(ConnectionProvider.ZOTERO)
        if (
            not connection
            or connection.status != ConnectionStatus.CONNECTED
            or not payload
            or not payload.get("api_key")
            or not payload.get("library_id")
        ):
            return 0
        client = ZoteroClient(
            api_key=payload["api_key"],
            library_id=payload["library_id"],
            library_type=payload.get("library_type", "users"),
        )
        try:
            library_items = client.sync_library_items(limit=50)
        except Exception as exc:
            if connection:
                connection.status = ConnectionStatus.ERROR
                connection.metadata_json = connection.metadata_json | {"last_error": str(exc)}
                self.db.add(connection)
                self.db.commit()
            return 0
        items = list(self.db.scalars(self._base_query()).all())
        match_count = 0
        for item in items:
            existing = self.db.scalars(select(ZoteroMatch).where(ZoteroMatch.item_id == item.id)).all()
            for match in list(existing):
                self.db.delete(match)

            title_tokens = set(item.title.lower().split())
            for library_item in library_items:
                comparison_tokens = set((library_item.get("title") or "").lower().split())
                if not title_tokens or not comparison_tokens:
                    continue
                overlap = len(title_tokens & comparison_tokens) / max(len(title_tokens | comparison_tokens), 1)
                if overlap >= 0.25:
                    self.db.add(
                        ZoteroMatch(
                            item=item,
                            library_item_key=library_item["key"],
                            title=library_item["title"],
                            similarity_score=round(overlap, 4),
                            metadata_json=library_item,
                        )
                    )
                    match_count += 1
            self.ranking_service.score_item(item)
        if connection:
            connection.last_synced_at = datetime.now(UTC)
            self.db.add(connection)
        self.db.commit()
        return match_count

    def _record_action(self, item: Item, action_type: ActionType, metadata: dict | None = None) -> None:
        action = UserAction(item=item, action_type=action_type, metadata_json=metadata or {})
        self.db.add(action)

    def _item_operation_affected_edition_days(self, item: Item) -> list[str]:
        edition_day = edition_day_for_datetimes(
            published_at=item.published_at,
            first_seen_at=item.first_seen_at,
            timezone_name=self.settings.timezone,
        )
        if edition_day is None:
            return []
        return [edition_day.isoformat()]

    def _build_item_operation_metadata(
        self,
        *,
        operation_kind: str,
        trigger: str,
        title: str,
        summary: str,
        item: Item,
        ai_usage: dict[str, int] | None = None,
        basic_info: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        normalized_usage = self.ingestion_service._normalize_ai_usage(ai_usage)
        ai_cost_usd = self.ingestion_service._estimate_ai_cost_usd(
            prompt_tokens=normalized_usage["prompt_tokens"],
            completion_tokens=normalized_usage["completion_tokens"],
            total_tokens=normalized_usage["total_tokens"],
        )
        return {
            "operation_kind": operation_kind,
            "trigger": trigger,
            "title": title,
            "summary": summary,
            "affected_edition_days": self._item_operation_affected_edition_days(item),
            "ai_prompt_tokens": normalized_usage["prompt_tokens"],
            "ai_completion_tokens": normalized_usage["completion_tokens"],
            "ai_total_tokens": normalized_usage["total_tokens"],
            "ai_cost_usd": ai_cost_usd,
            "tts_cost_usd": 0.0,
            "total_cost_usd": ai_cost_usd,
            "basic_info": [
                {"label": "Item", "value": item.title},
                {"label": "Source", "value": item.source_name},
                {"label": "Type", "value": item.content_type.value},
                *(basic_info or []),
            ],
        }
