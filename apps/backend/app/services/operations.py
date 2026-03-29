from __future__ import annotations

import logging
from datetime import date

from fastapi import BackgroundTasks
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.metrics import record_operation_event
from app.db.models import (
    Digest,
    DigestEntry,
    IngestionRun,
    IngestionRunType,
    Item,
    ItemCluster,
    ItemContent,
    ItemInsight,
    ItemMention,
    ItemScore,
    RunStatus,
    UserAction,
    ZoteroExport,
    ZoteroMatch,
)
from app.services.briefs import BriefService
from app.services.ingestion import IngestionService
from app.services.item_enrichment import ItemEnrichmentService
from app.services.items import ItemService
from app.services.scheduling import ScheduleService

logger = logging.getLogger(__name__)


class OperationService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.settings = get_settings()
        self.schedule_service = ScheduleService(db)

    def _build_content_clear_detail(
        self,
        *,
        item_count: int,
        digest_count: int,
        history_count: int,
    ) -> str:
        if item_count == 0 and digest_count == 0 and history_count == 0:
            return (
                "No stored content or operation history was found. Sources, connections, and "
                "profile settings were left untouched."
            )
        return (
            f"Cleared {item_count} stored item{'s' if item_count != 1 else ''} and "
            f"reset {digest_count} generated brief{'s' if digest_count != 1 else ''}. "
            f"Removed {history_count} operation histor{'y' if history_count == 1 else 'ies'}. "
            "Sources, connections, and profile settings were left untouched."
        )

    def enqueue_ingest(self, background_tasks: BackgroundTasks | None = None) -> str:
        ingestion_service = IngestionService(self.db)
        cycle_run = ingestion_service.create_ingest_cycle_run()
        execution_mode = (
            "celery"
            if self.settings.app_env == "production"
            else "background"
            if background_tasks is not None
            else "inline"
        )
        try:
            from app.tasks.jobs import run_ingest_task

            if self.settings.app_env == "production":
                run_ingest_task.delay(cycle_run_id=cycle_run.id)
            elif background_tasks is not None:
                background_tasks.add_task(run_ingest_task, None, cycle_run.id)
            else:
                run_ingest_task(cycle_run_id=cycle_run.id)
        except Exception as exc:
            ingestion_service.fail_ingest_cycle_run(
                cycle_run.id,
                error=str(exc),
                message="Failed to start ingest cycle.",
            )
            record_operation_event(
                operation="ingest",
                event="enqueue_failed",
                execution_mode=execution_mode,
            )
            logger.exception(
                "operation.ingest.enqueue_failed",
                extra={"cycle_run_id": cycle_run.id},
            )
            raise
        record_operation_event(
            operation="ingest",
            event="enqueued",
            execution_mode=execution_mode,
        )
        logger.info(
            "operation.ingest.enqueued",
            extra={
                "cycle_run_id": cycle_run.id,
                "execution_mode": execution_mode,
            },
        )
        return cycle_run.id

    def enqueue_enrich_all(self, background_tasks: BackgroundTasks | None = None) -> str:
        enrichment_service = ItemEnrichmentService(self.db)
        operation_run = enrichment_service.create_backfill_operation_run()
        execution_mode = (
            "celery"
            if self.settings.app_env == "production"
            else "background"
            if background_tasks is not None
            else "inline"
        )
        try:
            from app.tasks.jobs import run_enrich_all_task

            if self.settings.app_env == "production":
                run_enrich_all_task.delay(operation_run.id)
            elif background_tasks is not None:
                background_tasks.add_task(run_enrich_all_task, operation_run.id)
            else:
                run_enrich_all_task(operation_run.id)
        except Exception as exc:
            IngestionService(self.db).finalize_operation_run(
                operation_run,
                status=RunStatus.FAILED,
                metadata={
                    "operation_kind": "corpus_enrichment_backfill",
                    "trigger": "manual_backfill",
                    "title": "Corpus enrichment backfill",
                    "summary": "Failed to start corpus enrichment backfill.",
                },
                error=str(exc),
            )
            record_operation_event(
                operation="enrich_all",
                event="enqueue_failed",
                execution_mode=execution_mode,
            )
            logger.exception(
                "operation.enrich_all.enqueue_failed",
                extra={"operation_run_id": operation_run.id},
            )
            raise
        record_operation_event(
            operation="enrich_all",
            event="enqueued",
            execution_mode=execution_mode,
        )
        logger.info(
            "operation.enrich_all.enqueued",
            extra={
                "operation_run_id": operation_run.id,
                "execution_mode": execution_mode,
            },
        )
        return operation_run.id

    def enqueue_digest(self, force: bool = False, brief_date: date | None = None) -> None:
        trigger = "connections_request" if force else "manual_digest"
        execution_mode = "celery" if self.settings.app_env == "production" else "inline"
        record_operation_event(
            operation="digest",
            event="enqueued",
            execution_mode=execution_mode,
        )
        logger.info(
            "operation.digest.enqueued",
            extra={
                "force": force,
                "brief_date": brief_date.isoformat() if brief_date else None,
                "execution_mode": execution_mode,
                "trigger": trigger,
            },
        )
        if self.settings.app_env == "production":
            from app.tasks.jobs import run_digest_task

            run_digest_task.delay(
                force=force,
                brief_date=brief_date.isoformat() if brief_date else None,
                trigger=trigger,
                editorial_note_mode="generate",
            )
        else:
            BriefService(self.db).generate_digest(
                brief_date or self.schedule_service.current_profile_date(),
                force=force,
                trigger=trigger,
                editorial_note_mode="generate",
            )

    def enqueue_failed_retries(self) -> None:
        execution_mode = "celery" if self.settings.app_env == "production" else "inline"
        record_operation_event(
            operation="retry_failed_runs",
            event="enqueued",
            execution_mode=execution_mode,
        )
        logger.info(
            "operation.retry_failed_runs.enqueued",
            extra={"execution_mode": execution_mode},
        )
        if self.settings.app_env == "production":
            from app.tasks.jobs import retry_failed_runs_task

            retry_failed_runs_task.delay()
        else:
            _, affected_edition_days = IngestionService(
                self.db
            ).retry_failed_runs_with_affected_edition_days()
            if affected_edition_days:
                BriefService(self.db).refresh_current_edition_day(trigger="ingest_refresh")

    def enqueue_zotero_sync(self) -> None:
        execution_mode = "celery" if self.settings.app_env == "production" else "inline"
        record_operation_event(
            operation="zotero_sync",
            event="enqueued",
            execution_mode=execution_mode,
        )
        logger.info(
            "operation.zotero_sync.enqueued",
            extra={"execution_mode": execution_mode},
        )
        if self.settings.app_env == "production":
            from app.tasks.jobs import run_zotero_sync_task

            run_zotero_sync_task.delay()
        else:
            ItemService(self.db).sync_zotero_matches()

    def enqueue_database_backup(
        self,
        *,
        background_tasks: BackgroundTasks | None = None,
        trigger: str = "manual_backup",
    ) -> str:
        ingestion_service = IngestionService(self.db)
        execution_mode = (
            "celery"
            if self.settings.app_env == "production"
            else "background"
            if background_tasks is not None
            else "inline"
        )
        operation_run = ingestion_service.start_operation_run(
            run_type=IngestionRunType.CLEANUP,
            operation_kind="database_backup",
            trigger=trigger,
            metadata={
                "title": "Database backup",
                "summary": "Database backup queued.",
                "basic_info": [
                    {"label": "Directory", "value": str(self.settings.database_backup_dir)},
                    {
                        "label": "Retention",
                        "value": str(self.settings.database_backup_retention_count),
                    },
                ],
            },
        )
        ingestion_service.append_operation_log(
            operation_run.id,
            message="Database backup queued.",
        )
        try:
            from app.tasks.jobs import run_database_backup_task

            if self.settings.app_env == "production":
                run_database_backup_task.delay(operation_run.id)
            elif background_tasks is not None:
                background_tasks.add_task(run_database_backup_task, operation_run.id)
            else:
                run_database_backup_task(operation_run.id)
        except Exception as exc:
            ingestion_service.append_operation_log(
                operation_run.id,
                message="Failed to start database backup.",
                level="error",
            )
            ingestion_service.finalize_operation_run(
                operation_run,
                status=RunStatus.FAILED,
                metadata={
                    "summary": "Failed to start database backup.",
                },
                error=str(exc),
            )
            record_operation_event(
                operation="database_backup",
                event="enqueue_failed",
                execution_mode=execution_mode,
            )
            logger.exception(
                "operation.database_backup.enqueue_failed",
                extra={"operation_run_id": operation_run.id},
            )
            raise
        record_operation_event(
            operation="database_backup",
            event="enqueued",
            execution_mode=execution_mode,
        )
        logger.info(
            "operation.database_backup.enqueued",
            extra={
                "operation_run_id": operation_run.id,
                "execution_mode": execution_mode,
                "trigger": trigger,
            },
        )
        return operation_run.id

    def clear_content_records(self) -> dict[str, str]:
        item_count = 0
        digest_count = 0
        history_count = 0
        try:
            item_count = self.db.scalar(select(func.count()).select_from(Item)) or 0
            digest_count = self.db.scalar(select(func.count()).select_from(Digest)) or 0
            history_count = self.db.scalar(select(func.count()).select_from(IngestionRun)) or 0

            self.db.execute(delete(DigestEntry))
            self.db.execute(delete(Digest))
            self.db.execute(delete(ItemMention))
            self.db.execute(delete(UserAction))
            self.db.execute(delete(ZoteroExport))
            self.db.execute(delete(ZoteroMatch))
            self.db.execute(delete(ItemInsight))
            self.db.execute(delete(ItemScore))
            self.db.execute(delete(ItemContent))
            self.db.execute(update(ItemCluster).values(representative_item_id=None))
            self.db.execute(delete(Item))
            self.db.execute(delete(ItemCluster))
            self.db.execute(delete(IngestionRun))
            self.db.commit()

            detail = self._build_content_clear_detail(
                item_count=item_count,
                digest_count=digest_count,
                history_count=history_count,
            )
            record_operation_event(
                operation="clear_content",
                event="completed",
                execution_mode="inline",
            )
            logger.warning(
                "operation.clear_content.completed",
                extra={
                    "item_count": item_count,
                    "digest_count": digest_count,
                    "history_count": history_count,
                },
            )
            return {
                "detail": detail,
                "operation_run_id": None,
            }
        except Exception:
            record_operation_event(
                operation="clear_content",
                event="failed",
                execution_mode="inline",
            )
            self.db.rollback()
            raise
