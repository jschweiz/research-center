from __future__ import annotations

import argparse
import logging
from contextlib import suppress
from datetime import date, time

from celery import current_task

from app.core.logging import bind_task_context, reset_task_context
from app.core.metrics import track_task_metrics
from app.db.models import ConnectionProvider, IngestionRun, IngestionRunType, RunStatus, Source
from app.db.session import get_session_factory
from app.services.backups import DatabaseBackupService
from app.services.briefs import BriefService
from app.services.connections import ConnectionService
from app.services.ingestion import IngestionService
from app.services.item_enrichment import ItemEnrichmentService
from app.services.items import ItemService
from app.services.operations import OperationService
from app.services.scheduling import ScheduleService
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


def _task_metadata(default_name: str) -> tuple[str | None, str, tuple]:
    task = None
    task_id = None
    task_name = default_name
    with suppress(Exception):
        task = current_task
        request = getattr(task, "request", None)
        task_id = getattr(request, "id", None)
        task_name = getattr(task, "name", None) or default_name
    return task_id, task_name, bind_task_context(task_id=task_id, task_name=task_name)


@celery_app.task(name="research_center.run_ingest")
def run_ingest_task(source_id: str | None = None, cycle_run_id: str | None = None) -> int:
    _, task_name, task_tokens = _task_metadata("research_center.run_ingest")
    try:
        with track_task_metrics(task_name) as set_task_outcome:
            logger.info(
                "task.ingest.started",
                extra={"source_id": source_id, "cycle_run_id": cycle_run_id},
            )
            with get_session_factory()() as db:
                service = IngestionService(db)
                brief_service = BriefService(db)
                enrichment_service = ItemEnrichmentService(db)
                try:
                    if source_id:
                        source = db.get(Source, source_id)
                        if source:
                            (
                                ingested,
                                affected_edition_days,
                            ) = service.run_source_with_affected_edition_days(source)
                            enrichment_service.enrich_item_ids(
                                service.drain_changed_item_ids(),
                                trigger="post_ingest",
                            )
                            if affected_edition_days:
                                brief_service.refresh_current_edition_day(trigger="ingest_refresh")
                            logger.info(
                                "task.ingest.completed",
                                extra={
                                    "source_id": source_id,
                                    "cycle_run_id": cycle_run_id,
                                    "ingested_count": ingested,
                                    "affected_edition_days": sorted(
                                        day.isoformat() for day in affected_edition_days
                                    ),
                                },
                            )
                            return ingested
                        set_task_outcome("skipped")
                        logger.warning("task.ingest.source_missing", extra={"source_id": source_id})
                        return 0
                    ingested, affected_edition_days = (
                        service.run_all_sources_with_affected_edition_days(
                            cycle_run_id=cycle_run_id
                        )
                    )
                    enrichment_service.enrich_item_ids(
                        service.drain_changed_item_ids(),
                        trigger="post_ingest",
                    )
                    if cycle_run_id:
                        if affected_edition_days:
                            current_edition_day = brief_service.current_edition_date()
                            service.append_operation_log(
                                cycle_run_id,
                                message=(
                                    "Refreshing current edition only: "
                                    f"{current_edition_day.isoformat()}."
                                ),
                            )
                        else:
                            service.append_operation_log(
                                cycle_run_id,
                                message="No edition refresh needed after ingest.",
                            )
                    if affected_edition_days:
                        brief_service.refresh_current_edition_day(trigger="ingest_refresh")
                    if cycle_run_id and affected_edition_days:
                        service.append_operation_log(
                            cycle_run_id,
                            message="Current edition refresh finished.",
                            level="success",
                        )
                    logger.info(
                        "task.ingest.completed",
                        extra={
                            "cycle_run_id": cycle_run_id,
                            "ingested_count": ingested,
                            "affected_edition_days": sorted(
                                day.isoformat() for day in affected_edition_days
                            ),
                        },
                    )
                    return ingested
                except Exception as exc:
                    if cycle_run_id:
                        service.fail_ingest_cycle_run(
                            cycle_run_id,
                            error=str(exc),
                            message="Ingest worker failed.",
                        )
                    logger.exception(
                        "task.ingest.failed",
                        extra={"source_id": source_id, "cycle_run_id": cycle_run_id},
                    )
                    raise
    finally:
        reset_task_context(task_tokens)


@celery_app.task(name="research_center.run_digest")
def run_digest_task(
    force: bool = False,
    only_if_due: bool = False,
    brief_date: str | None = None,
    trigger: str | None = None,
    editorial_note_mode: str | None = None,
) -> str:
    _, task_name, task_tokens = _task_metadata("research_center.run_digest")
    try:
        with track_task_metrics(task_name) as set_task_outcome:
            logger.info(
                "task.digest.started",
                extra={
                    "force": force,
                    "only_if_due": only_if_due,
                    "brief_date": brief_date,
                    "trigger": trigger,
                },
            )
            with get_session_factory()() as db:
                schedule_service = ScheduleService(db)
                target_date = (
                    date.fromisoformat(brief_date)
                    if brief_date
                    else schedule_service.current_profile_date()
                )
                if (
                    only_if_due
                    and not force
                    and brief_date is None
                    and not schedule_service.is_profile_digest_due()
                ):
                    set_task_outcome("skipped")
                    logger.info(
                        "task.digest.skipped_not_due",
                        extra={"target_date": target_date.isoformat()},
                    )
                    return "skipped:not_due"
                resolved_trigger = trigger or (
                    "morning_injection"
                    if only_if_due and brief_date is None
                    else "regenerate"
                    if force
                    else "generate"
                )
                digest = BriefService(db).generate_digest(
                    target_date,
                    force=force,
                    trigger=resolved_trigger,
                    editorial_note_mode=editorial_note_mode or "generate",
                )
                logger.info(
                    "task.digest.completed",
                    extra={
                        "digest_id": digest.id,
                        "target_date": target_date.isoformat(),
                        "trigger": resolved_trigger,
                    },
                )
                return digest.id
    except Exception:
        logger.exception(
            "task.digest.failed",
            extra={
                "force": force,
                "only_if_due": only_if_due,
                "brief_date": brief_date,
                "trigger": trigger,
            },
        )
        raise
    finally:
        reset_task_context(task_tokens)


@celery_app.task(name="research_center.run_enrich_all")
def run_enrich_all_task(operation_run_id: str | None = None) -> int:
    _, task_name, task_tokens = _task_metadata("research_center.run_enrich_all")
    try:
        with track_task_metrics(task_name):
            logger.info("task.enrich_all.started", extra={"operation_run_id": operation_run_id})
            with get_session_factory()() as db:
                enrichment_result = ItemEnrichmentService(db).enrich_all_items(
                    trigger="manual_backfill",
                    operation_run_id=operation_run_id,
                )
                if enrichment_result.updated_count:
                    BriefService(db).refresh_current_edition_day(trigger="enrichment_refresh")
                logger.info(
                    "task.enrich_all.completed",
                    extra={
                        "operation_run_id": operation_run_id,
                        "updated_count": enrichment_result.updated_count,
                    },
                )
                return enrichment_result.updated_count
    except Exception:
        logger.exception("task.enrich_all.failed", extra={"operation_run_id": operation_run_id})
        raise
    finally:
        reset_task_context(task_tokens)


@celery_app.task(name="research_center.run_zotero_sync")
def run_zotero_sync_task(only_if_due: bool = False) -> int:
    _, task_name, task_tokens = _task_metadata("research_center.run_zotero_sync")
    try:
        with track_task_metrics(task_name) as set_task_outcome:
            logger.info("task.zotero_sync.started", extra={"only_if_due": only_if_due})
            with get_session_factory()() as db:
                if only_if_due:
                    connection = ConnectionService(db).get_connection(ConnectionProvider.ZOTERO)
                    if not connection:
                        set_task_outcome("skipped")
                        logger.info("task.zotero_sync.skipped_missing_connection")
                        return 0
                    schedule_service = ScheduleService(db)
                    if not schedule_service.is_daily_job_due(
                        last_run_at=connection.last_synced_at,
                        due_time=time(hour=2, minute=0),
                        timezone_name=schedule_service.settings.timezone,
                    ):
                        set_task_outcome("skipped")
                        logger.info("task.zotero_sync.skipped_not_due")
                        return 0
                match_count = ItemService(db).sync_zotero_matches()
                logger.info("task.zotero_sync.completed", extra={"match_count": match_count})
                return match_count
    except Exception:
        logger.exception("task.zotero_sync.failed", extra={"only_if_due": only_if_due})
        raise
    finally:
        reset_task_context(task_tokens)


@celery_app.task(name="research_center.retry_failed_runs")
def retry_failed_runs_task() -> int:
    _, task_name, task_tokens = _task_metadata("research_center.retry_failed_runs")
    try:
        with track_task_metrics(task_name):
            logger.info("task.retry_failed_runs.started")
            with get_session_factory()() as db:
                (
                    ingested,
                    affected_edition_days,
                ) = IngestionService(db).retry_failed_runs_with_affected_edition_days()
                if affected_edition_days:
                    BriefService(db).refresh_current_edition_day(trigger="ingest_refresh")
                logger.info(
                    "task.retry_failed_runs.completed",
                    extra={
                        "ingested_count": ingested,
                        "affected_edition_days": sorted(
                            day.isoformat() for day in affected_edition_days
                        ),
                    },
                )
                return ingested
    except Exception:
        logger.exception("task.retry_failed_runs.failed")
        raise
    finally:
        reset_task_context(task_tokens)


@celery_app.task(name="research_center.generate_deeper_summary")
def generate_deeper_summary_task(item_id: str) -> None:
    _, task_name, task_tokens = _task_metadata("research_center.generate_deeper_summary")
    try:
        with track_task_metrics(task_name):
            logger.info("task.deeper_summary.started", extra={"item_id": item_id})
            with get_session_factory()() as db:
                IngestionService(db).generate_deeper_summary(item_id)
            logger.info("task.deeper_summary.completed", extra={"item_id": item_id})
    except Exception:
        logger.exception("task.deeper_summary.failed", extra={"item_id": item_id})
        raise
    finally:
        reset_task_context(task_tokens)


@celery_app.task(name="research_center.purge_raw_email_payloads")
def purge_raw_email_payloads_task() -> int:
    _, task_name, task_tokens = _task_metadata("research_center.purge_raw_email_payloads")
    try:
        with track_task_metrics(task_name):
            logger.info("task.raw_email_payload_purge.started")
            with get_session_factory()() as db:
                purged_count = IngestionService(db).purge_old_email_payloads()
            logger.info(
                "task.raw_email_payload_purge.completed",
                extra={"purged_count": purged_count},
            )
            return purged_count
    except Exception:
        logger.exception("task.raw_email_payload_purge.failed")
        raise
    finally:
        reset_task_context(task_tokens)


@celery_app.task(name="research_center.run_database_backup")
def run_database_backup_task(
    operation_run_id: str | None = None,
    trigger: str = "manual_backup",
) -> str:
    _, task_name, task_tokens = _task_metadata("research_center.run_database_backup")
    try:
        with track_task_metrics(task_name), get_session_factory()() as db:
            ingestion_service = IngestionService(db)
            operation_run = db.get(IngestionRun, operation_run_id) if operation_run_id else None
            resolved_trigger = trigger
            if operation_run is not None:
                metadata = (
                    operation_run.metadata_json
                    if isinstance(operation_run.metadata_json, dict)
                    else {}
                )
                resolved_trigger = str(metadata.get("trigger") or trigger)
            if operation_run is None:
                operation_run = ingestion_service.start_operation_run(
                    run_type=IngestionRunType.CLEANUP,
                    operation_kind="database_backup",
                    trigger=resolved_trigger,
                    metadata={
                        "title": "Database backup",
                        "summary": "Database backup started.",
                    },
                )
                operation_run_id = operation_run.id
            logger.info(
                "task.database_backup.started",
                extra={
                    "operation_run_id": operation_run_id,
                    "trigger": resolved_trigger,
                },
            )

            ingestion_service.append_operation_log(
                operation_run.id,
                message="Starting database backup snapshot.",
            )
            result = DatabaseBackupService(db).create_backup()
            ingestion_service.append_operation_log(
                operation_run.id,
                message=f"Created backup file {result.path.name}.",
                level="success",
            )
            if result.pruned_files:
                ingestion_service.append_operation_log(
                    operation_run.id,
                    message=(
                        f"Pruned {len(result.pruned_files)} older backup "
                        f"{'file' if len(result.pruned_files) == 1 else 'files'}."
                    ),
                    level="success",
                )
            summary = f"Created database backup {result.path.name}."
            if result.pruned_files:
                summary = (
                    f"{summary[:-1]} and pruned {len(result.pruned_files)} older "
                    f"{'snapshot' if len(result.pruned_files) == 1 else 'snapshots'}."
                )
            ingestion_service.finalize_operation_run(
                operation_run,
                status=RunStatus.SUCCEEDED,
                metadata={
                    "summary": summary,
                    "backup_file": result.path.name,
                    "backup_path": str(result.path),
                    "backup_size_bytes": result.size_bytes,
                    "backup_sha256": result.sha256,
                    "backup_table_count": result.table_count,
                    "backup_row_count": result.row_count,
                    "backup_pruned_files": result.pruned_files,
                    "alembic_version": result.alembic_version,
                    "basic_info": [
                        {"label": "File", "value": result.path.name},
                        {"label": "Size", "value": f"{result.size_bytes} bytes"},
                        {"label": "Rows", "value": str(result.row_count)},
                        {"label": "Tables", "value": str(result.table_count)},
                        {
                            "label": "Pruned",
                            "value": str(len(result.pruned_files)),
                        },
                    ],
                },
            )
            logger.info(
                "task.database_backup.completed",
                extra={
                    "operation_run_id": operation_run.id,
                    "backup_file": result.path.name,
                    "backup_size_bytes": result.size_bytes,
                    "backup_sha256": result.sha256,
                    "backup_pruned_count": len(result.pruned_files),
                },
            )
            return result.path.name
    except Exception as exc:
        if operation_run_id:
            with get_session_factory()() as db:
                ingestion_service = IngestionService(db)
                operation_run = db.get(IngestionRun, operation_run_id)
                if operation_run is not None:
                    ingestion_service.append_operation_log(
                        operation_run.id,
                        message="Database backup failed.",
                        level="error",
                    )
                    ingestion_service.finalize_operation_run(
                        operation_run,
                        status=RunStatus.FAILED,
                        metadata={
                            "summary": "Database backup failed.",
                        },
                        error=str(exc),
                    )
        logger.exception(
            "task.database_backup.failed",
            extra={"operation_run_id": operation_run_id},
        )
        raise
    finally:
        reset_task_context(task_tokens)


def main() -> None:
    parser = argparse.ArgumentParser(description="Research Center job helper.")
    parser.add_argument(
        "command",
        choices=[
            "enqueue-ingest",
            "enqueue-enrich-all",
            "enqueue-database-backup",
            "enqueue-zotero-sync",
            "enqueue-digest",
            "enqueue-purge-raw-email-payloads",
            "run-ingest-inline",
            "run-enrich-all-inline",
            "run-database-backup-inline",
            "run-zotero-sync-inline",
            "run-digest-inline",
            "run-purge-raw-email-payloads-inline",
        ],
    )
    args = parser.parse_args()
    logger.info("job_helper.command_received", extra={"command": args.command})

    try:
        if args.command == "enqueue-ingest":
            run_ingest_task.delay()
        elif args.command == "enqueue-enrich-all":
            run_enrich_all_task.delay()
        elif args.command == "enqueue-database-backup":
            with get_session_factory()() as db:
                OperationService(db).enqueue_database_backup(trigger="scheduled_backup")
        elif args.command == "enqueue-zotero-sync":
            run_zotero_sync_task.delay(only_if_due=True)
        elif args.command == "enqueue-digest":
            run_digest_task.delay(only_if_due=True)
        elif args.command == "enqueue-purge-raw-email-payloads":
            purge_raw_email_payloads_task.delay()
        elif args.command == "run-ingest-inline":
            run_ingest_task()
        elif args.command == "run-enrich-all-inline":
            run_enrich_all_task()
        elif args.command == "run-database-backup-inline":
            run_database_backup_task()
        elif args.command == "run-zotero-sync-inline":
            run_zotero_sync_task(only_if_due=True)
        elif args.command == "run-digest-inline":
            run_digest_task(only_if_due=True)
        elif args.command == "run-purge-raw-email-payloads-inline":
            purge_raw_email_payloads_task()
    except Exception:
        logger.exception("job_helper.command_failed", extra={"command": args.command})
        raise
    logger.info("job_helper.command_completed", extra={"command": args.command})


if __name__ == "__main__":
    main()
