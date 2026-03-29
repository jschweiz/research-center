from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db_session
from app.schemas.ops import (
    IngestionRunHistoryRead,
    JobResponse,
    RegenerateBriefRequest,
)
from app.services.ingestion import IngestionService
from app.services.operations import OperationService

CURRENT_USER = Depends(get_current_user)
DB_SESSION = Depends(get_db_session)


router = APIRouter(dependencies=[CURRENT_USER])


@router.post("/ingest-now", response_model=JobResponse)
def ingest_now(
    background_tasks: BackgroundTasks,
    db: Session = DB_SESSION,
) -> JobResponse:
    operation_run_id = OperationService(db).enqueue_ingest(background_tasks=background_tasks)
    return JobResponse(
        queued=True,
        task_name="ingest",
        detail="Ingest started.",
        operation_run_id=operation_run_id,
    )


@router.post("/enrich-all", response_model=JobResponse)
def enrich_all(
    background_tasks: BackgroundTasks,
    db: Session = DB_SESSION,
) -> JobResponse:
    operation_run_id = OperationService(db).enqueue_enrich_all(background_tasks=background_tasks)
    return JobResponse(
        queued=True,
        task_name="enrich_all",
        detail="Corpus enrichment backfill queued.",
        operation_run_id=operation_run_id,
    )


@router.post("/backup-now", response_model=JobResponse)
def backup_now(
    background_tasks: BackgroundTasks,
    db: Session = DB_SESSION,
) -> JobResponse:
    operation_run_id = OperationService(db).enqueue_database_backup(
        background_tasks=background_tasks,
        trigger="manual_backup",
    )
    return JobResponse(
        queued=True,
        task_name="database_backup",
        detail="Database backup queued.",
        operation_run_id=operation_run_id,
    )


@router.post("/regenerate-brief", response_model=JobResponse)
def regenerate_brief(
    payload: RegenerateBriefRequest | None = None,
    db: Session = DB_SESSION,
) -> JobResponse:
    OperationService(db).enqueue_digest(
        force=True,
        brief_date=payload.brief_date if payload else None,
    )
    detail = (
        f"Digest regeneration queued for {payload.brief_date.isoformat()}."
        if payload and payload.brief_date
        else "Digest regeneration queued."
    )
    return JobResponse(queued=True, task_name="digest", detail=detail)


@router.post("/retry-failed-jobs", response_model=JobResponse)
def retry_failed_jobs(db: Session = DB_SESSION) -> JobResponse:
    OperationService(db).enqueue_failed_retries()
    return JobResponse(
        queued=True,
        task_name="retry_failed_jobs",
        detail="Errored ingests queued for retry.",
    )


@router.post("/clear-content", response_model=JobResponse)
def clear_content(db: Session = DB_SESSION) -> JobResponse:
    result = OperationService(db).clear_content_records()
    return JobResponse(
        queued=False,
        task_name="clear_content",
        detail=result["detail"],
        operation_run_id=result["operation_run_id"],
    )


@router.get("/ingestion-runs", response_model=list[IngestionRunHistoryRead])
def list_ingestion_runs(
    db: Session = DB_SESSION,
) -> list[IngestionRunHistoryRead]:
    return IngestionService(db).list_recent_ingestion_cycles()
