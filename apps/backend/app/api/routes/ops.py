from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_current_user
from app.schemas.advanced_enrichment import (
    AdvancedCompileRequest,
    AnswerQueryRequest,
    FileOutputRequest,
    HealthCheckRequest,
)
from app.schemas.local_control import CodexStatusRead
from app.schemas.ops import (
    IngestionRunHistoryRead,
    JobResponse,
    PipelineStatusRead,
    RegenerateBriefRequest,
)
from app.schemas.sources import SourceInjectRequest, SourceLatestLogRead
from app.services.vault_advanced_enrichment import VaultAdvancedEnrichmentService
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_operations import VaultOperationService
from app.services.vault_source_registry import VaultSourceRegistryService
from app.services.vault_sources import SourceFetchCancelledError

CURRENT_USER = Depends(get_current_user)

router = APIRouter(dependencies=[CURRENT_USER])


def _run_ingest_job() -> JobResponse:
    service = VaultOperationService()
    operation_run_id = service.run_ingest_pipeline()
    return JobResponse(
        queued=False,
        task_name="ingest",
        detail="Fetch, lightweight enrichment, and index rebuild completed.",
        operation_run_id=operation_run_id,
    )


@router.post("/run-ingest", response_model=JobResponse)
def run_ingest() -> JobResponse:
    return _run_ingest_job()


@router.post("/ingest-now", response_model=JobResponse)
def ingest_now() -> JobResponse:
    return _run_ingest_job()


@router.post("/regenerate-brief", response_model=JobResponse)
def regenerate_brief(
    payload: RegenerateBriefRequest | None = None,
) -> JobResponse:
    service = VaultOperationService()
    service.regenerate_brief(
        brief_date=payload.brief_date if payload else None,
    )
    detail = (
        f"Digest regeneration completed for {payload.brief_date.isoformat()}."
        if payload and payload.brief_date
        else "Digest regeneration completed."
    )
    return JobResponse(
        queued=False,
        task_name="digest",
        detail=detail,
        operation_run_id=service.latest_run_id("brief_generation"),
    )


def _fetch_sources_job() -> JobResponse:
    service = VaultOperationService()
    operation_run_id = service.fetch_sources()
    return JobResponse(
        queued=False,
        task_name="fetch_sources",
        detail="Raw source fetch completed.",
        operation_run_id=operation_run_id,
    )


@router.post("/fetch-sources", response_model=JobResponse)
def fetch_sources() -> JobResponse:
    return _fetch_sources_job()


@router.post("/sync-sources", response_model=JobResponse)
def sync_sources() -> JobResponse:
    return _fetch_sources_job()


@router.post("/lightweight-enrich", response_model=JobResponse)
def lightweight_enrich() -> JobResponse:
    service = VaultOperationService()
    run = service.lightweight_enrich()
    return JobResponse(
        queued=False,
        task_name="lightweight_enrich",
        detail=run.summary,
        operation_run_id=run.id,
    )


@router.get("/pipeline-status", response_model=PipelineStatusRead)
def pipeline_status() -> PipelineStatusRead:
    return VaultOperationService().pipeline_status()


def _rebuild_items_index_job() -> JobResponse:
    service = VaultOperationService()
    operation_run_id = service.rebuild_index()
    return JobResponse(
        queued=False,
        task_name="rebuild_items_index",
        detail="Local DB index rebuild completed.",
        operation_run_id=operation_run_id,
    )


@router.post("/rebuild-items-index", response_model=JobResponse)
def rebuild_items_index() -> JobResponse:
    return _rebuild_items_index_job()


@router.post("/rebuild-index", response_model=JobResponse)
def rebuild_index() -> JobResponse:
    return _rebuild_items_index_job()


@router.post("/compile-wiki", response_model=JobResponse)
def compile_wiki(payload: AdvancedCompileRequest | None = None) -> JobResponse:
    service = VaultOperationService()
    run = service.run_advanced_compile(
        source_id=payload.source_id if payload else None,
        doc_id=payload.doc_id if payload else None,
        limit=payload.limit if payload else None,
    )
    return JobResponse(
        queued=False,
        task_name="compile_wiki",
        detail=run.summary,
        operation_run_id=run.id,
    )


@router.post("/generate-audio", response_model=JobResponse)
def generate_audio(
    payload: RegenerateBriefRequest | None = None,
) -> JobResponse:
    service = VaultOperationService()
    operation_run_id = service.generate_audio_only(
        brief_date=payload.brief_date if payload else None,
    )
    detail = (
        f"Audio generation completed for {payload.brief_date.isoformat()}."
        if payload and payload.brief_date
        else "Audio generation completed."
    )
    return JobResponse(
        queued=False,
        task_name="generate_audio",
        detail=detail,
        operation_run_id=operation_run_id,
    )


@router.post("/publish-latest", response_model=JobResponse)
def publish_latest(
    payload: RegenerateBriefRequest | None = None,
) -> JobResponse:
    service = VaultOperationService()
    operation_run_id = service.publish_latest(
        brief_date=payload.brief_date if payload else None,
    )
    detail = (
        f"Viewer publish completed for {payload.brief_date.isoformat()}."
        if payload and payload.brief_date
        else "Viewer publish completed."
    )
    return JobResponse(
        queued=False,
        task_name="publish_latest",
        detail=detail,
        operation_run_id=operation_run_id,
    )


@router.post("/advanced-compile", response_model=JobResponse)
def advanced_compile(payload: AdvancedCompileRequest | None = None) -> JobResponse:
    service = VaultOperationService()
    run = service.run_advanced_compile(
        source_id=payload.source_id if payload else None,
        doc_id=payload.doc_id if payload else None,
        limit=payload.limit if payload else None,
    )
    return JobResponse(
        queued=False,
        task_name="advanced_compile",
        detail=run.summary,
        operation_run_id=run.id,
    )


@router.post("/health-check", response_model=JobResponse)
def health_check(payload: HealthCheckRequest | None = None) -> JobResponse:
    service = VaultOperationService()
    run = service.run_health_check(
        scope=payload.scope if payload else "vault",
        topic=payload.topic if payload else None,
    )
    return JobResponse(
        queued=False,
        task_name="health_check",
        detail=run.summary,
        operation_run_id=run.id,
    )


@router.post("/answer-query", response_model=JobResponse)
def answer_query(payload: AnswerQueryRequest) -> JobResponse:
    service = VaultOperationService()
    run = service.run_answer_query(question=payload.question, output_kind=payload.output_kind)
    return JobResponse(
        queued=False,
        task_name="answer_query",
        detail=run.summary,
        operation_run_id=run.id,
    )


@router.get("/advanced-runtime", response_model=CodexStatusRead)
def advanced_runtime() -> CodexStatusRead:
    return CodexStatusRead.model_validate(VaultAdvancedEnrichmentService().codex_status())


@router.post("/file-output", response_model=JobResponse)
def file_output(payload: FileOutputRequest) -> JobResponse:
    service = VaultOperationService()
    run = service.run_file_output(path=payload.path)
    return JobResponse(
        queued=False,
        task_name="file_output",
        detail=run.summary,
        operation_run_id=run.id,
    )


@router.post("/deep-enrichment", response_model=JobResponse)
def deep_enrichment() -> JobResponse:
    run = VaultOperationService().run_deep_enrichment_placeholder()
    return JobResponse(
        queued=False,
        task_name="deep_enrichment",
        detail=run.summary,
        operation_run_id=run.id,
    )


@router.post("/sources/{source_id}/run", response_model=JobResponse)
def run_source_pipeline(
    source_id: str,
    payload: SourceInjectRequest | None = None,
) -> JobResponse:
    registry = VaultSourceRegistryService()
    source = registry.get_source(source_id)
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found.")
    requested_max_items = payload.max_items if payload else None
    try:
        operation_run_id = VaultOperationService().run_source_pipeline(
            source_id=source_id,
            max_items=requested_max_items,
        )
    except SourceFetchCancelledError as exc:
        return JobResponse(
            queued=False,
            task_name="source_inject",
            detail=str(exc),
            operation_run_id=exc.run_id,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    effective_max_items = requested_max_items or source.max_items
    return JobResponse(
        queued=False,
        task_name="source_inject",
        detail=(
            "Source fetch, lightweight enrichment, and index rebuild completed for "
            f"{source.name} with a cap of {effective_max_items} document"
            f"{'' if effective_max_items == 1 else 's'}."
        ),
        operation_run_id=operation_run_id,
    )


@router.get("/sources/{source_id}/latest-run", response_model=SourceLatestLogRead)
def latest_source_run(source_id: str) -> SourceLatestLogRead:
    registry = VaultSourceRegistryService()
    if registry.get_source(source_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found.")
    run = registry.latest_log(source_id)
    if run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No extraction log exists for this source yet.",
        )
    return SourceLatestLogRead(run=run)


@router.get("/ingestion-runs", response_model=list[IngestionRunHistoryRead])
def list_ingestion_runs() -> list[IngestionRunHistoryRead]:
    return VaultIngestionService().list_recent_runs()
