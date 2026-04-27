from __future__ import annotations

from datetime import UTC, date, datetime

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.api.deps import get_db_session, get_local_control_device
from app.core.outbound import UnsafeOutboundUrlError
from app.schemas.advanced_enrichment import (
    AdvancedCompileRequest,
    AnswerQueryRequest,
    FileOutputRequest,
    HealthCheckRequest,
)
from app.schemas.briefs import BriefAvailabilityRead, DigestRead
from app.schemas.common import PaginatedResponse
from app.schemas.items import (
    ActionRead,
    CapturedPageImportRequest,
    ItemDetailRead,
    ItemListEntry,
    ManualImportRequest,
    ZoteroSaveRequest,
)
from app.schemas.local_control import (
    LocalControlInsightsRead,
    LocalControlJobResponse,
    LocalControlOperationsRead,
    LocalControlStatusRead,
    PairRedeemRequest,
    PairRedeemResponse,
)
from app.schemas.ops import RegenerateBriefRequest
from app.schemas.profile import ProfileRead, ProfileUpdate
from app.schemas.sources import SourceInjectRequest, SourceRead
from app.services.brief_dates import iso_week_start
from app.services.items import ItemService
from app.services.local_control import LocalControlError, LocalControlService
from app.services.profile import ProfileService
from app.services.vault_briefs import VaultBriefService
from app.services.vault_git_sync import VaultGitSyncError
from app.services.vault_items import ItemSummaryImportError
from app.services.vault_operations import VaultOperationService
from app.services.vault_source_registry import VaultSourceRegistryService
from app.services.vault_sources import SourceFetchCancelledError

router = APIRouter(prefix="/local-control", tags=["local-control"])


@router.post("/pair/redeem", response_model=PairRedeemResponse)
def redeem_pairing_token(
    payload: PairRedeemRequest,
    request: Request,
) -> PairRedeemResponse:
    try:
        return LocalControlService().redeem_pairing_token(
            pairing_token=payload.pairing_token,
            device_label=payload.device_label,
            client_ip=request.client.host if request.client else None,
        )
    except LocalControlError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/status", response_model=LocalControlStatusRead)
def get_local_control_status(
    device=Depends(get_local_control_device),
) -> LocalControlStatusRead:
    return LocalControlService().build_status(device)


@router.get("/insights", response_model=LocalControlInsightsRead)
def get_local_control_insights(
    _device=Depends(get_local_control_device),
) -> LocalControlInsightsRead:
    return LocalControlService().build_insights()


@router.get("/operations", response_model=LocalControlOperationsRead)
def get_local_control_operations(
    _device=Depends(get_local_control_device),
) -> LocalControlOperationsRead:
    runs = LocalControlService().list_recent_operations()
    return LocalControlOperationsRead(runs=runs)


@router.get("/documents", response_model=PaginatedResponse[ItemListEntry])
def list_local_control_documents(
    q: str | None = None,
    status_filter: str | None = Query(default=None, alias="status"),
    content_type: str | None = None,
    source_id: str | None = None,
    date_from: date | None = Query(default=None, alias="from"),
    date_to: date | None = Query(default=None, alias="to"),
    hide_sub_documents: bool = False,
    sort: str = "importance",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=100),
    _device=Depends(get_local_control_device),
) -> PaginatedResponse[ItemListEntry]:
    items, total = ItemService().list_items_page(
        query=q,
        status_filter=status_filter,
        content_type=content_type,
        source_id=source_id,
        date_from=date_from,
        date_to=date_to,
        sort=sort,
        page=page,
        page_size=page_size,
        include_hidden_primary_newsletters=True,
        include_sub_documents=not hide_sub_documents,
    )
    return PaginatedResponse[ItemListEntry](items=items, total=total)


@router.post(
    "/documents/import-page",
    response_model=ItemDetailRead,
    status_code=status.HTTP_201_CREATED,
)
def import_local_control_page(
    payload: CapturedPageImportRequest,
    _device=Depends(get_local_control_device),
) -> ItemDetailRead:
    return ItemService().import_captured_page(payload)


@router.post(
    "/documents/import-url-with-summary",
    response_model=ItemDetailRead,
    status_code=status.HTTP_201_CREATED,
)
def import_local_control_document_with_summary(
    payload: ManualImportRequest,
    _device=Depends(get_local_control_device),
) -> ItemDetailRead:
    try:
        return ItemService().import_url_with_summary(str(payload.url))
    except UnsafeOutboundUrlError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ItemSummaryImportError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc


@router.get("/documents/{item_id}", response_model=ItemDetailRead)
def get_local_control_document(
    item_id: str,
    _device=Depends(get_local_control_device),
) -> ItemDetailRead:
    item = ItemService().get_item_detail_readonly(item_id)
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return item


@router.post("/documents/{item_id}/read", response_model=ActionRead)
def mark_local_control_document_read(
    item_id: str,
    _device=Depends(get_local_control_device),
) -> ActionRead:
    result = ItemService().mark_read(item_id)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return result


@router.post("/documents/{item_id}/star", response_model=ActionRead)
def star_local_control_document(
    item_id: str,
    _device=Depends(get_local_control_device),
) -> ActionRead:
    result = ItemService().toggle_star(item_id)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return result


@router.post("/documents/{item_id}/save-to-zotero", response_model=ActionRead)
def save_local_control_document_to_zotero(
    item_id: str,
    payload: ZoteroSaveRequest,
    _device=Depends(get_local_control_device),
) -> ActionRead:
    result = ItemService().save_to_zotero(
        item_id,
        tags=payload.tags,
        note_prefix=payload.note_prefix,
    )
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return result


@router.get("/sources", response_model=list[SourceRead])
def list_local_control_sources(
    _device=Depends(get_local_control_device),
) -> list[SourceRead]:
    return VaultSourceRegistryService().list_sources()


@router.get("/briefs/availability", response_model=BriefAvailabilityRead)
def get_local_control_brief_availability(
    _device=Depends(get_local_control_device),
) -> BriefAvailabilityRead:
    return VaultBriefService().list_availability()


@router.get("/briefs/today", response_model=DigestRead)
def get_local_control_today_brief(
    _device=Depends(get_local_control_device),
) -> DigestRead:
    digest = VaultBriefService().get_or_generate_today()
    if not digest:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Digest not available.")
    return digest


@router.get("/briefs/weeks/{week_start}", response_model=DigestRead)
def get_local_control_weekly_brief(
    week_start: date = Path(..., description="ISO week start date in YYYY-MM-DD format"),
    _device=Depends(get_local_control_device),
) -> DigestRead:
    if iso_week_start(week_start) != week_start:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Week start must be an ISO week Monday.")

    digest = VaultBriefService().get_weekly_digest(week_start)
    if not digest:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Weekly digest not found.")
    return digest


@router.get("/briefs/{brief_date}", response_model=DigestRead)
def get_local_control_brief(
    brief_date: date = Path(..., description="Date in YYYY-MM-DD format"),
    _device=Depends(get_local_control_device),
) -> DigestRead:
    digest = VaultBriefService().get_or_generate_by_date(brief_date)
    if not digest:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Digest not found.")
    return digest


@router.get("/briefs/{brief_date}/audio")
def get_local_control_brief_audio(
    brief_date: date = Path(..., description="Date in YYYY-MM-DD format"),
    _device=Depends(get_local_control_device),
) -> FileResponse:
    service = VaultBriefService()
    try:
        audio_path = service.get_audio_artifact_path(brief_date)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    if not audio_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Audio brief not available.")

    return FileResponse(
        audio_path,
        media_type=service.voice_client.media_type,
        filename=f"brief-{brief_date.isoformat()}.{service.voice_client.output_format}",
    )


@router.get("/profile", response_model=ProfileRead)
def get_local_control_profile(
    _device=Depends(get_local_control_device),
    db: Session = Depends(get_db_session),
) -> ProfileRead:
    return ProfileService(db).get_profile()


@router.patch("/profile", response_model=ProfileRead)
def update_local_control_profile(
    payload: ProfileUpdate,
    _device=Depends(get_local_control_device),
    db: Session = Depends(get_db_session),
) -> ProfileRead:
    return ProfileService(db).update_profile(payload)


@router.post("/jobs/ingest", response_model=LocalControlJobResponse)
def run_local_ingest(
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        operation_run_id = VaultOperationService().run_ingest_pipeline()
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="ingest",
        detail="Fetch, lightweight enrichment, and index rebuild completed.",
        operation_run_id=operation_run_id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/fetch-sources", response_model=LocalControlJobResponse)
def run_local_fetch_sources(
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        operation_run_id = VaultOperationService().fetch_sources()
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="fetch_sources",
        detail="Raw source fetch completed.",
        operation_run_id=operation_run_id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/sources/{source_id}/inject", response_model=LocalControlJobResponse)
def run_local_source_pipeline(
    source_id: str,
    payload: SourceInjectRequest | None = None,
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    registry = VaultSourceRegistryService()
    source = registry.get_source(source_id)
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found.")

    requested_max_items = payload.max_items if payload else None
    requested_alphaxiv_sort = payload.alphaxiv_sort if payload else None
    try:
        operation_run_id = VaultOperationService().run_source_pipeline(
            source_id=source_id,
            max_items=requested_max_items,
            alphaxiv_sort=requested_alphaxiv_sort,
        )
    except SourceFetchCancelledError as exc:
        return LocalControlJobResponse(
            queued=False,
            task_name="source_inject",
            detail=str(exc),
            operation_run_id=exc.run_id,
            published_edition=None,
            completed_at=datetime.now(UTC),
        )
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    effective_max_items = requested_max_items or source.max_items
    alphaxiv_sort_suffix = (
        f" using alphaXiv {requested_alphaxiv_sort} sort"
        if source.custom_pipeline_id == "alphaxiv-paper" and requested_alphaxiv_sort
        else ""
    )
    return LocalControlJobResponse(
        queued=False,
        task_name="source_inject",
        detail=(
            "Source fetch completed for "
            f"{source.name} with a cap of {effective_max_items} document"
            f"{'' if effective_max_items == 1 else 's'}{alphaxiv_sort_suffix}. "
            "Lightweight enrichment and index refresh remain manual."
        ),
        operation_run_id=operation_run_id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/sources/{source_id}/stop", response_model=LocalControlJobResponse)
def stop_local_source_pipeline(
    source_id: str,
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    registry = VaultSourceRegistryService()
    source = registry.get_source(source_id)
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found.")

    try:
        run = VaultOperationService().request_stop_for_source(source_id=source_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return LocalControlJobResponse(
        queued=False,
        task_name="stop_source_fetch",
        detail=(
            f"Stop requested for {source.name}. The current document may finish before the fetch exits."
        ),
        operation_run_id=run.id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/lightweight-enrich", response_model=LocalControlJobResponse)
def run_local_lightweight_enrich(
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        run = VaultOperationService().lightweight_enrich()
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="lightweight_enrich",
        detail=run.summary,
        operation_run_id=run.id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/lightweight-metadata", response_model=LocalControlJobResponse)
def run_local_lightweight_metadata(
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        run = VaultOperationService().lightweight_metadata_enrich()
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="lightweight_metadata",
        detail=run.summary,
        operation_run_id=run.id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/lightweight-scoring", response_model=LocalControlJobResponse)
def run_local_lightweight_scoring(
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        run = VaultOperationService().lightweight_scoring_enrich()
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="lightweight_scoring",
        detail=run.summary,
        operation_run_id=run.id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/lightweight-enrich/stop", response_model=LocalControlJobResponse)
def stop_local_lightweight_enrich(
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        run = VaultOperationService().request_stop_for_lightweight()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return LocalControlJobResponse(
        queued=False,
        task_name="stop_lightweight_enrich",
        detail=(
            "Stop requested for lightweight enrichment. The current Ollama request may finish before the pass exits."
        ),
        operation_run_id=run.id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/rebuild-items-index", response_model=LocalControlJobResponse)
def run_local_rebuild_items_index(
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        operation_run_id = VaultOperationService().rebuild_index()
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="rebuild_items_index",
        detail="Local DB index rebuild completed.",
        operation_run_id=operation_run_id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/compile-wiki", response_model=LocalControlJobResponse)
def run_local_compile_wiki(
    payload: AdvancedCompileRequest | None = None,
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        run = VaultOperationService().run_advanced_compile(
            source_id=payload.source_id if payload else None,
            doc_id=payload.doc_id if payload else None,
            limit=payload.limit if payload else None,
        )
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="compile_wiki",
        detail=run.summary,
        operation_run_id=run.id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/advanced-compile", response_model=LocalControlJobResponse)
def run_local_advanced_compile(
    payload: AdvancedCompileRequest | None = None,
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        run = VaultOperationService().run_advanced_compile(
            source_id=payload.source_id if payload else None,
            doc_id=payload.doc_id if payload else None,
            limit=payload.limit if payload else None,
        )
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="advanced_compile",
        detail=run.summary,
        operation_run_id=run.id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/health-check", response_model=LocalControlJobResponse)
def run_local_health_check(
    payload: HealthCheckRequest | None = None,
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        run = VaultOperationService().run_health_check(
            scope=payload.scope if payload else "vault",
            topic=payload.topic if payload else None,
        )
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="health_check",
        detail=run.summary,
        operation_run_id=run.id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/answer-query", response_model=LocalControlJobResponse)
def run_local_answer_query(
    payload: AnswerQueryRequest,
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        run = VaultOperationService().run_answer_query(
            question=payload.question,
            output_kind=payload.output_kind,
        )
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="answer_query",
        detail=run.summary,
        operation_run_id=run.id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/file-output", response_model=LocalControlJobResponse)
def run_local_file_output(
    payload: FileOutputRequest,
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        run = VaultOperationService().run_file_output(path=payload.path)
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="file_output",
        detail=run.summary,
        operation_run_id=run.id,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/regenerate-brief", response_model=LocalControlJobResponse)
def regenerate_local_brief(
    payload: RegenerateBriefRequest | None = None,
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    service = VaultOperationService()
    target_date = (
        payload.brief_date
        if payload and payload.brief_date
        else VaultBriefService().current_edition_date()
    )
    try:
        published = service.regenerate_brief(brief_date=target_date)
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="digest",
        detail=f"Brief regenerated for {target_date.isoformat()} and the viewer artifacts were refreshed.",
        operation_run_id=None,
        published_edition=published,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/generate-audio", response_model=LocalControlJobResponse)
def generate_local_audio(
    payload: RegenerateBriefRequest | None = None,
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    target_date = (
        payload.brief_date
        if payload and payload.brief_date
        else VaultBriefService().current_edition_date()
    )
    try:
        audio_brief, published = VaultOperationService().generate_audio(brief_date=target_date)
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    if not audio_brief or audio_brief.status != "succeeded":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Audio generation did not complete successfully.",
        )
    return LocalControlJobResponse(
        queued=False,
        task_name="audio_generation",
        detail=f"Audio brief generated for {target_date.isoformat()} and synced into the viewer bundle.",
        operation_run_id=None,
        published_edition=published,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/publish", response_model=LocalControlJobResponse)
def publish_local_snapshot(
    payload: RegenerateBriefRequest | None = None,
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    target_date = (
        payload.brief_date
        if payload and payload.brief_date
        else VaultBriefService().current_edition_date()
    )
    try:
        published = VaultOperationService().publish(brief_date=target_date)
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="publish",
        detail=f"Viewer artifacts refreshed for {target_date.isoformat()}.",
        operation_run_id=None,
        published_edition=published,
        completed_at=datetime.now(UTC),
    )


@router.post("/jobs/sync-vault", response_model=LocalControlJobResponse)
def sync_local_vault(
    _device=Depends(get_local_control_device),
) -> LocalControlJobResponse:
    try:
        VaultOperationService().synchronize_local_control()
    except VaultGitSyncError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return LocalControlJobResponse(
        queued=False,
        task_name="sync_vault",
        detail="Raw sources and local-control outputs were synchronized with GitHub. Codex-managed wiki changes were left untouched.",
        operation_run_id=None,
        published_edition=None,
        completed_at=datetime.now(UTC),
    )
