from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from app.api.deps import get_current_user
from app.schemas.ops import JobResponse
from app.schemas.sources import (
    SourceCreate,
    SourceInjectRequest,
    SourceLatestLogRead,
    SourceProbeRead,
    SourceRead,
    SourceUpdate,
)
from app.services.vault_operations import VaultOperationService
from app.services.vault_source_registry import VaultSourceProbeError, VaultSourceRegistryService
from app.services.vault_sources import SourceFetchCancelledError

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("", response_model=list[SourceRead])
def list_sources(
    include_manual: bool = Query(default=False),
) -> list[SourceRead]:
    del include_manual
    return VaultSourceRegistryService().list_sources()


@router.post("", response_model=SourceRead, status_code=status.HTTP_201_CREATED)
def create_source(payload: SourceCreate) -> SourceRead:
    return VaultSourceRegistryService().create_source(payload)


@router.post("/{source_id}/probe", response_model=SourceProbeRead)
def probe_source(source_id: str) -> SourceProbeRead:
    try:
        return VaultSourceRegistryService().probe_source(source_id)
    except VaultSourceProbeError as exc:
        detail = str(exc)
        status_code = (
            status.HTTP_404_NOT_FOUND
            if "not found" in detail.lower()
            else status.HTTP_400_BAD_REQUEST
        )
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/{source_id}/inject", response_model=JobResponse)
def inject_source(
    source_id: str,
    payload: SourceInjectRequest | None = None,
) -> JobResponse:
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
        return JobResponse(
            queued=False,
            task_name="source_inject",
            detail=str(exc),
            operation_run_id=exc.run_id,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    effective_max_items = requested_max_items or source.max_items
    alphaxiv_sort_suffix = (
        f" using alphaXiv {requested_alphaxiv_sort} sort"
        if source.custom_pipeline_id == "alphaxiv-paper" and requested_alphaxiv_sort
        else ""
    )
    return JobResponse(
        queued=False,
        task_name="source_inject",
        detail=(
            "Source fetch completed for "
            f"{source.name} with a cap of {effective_max_items} document"
            f"{'' if effective_max_items == 1 else 's'}{alphaxiv_sort_suffix}. "
            "Lightweight enrichment and index refresh remain manual."
        ),
        operation_run_id=operation_run_id,
    )


@router.get("/{source_id}/latest-log", response_model=SourceLatestLogRead)
def latest_source_log(source_id: str) -> SourceLatestLogRead:
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


@router.patch("/{source_id}", response_model=SourceRead)
def update_source(
    source_id: str,
    payload: SourceUpdate,
) -> SourceRead:
    source = VaultSourceRegistryService().update_source(source_id, payload)
    if not source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found.")
    return source


@router.delete("/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_source(source_id: str) -> Response:
    deleted = VaultSourceRegistryService().delete_source(source_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found.")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
