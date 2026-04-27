from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.api.deps import get_current_user
from app.core.outbound import UnsafeOutboundUrlError
from app.schemas.items import (
    ActionRead,
    ItemDetailRead,
    ItemListEntry,
    ManualImportRequest,
)
from app.services.items import ItemService
from app.services.vault_items import ItemSummaryImportError

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("", response_model=list[ItemListEntry])
def list_items(
    q: str | None = None,
    status_filter: str | None = Query(default=None, alias="status"),
    content_type: str | None = None,
    source_id: str | None = None,
    sort: str = "importance",
) -> list[ItemListEntry]:
    return ItemService().list_items(
        query=q,
        status_filter=status_filter,
        content_type=content_type,
        source_id=source_id,
        sort=sort,
    )


@router.get("/{item_id}", response_model=ItemDetailRead)
def get_item(item_id: str) -> ItemDetailRead:
    item = ItemService().get_item_detail(item_id)
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return item


@router.post("/import-url", response_model=ItemDetailRead, status_code=status.HTTP_201_CREATED)
def import_url(payload: ManualImportRequest) -> ItemDetailRead:
    try:
        return ItemService().import_url(str(payload.url))
    except UnsafeOutboundUrlError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/import-url-with-summary", response_model=ItemDetailRead, status_code=status.HTTP_201_CREATED)
def import_url_with_summary(payload: ManualImportRequest) -> ItemDetailRead:
    try:
        return ItemService().import_url_with_summary(str(payload.url))
    except UnsafeOutboundUrlError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ItemSummaryImportError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc


@router.post("/{item_id}/archive", response_model=ActionRead)
def archive_item(item_id: str) -> ActionRead:
    result = ItemService().archive_item(item_id)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return result


@router.post("/{item_id}/star", response_model=ActionRead)
def star_item(item_id: str) -> ActionRead:
    result = ItemService().toggle_star(item_id)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return result
