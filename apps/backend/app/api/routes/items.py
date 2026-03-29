from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db_session
from app.core.outbound import UnsafeOutboundUrlError
from app.schemas.items import (
    ActionRead,
    ItemDetailRead,
    ItemListEntry,
    ManualImportRequest,
    ZoteroSaveRequest,
)
from app.schemas.ops import JobResponse
from app.services.items import ItemService

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("", response_model=list[ItemListEntry])
def list_items(
    q: str | None = None,
    status_filter: str | None = Query(default=None, alias="status"),
    content_type: str | None = None,
    source_id: str | None = None,
    sort: str = "importance",
    db: Session = Depends(get_db_session),
) -> list[ItemListEntry]:
    return ItemService(db).list_items(
        query=q,
        status_filter=status_filter,
        content_type=content_type,
        source_id=source_id,
        sort=sort,
    )


@router.get("/{item_id}", response_model=ItemDetailRead)
def get_item(item_id: str, db: Session = Depends(get_db_session)) -> ItemDetailRead:
    item = ItemService(db).get_item_detail(item_id)
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return item


@router.post("/import-url", response_model=ItemDetailRead, status_code=status.HTTP_201_CREATED)
def import_url(payload: ManualImportRequest, db: Session = Depends(get_db_session)) -> ItemDetailRead:
    try:
        return ItemService(db).import_url(str(payload.url))
    except UnsafeOutboundUrlError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/{item_id}/archive", response_model=ActionRead)
def archive_item(item_id: str, db: Session = Depends(get_db_session)) -> ActionRead:
    action = ItemService(db).archive_item(item_id)
    if not action:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return action


@router.post("/{item_id}/star", response_model=ActionRead)
def star_item(item_id: str, db: Session = Depends(get_db_session)) -> ActionRead:
    action = ItemService(db).toggle_star(item_id)
    if not action:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return action


@router.post("/{item_id}/ignore-similar", response_model=ActionRead)
def ignore_similar(item_id: str, db: Session = Depends(get_db_session)) -> ActionRead:
    action = ItemService(db).ignore_similar(item_id)
    if not action:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return action


@router.post("/{item_id}/save-to-zotero", response_model=ActionRead)
def save_to_zotero(
    item_id: str, payload: ZoteroSaveRequest, db: Session = Depends(get_db_session)
) -> ActionRead:
    action = ItemService(db).save_to_zotero(item_id, payload.tags, payload.note_prefix)
    if not action:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return action


@router.post("/{item_id}/generate-deeper-summary", response_model=JobResponse)
def generate_deeper_summary(item_id: str, db: Session = Depends(get_db_session)) -> JobResponse:
    queued = ItemService(db).enqueue_deeper_summary(item_id)
    if not queued:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found.")
    return JobResponse(queued=True, task_name="generate_deeper_summary", detail="Summary job queued.")
