from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db_session
from app.schemas.sources import SourceCreate, SourceProbeRead, SourceRead, SourceUpdate
from app.services.ingestion import IngestionService, SourceProbeError
from app.services.sources import SourceService

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("", response_model=list[SourceRead])
def list_sources(
    include_manual: bool = Query(default=False),
    db: Session = Depends(get_db_session),
) -> list[SourceRead]:
    return SourceService(db).list_sources(include_manual=include_manual)


@router.post("", response_model=SourceRead, status_code=status.HTTP_201_CREATED)
def create_source(payload: SourceCreate, db: Session = Depends(get_db_session)) -> SourceRead:
    return SourceService(db).create_source(payload)


@router.post("/{source_id}/probe", response_model=SourceProbeRead)
def probe_source(source_id: str, db: Session = Depends(get_db_session)) -> SourceProbeRead:
    source = SourceService(db).get_source(source_id)
    if not source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found.")
    try:
        return IngestionService(db).probe_source(source)
    except SourceProbeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.patch("/{source_id}", response_model=SourceRead)
def update_source(
    source_id: str,
    payload: SourceUpdate,
    db: Session = Depends(get_db_session),
) -> SourceRead:
    source = SourceService(db).update_source(source_id, payload)
    if not source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found.")
    return source


@router.delete("/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_source(source_id: str, db: Session = Depends(get_db_session)) -> Response:
    deleted = SourceService(db).delete_source(source_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found.")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
