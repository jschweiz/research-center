from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import FileResponse

from app.api.deps import get_current_user
from app.schemas.briefs import AudioBriefRead, BriefAvailabilityRead, DigestRead
from app.services.brief_dates import iso_week_start
from app.services.briefs import BriefService

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("/today", response_model=DigestRead)
def get_today_brief() -> DigestRead:
    digest = BriefService().get_or_generate_today()
    if not digest:
        raise HTTPException(status_code=404, detail="Digest not available.")
    return digest


@router.get("/availability", response_model=BriefAvailabilityRead)
def get_brief_availability() -> BriefAvailabilityRead:
    return BriefService().list_availability()


@router.get("/weeks/{week_start}", response_model=DigestRead)
def get_weekly_brief(
    week_start: date = Path(..., description="ISO week start date in YYYY-MM-DD format"),
) -> DigestRead:
    if iso_week_start(week_start) != week_start:
        raise HTTPException(status_code=422, detail="Week start must be an ISO week Monday.")
    digest = BriefService().get_weekly_digest(week_start)
    if not digest:
        raise HTTPException(status_code=404, detail="Weekly digest not found.")
    return digest


@router.get("/{brief_date}", response_model=DigestRead)
def get_brief(
    brief_date: date = Path(..., description="Date in YYYY-MM-DD format"),
) -> DigestRead:
    digest = BriefService().get_or_generate_by_date(brief_date)
    if not digest:
        raise HTTPException(status_code=404, detail="Digest not found.")
    return digest


@router.post("/{brief_date}/generate-audio-summary", response_model=AudioBriefRead)
def generate_audio_summary(
    brief_date: date = Path(..., description="Date in YYYY-MM-DD format"),
) -> AudioBriefRead:
    audio_brief = BriefService().generate_audio_brief(brief_date)
    if not audio_brief:
        raise HTTPException(status_code=404, detail="Digest not found.")
    return audio_brief


@router.get("/{brief_date}/audio")
def get_audio_summary(
    brief_date: date = Path(..., description="Date in YYYY-MM-DD format"),
) -> FileResponse:
    service = BriefService()
    try:
        audio_path = service.get_audio_artifact_path(brief_date)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not audio_path or not audio_path.exists():
        raise HTTPException(status_code=404, detail="Audio brief not available.")
    return FileResponse(
        audio_path,
        media_type=service.voice_client.media_type,
        filename=f"brief-{brief_date.isoformat()}.{service.voice_client.output_format}",
    )
