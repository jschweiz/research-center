from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db_session
from app.schemas.profile import ProfileRead, ProfileUpdate
from app.services.profile import ProfileService

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("", response_model=ProfileRead)
def get_profile(db: Session = Depends(get_db_session)) -> ProfileRead:
    return ProfileService(db).get_profile()


@router.patch("", response_model=ProfileRead)
def update_profile(payload: ProfileUpdate, db: Session = Depends(get_db_session)) -> ProfileRead:
    return ProfileService(db).update_profile(payload)
