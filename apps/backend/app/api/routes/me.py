from fastapi import APIRouter, Depends

from app.api.deps import get_current_user
from app.schemas.auth import MeResponse

router = APIRouter()


@router.get("/me", response_model=MeResponse, dependencies=[Depends(get_current_user)])
def get_me(user: dict = Depends(get_current_user)) -> MeResponse:
    return MeResponse(email=user["email"], authenticated=True)
