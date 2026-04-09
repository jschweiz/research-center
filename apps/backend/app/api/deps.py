from datetime import UTC, datetime

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import get_session_manager
from app.db.session import get_db
from app.services.local_control import LocalControlError, LocalControlService
from app.vault.models import PairedDeviceState


def get_current_email(request: Request) -> str:
    settings = get_settings()
    token = request.cookies.get(settings.session_cookie_name)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")

    email = get_session_manager().load_token(token)
    if not email:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired.")
    return email


def get_current_user(email: str = Depends(get_current_email)) -> dict:
    return {"email": email}


DBSession = Session


def get_db_session(db: Session = Depends(get_db)) -> Session:
    return db


def _allow_loopback_local_control(*, request: Request) -> bool:
    settings = get_settings()
    client_host = request.client.host if request.client else None
    return not settings.is_production and client_host in {"127.0.0.1", "::1", "localhost"}


def get_local_control_device(
    request: Request,
):
    if _allow_loopback_local_control(request=request):
        now = datetime.now(UTC)
        client_host = request.client.host if request.client else None
        return PairedDeviceState(
            id="local-mac-loopback",
            label="Local Mac",
            token_hash="",
            last_used_at=now,
            last_seen_ip=client_host,
            revoked_at=None,
            metadata_json={"trusted_loopback": True},
            paired_at=now,
            updated_at=now,
        )

    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Local control token is required.",
        )
    try:
        return LocalControlService().authenticate_access_token(
            access_token=token.strip(),
            client_ip=request.client.host if request.client else None,
        )
    except LocalControlError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
