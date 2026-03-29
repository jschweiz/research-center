import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.api.deps import get_current_user
from app.core.config import get_settings
from app.core.metrics import record_auth_event
from app.core.rate_limit import get_login_rate_limiter
from app.core.security import get_session_manager
from app.schemas.auth import LoginRequest, MeResponse

router = APIRouter()
logger = logging.getLogger(__name__)
CurrentUser = Annotated[dict, Depends(get_current_user)]


@router.post("/login", response_model=MeResponse)
def login(payload: LoginRequest, request: Request, response: Response) -> MeResponse:
    manager = get_session_manager()
    settings = get_settings()
    client_ip = request.client.host if request.client else None
    limiter = get_login_rate_limiter()
    blocked_until = limiter.blocked_until(email=str(payload.email), client_ip=client_ip)
    if blocked_until is not None:
        record_auth_event("login_rate_limited")
        logger.warning(
            "auth.login_rate_limited",
            extra={
                "client_ip": client_ip,
                "blocked_until": blocked_until.isoformat(),
            },
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts. Try again later.",
        )

    if not manager.verify_credentials(str(payload.email), payload.password):
        blocked_until = limiter.record_failure(email=str(payload.email), client_ip=client_ip)
        if blocked_until is not None:
            record_auth_event("login_rate_limited")
            logger.warning(
                "auth.login_rate_limited",
                extra={
                    "client_ip": client_ip,
                    "blocked_until": blocked_until.isoformat(),
                },
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many login attempts. Try again later.",
            )

        record_auth_event("login_failed")
        logger.warning("auth.login_failed", extra={"client_ip": client_ip})
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")

    limiter.record_success(email=str(payload.email), client_ip=client_ip)
    token = manager.issue_token(str(payload.email))
    response.set_cookie(
        key=settings.session_cookie_name,
        value=token,
        httponly=True,
        samesite="lax",
        secure=settings.is_production,
        max_age=60 * 60 * 24 * 30,
    )
    record_auth_event("login_succeeded")
    logger.info(
        "auth.login_succeeded",
        extra={
            "client_ip": client_ip,
        },
    )
    return MeResponse(email=payload.email, authenticated=True)


@router.post("/logout")
def logout(request: Request, response: Response) -> dict[str, str]:
    settings = get_settings()
    response.delete_cookie(settings.session_cookie_name)
    record_auth_event("logout")
    logger.info("auth.logout", extra={"client_ip": request.client.host if request.client else None})
    return {"status": "ok"}


@router.get("/me", response_model=MeResponse)
def me(user: CurrentUser) -> MeResponse:
    return MeResponse(email=user["email"], authenticated=True)
