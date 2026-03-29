from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db_session
from app.core.config import get_settings
from app.core.security import get_session_manager
from app.db.models import ConnectionProvider, ConnectionStatus
from app.integrations.gmail_oauth import GmailOAuthClient
from app.schemas.connections import ConnectionCapabilitiesRead, ConnectionPayload, ConnectionRead
from app.services.connections import ConnectionService

router = APIRouter()


def _frontend_connections_redirect(*, gmail: str, reason: str | None = None) -> str:
    settings = get_settings()
    query = {"gmail": gmail}
    if reason:
        query["reason"] = reason
    return f"{settings.frontend_origin}/connections?{urlencode(query)}"


@router.get("/capabilities", response_model=ConnectionCapabilitiesRead)
def get_connection_capabilities(
    _: dict = Depends(get_current_user),
) -> ConnectionCapabilitiesRead:
    return ConnectionCapabilitiesRead(
        gmail_oauth_configured=GmailOAuthClient().is_configured(),
    )


@router.get("/gmail", response_model=ConnectionRead | None)
def get_gmail_connection(
    db: Session = Depends(get_db_session),
    _: dict = Depends(get_current_user),
) -> ConnectionRead | None:
    return ConnectionService(db).get_connection(ConnectionProvider.GMAIL)


@router.post("/gmail", response_model=ConnectionRead, status_code=status.HTTP_201_CREATED)
def upsert_gmail_connection(
    payload: ConnectionPayload,
    db: Session = Depends(get_db_session),
    _: dict = Depends(get_current_user),
) -> ConnectionRead:
    return ConnectionService(db).upsert_connection(ConnectionProvider.GMAIL, payload)


@router.get("/gmail/oauth/start")
def gmail_oauth_start(
    request: Request,
    user: dict = Depends(get_current_user),
) -> RedirectResponse:
    oauth = GmailOAuthClient()
    if not oauth.is_configured():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Gmail OAuth is not configured.")
    state = get_session_manager().issue_scoped_token(
        {"email": user["email"], "flow": "gmail_oauth"},
        salt="research-center-gmail-oauth",
    )
    authorization_url = oauth.build_authorization_url(
        redirect_uri=str(request.url_for("gmail_oauth_callback")),
        state=state,
    )
    return RedirectResponse(authorization_url, status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@router.get("/gmail/oauth/callback", name="gmail_oauth_callback")
def gmail_oauth_callback(
    request: Request,
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    db: Session = Depends(get_db_session),
) -> RedirectResponse:
    if error:
        return RedirectResponse(
            _frontend_connections_redirect(gmail="error", reason=error),
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )
    if not code or not state:
        return RedirectResponse(
            _frontend_connections_redirect(gmail="error", reason="missing_oauth_parameters"),
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )

    state_payload = get_session_manager().load_scoped_token(
        state,
        salt="research-center-gmail-oauth",
        max_age_seconds=60 * 10,
    )
    if not state_payload or state_payload.get("email") != get_settings().admin_email.lower():
        return RedirectResponse(
            _frontend_connections_redirect(gmail="error", reason="invalid_state"),
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )

    oauth = GmailOAuthClient()
    if not oauth.is_configured():
        return RedirectResponse(
            _frontend_connections_redirect(gmail="error", reason="oauth_not_configured"),
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )

    try:
        tokens = oauth.exchange_code(code=code, redirect_uri=str(request.url_for("gmail_oauth_callback")))
        profile = oauth.fetch_profile(tokens["access_token"])
    except Exception:
        return RedirectResponse(
            _frontend_connections_redirect(gmail="error", reason="token_exchange_failed"),
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )

    service = ConnectionService(db)
    existing = service.get_connection(ConnectionProvider.GMAIL)
    existing_metadata = existing.metadata_json if existing else {}
    service.store_connection(
        provider=ConnectionProvider.GMAIL,
        label=existing.label if existing else "Primary Gmail",
        payload=tokens,
        metadata_json={
            key: value
            for key, value in (
                existing_metadata
                | {
                    "connected_email": profile.get("emailAddress"),
                    "oauth_scope": tokens.get("scope"),
                    "auth_mode": "oauth",
                    "last_error": None,
                }
            ).items()
            if value is not None
        },
        status=ConnectionStatus.CONNECTED,
        last_synced_at=existing.last_synced_at if existing else None,
    )
    return RedirectResponse(
        _frontend_connections_redirect(gmail="connected"),
        status_code=status.HTTP_307_TEMPORARY_REDIRECT,
    )


@router.get("/zotero", response_model=ConnectionRead | None)
def get_zotero_connection(
    db: Session = Depends(get_db_session),
    _: dict = Depends(get_current_user),
) -> ConnectionRead | None:
    return ConnectionService(db).get_zotero_connection(refresh_if_needed=True)


@router.post("/zotero", response_model=ConnectionRead, status_code=status.HTTP_201_CREATED)
def upsert_zotero_connection(
    payload: ConnectionPayload,
    db: Session = Depends(get_db_session),
    _: dict = Depends(get_current_user),
) -> ConnectionRead:
    return ConnectionService(db).upsert_zotero_connection(payload)
