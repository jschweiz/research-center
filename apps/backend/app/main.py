from __future__ import annotations

import logging
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from time import perf_counter
from urllib.parse import urlsplit

import sentry_sdk
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from sentry_sdk.integrations.fastapi import FastApiIntegration

from app.api.router import api_router
from app.core.config import Settings, get_settings
from app.core.logging import (
    REQUEST_ID_HEADER,
    bind_request_context,
    build_request_id,
    configure_logging,
    reset_request_context,
)
from app.core.metrics import (
    PROMETHEUS_CONTENT_TYPE,
    normalize_metrics_path,
    record_http_request_finished,
    record_http_request_started,
    render_metrics,
)
from app.db.session import ensure_schema
from app.vault.store import VaultStore

UNSAFE_HTTP_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
ORIGIN_PROTECTED_GET_PATH_SUFFIXES = {"/connections/gmail/oauth/start"}
logger = logging.getLogger(__name__)


def _origin_from_referer(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def _apply_api_response_headers(
    response: Response,
    *,
    settings: Settings,
    request_id: str,
) -> None:
    response.headers[REQUEST_ID_HEADER] = request_id
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    if settings.is_production:
        response.headers.setdefault(
            "Strict-Transport-Security",
            "max-age=31536000; includeSubDomains",
        )


def _request_log_level(status_code: int) -> int:
    if status_code >= 500:
        return logging.ERROR
    if status_code >= 400:
        return logging.WARNING
    return logging.INFO


def _extract_metrics_token(request: Request) -> str | None:
    authorization = request.headers.get("authorization")
    if authorization:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() == "bearer" and token.strip():
            return token.strip()
    metrics_token = request.headers.get("x-metrics-token")
    return metrics_token.strip() if metrics_token else None


def _metrics_access_allowed(request: Request, settings: Settings) -> bool:
    if settings.metrics_token:
        return secrets.compare_digest(
            _extract_metrics_token(request) or "",
            settings.metrics_token,
        )
    return not settings.is_production


def _is_origin_protected_get_path(path: str, *, api_prefix: str) -> bool:
    if any(path == f"{api_prefix}{suffix}" for suffix in ORIGIN_PROTECTED_GET_PATH_SUFFIXES):
        return True

    if path == f"{api_prefix}/connections/zotero":
        return True

    briefs_prefix = f"{api_prefix}/briefs/"
    if path.startswith(briefs_prefix):
        brief_path = path.removeprefix(briefs_prefix)
        return brief_path != "availability"

    items_prefix = f"{api_prefix}/items/"
    if path.startswith(items_prefix):
        item_path = path.removeprefix(items_prefix)
        return bool(item_path) and "/" not in item_path and item_path != "import-url"

    return False


def _is_local_control_api_path(path: str, *, api_prefix: str) -> bool:
    local_control_prefix = f"{api_prefix}/local-control"
    return path == local_control_prefix or path.startswith(f"{local_control_prefix}/")


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings)

    if settings.sentry_dsn:
        sentry_sdk.init(
            dsn=settings.sentry_dsn,
            integrations=[FastApiIntegration()],
            traces_sample_rate=0.2,
            environment=settings.app_env,
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if settings.metrics_enabled and settings.is_production and not settings.metrics_token:
            logger.warning(
                "metrics.api_endpoint_disabled",
                extra={"reason": "missing_metrics_token"},
            )
        logger.info(
            "app.startup.begin",
            extra={
                "timezone": settings.timezone,
                "json_logging": settings.use_json_logging,
                "metrics_enabled": settings.metrics_enabled,
                "metrics_path": settings.metrics_path,
                "vault_root_dir": str(settings.vault_root_dir),
            },
        )
        try:
            ensure_schema()
            logger.info("app.startup.schema_ready", extra={"auto_create_schema": settings.auto_create_schema})
            VaultStore().ensure_layout()
            logger.info("app.startup.vault_ready")
            logger.info("app.startup.complete")
            yield
        except Exception:
            logger.exception("app.startup.failed")
            raise
        finally:
            logger.info("app.shutdown.complete")

    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[settings.frontend_origin],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def protect_api_requests(request: Request, call_next):
        request_id = build_request_id(request.headers.get(REQUEST_ID_HEADER))
        request.state.request_id = request_id
        request_started_at = perf_counter()
        request_context_token = bind_request_context(request_id)
        api_request = request.url.path.startswith(settings.api_prefix)
        metrics_path = normalize_metrics_path(request.url.path)
        blocked_reason: str | None = None
        client_ip = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")
        if api_request and settings.metrics_enabled:
            record_http_request_started(request.method, metrics_path)
        try:
            if not api_request:
                return await call_next(request)

            if request.url.path != f"{settings.api_prefix}/health":
                logger.info(
                    "request.started",
                    extra={
                        "method": request.method,
                        "path": request.url.path,
                        "client_ip": client_ip,
                        "user_agent": user_agent[:160] if user_agent else None,
                    },
                )

            protected_get = request.method.upper() == "GET" and _is_origin_protected_get_path(
                request.url.path,
                api_prefix=settings.api_prefix,
            )
            if settings.is_production and not _is_local_control_api_path(
                request.url.path,
                api_prefix=settings.api_prefix,
            ) and (
                request.method.upper() in UNSAFE_HTTP_METHODS or protected_get
            ):
                allowed_origin = settings.frontend_origin.rstrip("/")
                request_origin = (
                    request.headers.get("origin")
                    or _origin_from_referer(request.headers.get("referer"))
                    or ""
                ).rstrip("/")
                if request_origin != allowed_origin:
                    blocked_reason = "origin_not_allowed"
                    response = JSONResponse(
                        status_code=403,
                        content={"detail": "Request origin is not allowed."},
                    )
                    _apply_api_response_headers(
                        response,
                        settings=settings,
                        request_id=request_id,
                    )
                    duration_ms = round((perf_counter() - request_started_at) * 1000, 2)
                    logger.warning(
                        "request.completed",
                        extra={
                            "method": request.method,
                            "path": request.url.path,
                            "status_code": response.status_code,
                            "duration_ms": duration_ms,
                            "client_ip": client_ip,
                            "blocked_reason": blocked_reason,
                        },
                    )
                    if settings.metrics_enabled:
                        record_http_request_finished(
                            method=request.method,
                            path=metrics_path,
                            status_code=response.status_code,
                            duration_seconds=duration_ms / 1000,
                        )
                    return response

            response = await call_next(request)
            _apply_api_response_headers(response, settings=settings, request_id=request_id)
            if settings.metrics_enabled:
                record_http_request_finished(
                    method=request.method,
                    path=metrics_path,
                    status_code=response.status_code,
                    duration_seconds=(perf_counter() - request_started_at),
                )
            if request.url.path != f"{settings.api_prefix}/health" or response.status_code >= 400:
                duration_ms = round((perf_counter() - request_started_at) * 1000, 2)
                logger.log(
                    _request_log_level(response.status_code),
                    "request.completed",
                    extra={
                        "method": request.method,
                        "path": request.url.path,
                        "status_code": response.status_code,
                        "duration_ms": duration_ms,
                        "client_ip": client_ip,
                    },
                )
            return response
        except Exception:
            if api_request:
                if settings.metrics_enabled:
                    record_http_request_finished(
                        method=request.method,
                        path=metrics_path,
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        duration_seconds=(perf_counter() - request_started_at),
                    )
                logger.exception(
                    "request.failed",
                    extra={
                        "method": request.method,
                        "path": request.url.path,
                        "duration_ms": round((perf_counter() - request_started_at) * 1000, 2),
                        "client_ip": client_ip,
                    },
                )
            raise
        finally:
            reset_request_context(request_context_token)

    if settings.metrics_enabled:

        @app.get(settings.metrics_path, include_in_schema=False)
        def metrics_endpoint(request: Request) -> PlainTextResponse:
            if not settings.metrics_token and settings.is_production:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
            if not _metrics_access_allowed(request, settings):
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
            response = PlainTextResponse(
                render_metrics(),
                media_type=PROMETHEUS_CONTENT_TYPE,
            )
            response.headers["Cache-Control"] = "no-store"
            return response

    app.include_router(api_router, prefix=settings.api_prefix)

    def local_app_index() -> Path | None:
        index_path = settings.web_dist_dir / "index.html"
        return index_path if index_path.exists() else None

    def local_app_config_payload() -> dict[str, object]:
        return {
            "mode": settings.local_web_mode,
            "apiBaseUrl": settings.api_prefix,
            "pairedLocalUrl": settings.local_server_base_url.rstrip("/"),
            "hostedViewerUrl": settings.hosted_viewer_url,
            "cloudKit": None,
        }

    if local_app_index() is not None:

        @app.get("/app-config.json", include_in_schema=False)
        def local_app_config() -> JSONResponse:
            return JSONResponse(local_app_config_payload())

        @app.get("/")
        def serve_root_app() -> FileResponse:
            index_path = local_app_index()
            assert index_path is not None
            return FileResponse(index_path)

        @app.get("/{full_path:path}", include_in_schema=False)
        def serve_local_app(full_path: str) -> FileResponse:
            if full_path.startswith("api/") or full_path == "api":
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
            if full_path == settings.metrics_path.lstrip("/"):
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
            asset_path = settings.web_dist_dir / full_path
            if asset_path.is_file():
                return FileResponse(asset_path)
            index_path = local_app_index()
            assert index_path is not None
            return FileResponse(index_path)

    else:

        @app.get("/")
        def root() -> dict[str, str]:
            return {
                "name": settings.app_name,
                "api": f"{settings.api_prefix}/health",
                "vault_root_dir": str(settings.vault_root_dir),
            }

    return app


app = create_app()
