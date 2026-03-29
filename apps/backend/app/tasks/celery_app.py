import logging
from urllib.parse import urlsplit

import sentry_sdk
from celery import Celery
from sentry_sdk.integrations.celery import CeleryIntegration

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.core.metrics import start_metrics_http_server

settings = get_settings()
configure_logging(settings)
logger = logging.getLogger(__name__)

if settings.sentry_dsn:
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        integrations=[CeleryIntegration()],
        traces_sample_rate=0.2,
        environment=settings.app_env,
    )

celery_app = Celery(
    "research_center",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.tasks.jobs"],
)
celery_app.conf.update(
    task_track_started=True,
    timezone=settings.timezone,
    enable_utc=True,
    worker_hijack_root_logger=False,
)
if settings.metrics_enabled and settings.worker_metrics_port is not None:
    try:
        started = start_metrics_http_server(
            host=settings.worker_metrics_host,
            port=settings.worker_metrics_port,
            path=settings.metrics_path,
            token=settings.metrics_token,
        )
        logger.info(
            "worker.metrics_exporter",
            extra={
                "started": started,
                "host": settings.worker_metrics_host,
                "port": settings.worker_metrics_port,
                "path": settings.metrics_path,
                "token_protected": bool(settings.metrics_token),
            },
        )
    except OSError:
        logger.exception(
            "worker.metrics_exporter_failed",
            extra={
                "host": settings.worker_metrics_host,
                "port": settings.worker_metrics_port,
                "path": settings.metrics_path,
            },
        )
logger.info(
    "worker.configured",
    extra={
        "broker_scheme": urlsplit(settings.celery_broker_url).scheme or "unknown",
        "result_backend_scheme": urlsplit(settings.celery_result_backend).scheme or "unknown",
        "timezone": settings.timezone,
        "json_logging": settings.use_json_logging,
        "worker_metrics_port": settings.worker_metrics_port,
    },
)
