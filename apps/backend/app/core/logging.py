from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from app.core.config import Settings

REQUEST_ID_HEADER = "X-Request-ID"
_REQUEST_ID = ContextVar("request_id", default=None)
_TASK_ID = ContextVar("task_id", default=None)
_TASK_NAME = ContextVar("task_name", default=None)
_RESERVED_LOG_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__) | {
    "message",
    "asctime",
}
_LOG_RECORD_FACTORY_INSTALLED = False


def current_request_id() -> str | None:
    return _REQUEST_ID.get()


def build_request_id(candidate: str | None = None) -> str:
    value = str(candidate or "").strip()
    if value and len(value) <= 128 and all(
        character.isalnum() or character in "-_."
        for character in value
    ):
        return value
    return uuid4().hex


def bind_request_context(request_id: str) -> Token[str | None]:
    return _REQUEST_ID.set(request_id)


def reset_request_context(token: Token[str | None]) -> None:
    _REQUEST_ID.reset(token)


def bind_task_context(
    *,
    task_id: str | None,
    task_name: str | None,
) -> tuple[Token[str | None], Token[str | None]]:
    return (_TASK_ID.set(task_id), _TASK_NAME.set(task_name))


def reset_task_context(tokens: tuple[Token[str | None], Token[str | None]]) -> None:
    task_id_token, task_name_token = tokens
    _TASK_NAME.reset(task_name_token)
    _TASK_ID.reset(task_id_token)


def _current_context_fields() -> dict[str, str]:
    fields: dict[str, str] = {}
    request_id = _REQUEST_ID.get()
    task_id = _TASK_ID.get()
    task_name = _TASK_NAME.get()
    if request_id:
        fields["request_id"] = request_id
    if task_id:
        fields["task_id"] = task_id
    if task_name:
        fields["task_name"] = task_name
    return fields


def _normalize_log_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        normalized = value if value.tzinfo else value.replace(tzinfo=UTC)
        return normalized.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {str(key): _normalize_log_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_normalize_log_value(item) for item in value]
    return str(value)


def _record_extra_fields(record: logging.LogRecord) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for key, value in record.__dict__.items():
        if key in _RESERVED_LOG_RECORD_FIELDS or key.startswith("_"):
            continue
        if value in (None, "", "-", ()):
            continue
        fields[key] = _normalize_log_value(value)
    return fields


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                tz=UTC,
            ).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(_record_extra_fields(record))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=True)


class TextLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
        parts = [timestamp, record.levelname, record.name, record.getMessage()]
        for key, value in sorted(_record_extra_fields(record).items()):
            parts.append(f"{key}={json.dumps(value, ensure_ascii=True, separators=(',', ':'))}")
        rendered = " ".join(parts)
        if record.exc_info:
            rendered = f"{rendered}\n{self.formatException(record.exc_info)}"
        return rendered


class ContextDefaultsFilter(logging.Filter):
    def __init__(self, *, app_env: str) -> None:
        super().__init__()
        self.app_env = app_env

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "app_env"):
            record.app_env = self.app_env
        return True


def _install_log_record_factory() -> None:
    global _LOG_RECORD_FACTORY_INSTALLED
    if _LOG_RECORD_FACTORY_INSTALLED:
        return

    base_factory = logging.getLogRecordFactory()

    def factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = base_factory(*args, **kwargs)
        for key, value in _current_context_fields().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return record

    logging.setLogRecordFactory(factory)
    _LOG_RECORD_FACTORY_INSTALLED = True


def _remove_existing_handler(root: logging.Logger) -> None:
    for handler in list(root.handlers):
        if getattr(handler, "_research_center_handler", False):
            root.removeHandler(handler)


def configure_logging(settings: Settings) -> None:
    _install_log_record_factory()

    level = getattr(logging, settings.log_level, logging.INFO)
    formatter: logging.Formatter = (
        JsonLogFormatter() if settings.use_json_logging else TextLogFormatter()
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    _remove_existing_handler(root_logger)

    handler = logging.StreamHandler(sys.stdout)
    handler._research_center_handler = True
    handler.setLevel(level)
    handler.addFilter(ContextDefaultsFilter(app_env=settings.app_env))
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    logging.captureWarnings(True)

    for logger_name in ("uvicorn", "uvicorn.error", "celery", "celery.app.trace"):
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(level)

    access_logger = logging.getLogger("uvicorn.access")
    access_logger.handlers.clear()
    access_logger.propagate = False
    access_logger.setLevel(level)
