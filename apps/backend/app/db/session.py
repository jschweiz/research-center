from collections.abc import Generator
from functools import lru_cache
from typing import Any

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings

SQLITE_BUSY_TIMEOUT_MS = 30_000


def _is_sqlite_url(database_url: str) -> bool:
    return database_url.startswith("sqlite")


def _engine_kwargs(database_url: str) -> dict[str, Any]:
    kwargs = {"future": True, "pool_pre_ping": True}
    if _is_sqlite_url(database_url):
        kwargs["connect_args"] = {
            "check_same_thread": False,
            "timeout": SQLITE_BUSY_TIMEOUT_MS / 1000,
        }
    return kwargs


def _configure_sqlite_engine(engine: Engine) -> None:
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
        finally:
            cursor.close()


@lru_cache
def get_engine() -> Engine:
    settings = get_settings()
    engine = create_engine(settings.database_url, **_engine_kwargs(settings.database_url))
    if _is_sqlite_url(settings.database_url):
        _configure_sqlite_engine(engine)
    return engine


@lru_cache
def get_session_factory():
    return sessionmaker(bind=get_engine(), autocommit=False, autoflush=False, expire_on_commit=False)


def reset_engine_cache() -> None:
    if get_engine.cache_info().currsize:
        get_engine().dispose()
    get_session_factory.cache_clear()
    get_engine.cache_clear()


def get_db() -> Generator[Session, None, None]:
    db = get_session_factory()()
    try:
        yield db
    finally:
        db.close()
