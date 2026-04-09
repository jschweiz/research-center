from collections.abc import Generator
from functools import lru_cache
from typing import Any

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings

SQLITE_BUSY_TIMEOUT_MS = 30_000
PROFILE_SETTINGS_SQLITE_ADDITIVE_COLUMNS = {
    "data_mode": "ALTER TABLE profile_settings ADD COLUMN data_mode VARCHAR(4) NOT NULL DEFAULT 'SEED'",
    "summary_depth": "ALTER TABLE profile_settings ADD COLUMN summary_depth VARCHAR(50) NOT NULL DEFAULT 'balanced'",
    "ranking_thresholds": "ALTER TABLE profile_settings ADD COLUMN ranking_thresholds JSON NOT NULL DEFAULT '{}'",
    "brief_sections": "ALTER TABLE profile_settings ADD COLUMN brief_sections JSON NOT NULL DEFAULT '{}'",
    "audio_brief_settings": "ALTER TABLE profile_settings ADD COLUMN audio_brief_settings JSON NOT NULL DEFAULT '{}'",
    "prompt_guidance": "ALTER TABLE profile_settings ADD COLUMN prompt_guidance JSON NOT NULL DEFAULT '{}'",
    "alphaxiv_search_settings": "ALTER TABLE profile_settings ADD COLUMN alphaxiv_search_settings JSON NOT NULL DEFAULT '{}'",
}


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


def _ensure_sqlite_profile_settings_columns(engine: Engine) -> None:
    inspector = inspect(engine)
    if "profile_settings" not in set(inspector.get_table_names()):
        return

    existing_columns = {column["name"] for column in inspector.get_columns("profile_settings")}
    statements = [
        sql
        for name, sql in PROFILE_SETTINGS_SQLITE_ADDITIVE_COLUMNS.items()
        if name not in existing_columns
    ]
    if not statements:
        return

    with engine.begin() as connection:
        # `create_all()` creates missing tables but does not add newly introduced columns.
        # Repair the small legacy profile table in place so older local DBs remain readable.
        for statement in statements:
            connection.execute(text(statement))


def _sqlite_has_unique_single_column_index(
    engine: Engine,
    *,
    table_name: str,
    column_name: str,
) -> bool:
    with engine.connect() as connection:
        for row in connection.execute(text(f"PRAGMA index_list('{table_name}')")).fetchall():
            if not row[2]:
                continue
            index_name = row[1]
            columns = [
                index_row[2]
                for index_row in connection.execute(
                    text(f"PRAGMA index_info('{index_name}')")
                ).fetchall()
            ]
            if columns == [column_name]:
                return True
    return False


def _ensure_sqlite_vault_item_projection_schema(engine: Engine) -> None:
    from app.db.models import VaultItemProjection

    legacy_table_name = "vault_items__legacy_unique_canonical_url"
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "vault_items" not in table_names and legacy_table_name not in table_names:
        return

    if legacy_table_name in table_names:
        source_table_name = legacy_table_name
        drop_current_table = "vault_items" in table_names
    elif _sqlite_has_unique_single_column_index(
        engine,
        table_name="vault_items",
        column_name="canonical_url",
    ):
        source_table_name = "vault_items"
        drop_current_table = False
    else:
        return

    column_names = [column.name for column in VaultItemProjection.__table__.columns]
    quoted_columns = ", ".join(f'"{name}"' for name in column_names)

    with engine.begin() as connection:
        if source_table_name == "vault_items":
            connection.execute(text(f'ALTER TABLE "vault_items" RENAME TO "{legacy_table_name}"'))
            source_table_name = legacy_table_name

        if drop_current_table:
            connection.execute(text('DROP TABLE IF EXISTS "vault_items"'))

        for row in connection.execute(
            text(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'index' AND tbl_name = :table_name AND sql IS NOT NULL"
            ),
            {"table_name": source_table_name},
        ).fetchall():
            connection.execute(text(f'DROP INDEX "{row[0]}"'))

        VaultItemProjection.__table__.create(bind=connection)
        connection.execute(
            text(
                f'INSERT INTO "vault_items" ({quoted_columns}) '
                f'SELECT {quoted_columns} FROM "{source_table_name}"'
            )
        )
        connection.execute(text(f'DROP TABLE "{source_table_name}"'))


def _ensure_sqlite_vault_runtime_structures(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS vault_item_fts
                USING fts5(
                    item_id UNINDEXED,
                    title,
                    short_summary,
                    cleaned_text,
                    source_name,
                    tags
                )
                """
            )
        )


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


def ensure_schema() -> None:
    settings = get_settings()
    if not settings.auto_create_schema:
        return

    from app.db import models  # noqa: F401
    from app.db.base import Base

    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    if _is_sqlite_url(settings.database_url):
        _ensure_sqlite_profile_settings_columns(engine)
        _ensure_sqlite_vault_item_projection_schema(engine)
        _ensure_sqlite_vault_runtime_structures(engine)


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
