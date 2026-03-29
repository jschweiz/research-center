from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from alembic.config import Config
from sqlalchemy import create_engine, func, select

from alembic import command
from app.core.config import get_settings
from app.db.base import Base
from app.db.models import (
    ContentType,
    DataMode,
    Digest,
    DigestEntry,
    DigestSection,
    Item,
    RunStatus,
    Source,
    SourceType,
)
from app.db.session import get_session_factory, reset_engine_cache
from app.services.ingestion import IngestionService


def _alembic_config(db_url: str) -> Config:
    backend_root = Path(__file__).resolve().parents[2]
    config = Config(str(backend_root / "alembic.ini"))
    config.set_main_option("script_location", str(backend_root / "alembic"))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def test_digest_cache_reset_migration_purges_cached_rows_and_recreates_tables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "migration.db"
    db_url = f"sqlite+pysqlite:///{db_path}"

    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_URL", db_url)
    monkeypatch.setenv("AUTO_CREATE_SCHEMA", "false")
    monkeypatch.setenv("SEED_DEMO_DATA", "false")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD", "change-me")
    monkeypatch.setenv("SECRET_KEY", "test-secret")
    monkeypatch.setenv("ENCRYPTION_KEY", "test-encryption")

    get_settings.cache_clear()
    reset_engine_cache()

    config = _alembic_config(db_url)
    engine = create_engine(db_url)
    Base.metadata.create_all(bind=engine)
    engine.dispose()
    command.stamp(config, "20260327_0004")

    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Migration Feed",
            url="https://example.com/migration.xml",
            priority=75,
            active=True,
            tags=["migration"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        item = IngestionService(db).ingest_payload(
            source=source,
            title="Cached digest item",
            canonical_url="https://example.com/migration-item",
            authors=["Researcher"],
            published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
            cleaned_text="A cached item that should survive while the digest cache is purged.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
        )

        digest = Digest(
            brief_date=date(2026, 3, 28),
            data_mode=DataMode.LIVE,
            status=RunStatus.SUCCEEDED,
            title="Morning Brief • 2026-03-28",
            editorial_note="Cached before the rewrite.",
            suggested_follow_ups=[],
        )
        digest.entries = [
            DigestEntry(
                item=item,
                section=DigestSection.HEADLINES,
                rank=1,
                note="Cached entry before the reset.",
            )
        ]
        db.add(digest)
        db.commit()

        assert db.scalar(select(func.count()).select_from(Digest)) == 1
        assert db.scalar(select(func.count()).select_from(DigestEntry)) == 1

    command.upgrade(config, "head")
    get_settings.cache_clear()
    reset_engine_cache()

    with get_session_factory()() as db:
        assert db.scalar(select(func.count()).select_from(Digest)) == 0
        assert db.scalar(select(func.count()).select_from(DigestEntry)) == 0

        item = db.scalar(select(Item).where(Item.canonical_url == "https://example.com/migration-item"))
        assert item is not None

        regenerated = Digest(
            brief_date=date(2026, 3, 29),
            data_mode=DataMode.LIVE,
            status=RunStatus.SUCCEEDED,
            title="Morning Brief • 2026-03-29",
            editorial_note="Regenerated after the reset.",
            suggested_follow_ups=[],
        )
        regenerated.entries = [
            DigestEntry(
                item=item,
                section=DigestSection.HEADLINES,
                rank=1,
                note="Fresh entry after the reset.",
            )
        ]
        db.add(regenerated)
        db.commit()

        stored = db.scalar(select(Digest).where(Digest.brief_date == date(2026, 3, 29)))
        assert stored is not None
        assert len(stored.entries) == 1
        assert stored.entries[0].section == DigestSection.HEADLINES

    get_settings.cache_clear()
    reset_engine_cache()
