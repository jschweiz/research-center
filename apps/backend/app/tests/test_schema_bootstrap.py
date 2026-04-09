from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from app.core.config import get_settings
from app.db.session import ensure_schema, reset_engine_cache


def _configure_schema_env(tmp_path: Path, monkeypatch) -> str:
    db_path = tmp_path / "schema-bootstrap.db"
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    monkeypatch.setenv("AUTO_CREATE_SCHEMA", "true")
    monkeypatch.setenv("SEED_DEMO_DATA", "false")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD", "change-me")
    monkeypatch.setenv("SECRET_KEY", "test-secret")
    monkeypatch.setenv("ENCRYPTION_KEY", "test-encryption")
    get_settings.cache_clear()
    reset_engine_cache()
    return f"sqlite+pysqlite:///{db_path}"


def test_profile_and_connection_routes_bootstrap_their_tables_on_fresh_start(
    authenticated_client: TestClient,
) -> None:
    profile = authenticated_client.get("/api/profile")
    assert profile.status_code == 200
    assert profile.json()["data_mode"] == "live"

    zotero = authenticated_client.get("/api/connections/zotero")
    assert zotero.status_code == 200
    assert zotero.json() is None


def test_auto_create_schema_repairs_vault_item_projection_duplicate_url_constraint(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_url = _configure_schema_env(tmp_path, monkeypatch)

    legacy_engine = create_engine(db_url)
    try:
        with legacy_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE vault_items (
                        item_id VARCHAR(255) NOT NULL,
                        raw_doc_path VARCHAR(2000) NOT NULL,
                        kind VARCHAR(80) NOT NULL,
                        title VARCHAR(500) NOT NULL,
                        source_id VARCHAR(120),
                        source_name VARCHAR(255) NOT NULL,
                        organization_name VARCHAR(255),
                        published_at DATETIME,
                        ingested_at DATETIME NOT NULL,
                        fetched_at DATETIME,
                        canonical_url VARCHAR(2000) NOT NULL,
                        content_type VARCHAR(10) NOT NULL,
                        extraction_confidence FLOAT NOT NULL,
                        cleaned_text TEXT,
                        short_summary TEXT,
                        tags_text TEXT NOT NULL,
                        status VARCHAR(32) NOT NULL,
                        doc_role VARCHAR(32) NOT NULL,
                        parent_id VARCHAR(255),
                        index_visibility VARCHAR(32) NOT NULL,
                        content_hash VARCHAR(64) NOT NULL,
                        identity_hash VARCHAR(64),
                        bucket VARCHAR(12) NOT NULL,
                        total_score FLOAT NOT NULL,
                        trend_score FLOAT NOT NULL,
                        novelty_score FLOAT NOT NULL,
                        lightweight_enrichment_status VARCHAR(32) NOT NULL,
                        lightweight_enriched_at DATETIME,
                        payload_json JSON NOT NULL,
                        updated_at DATETIME NOT NULL,
                        PRIMARY KEY (item_id),
                        UNIQUE (raw_doc_path),
                        UNIQUE (canonical_url)
                    )
                    """
                )
            )
            connection.execute(text("CREATE INDEX ix_vault_items_bucket ON vault_items (bucket)"))
            connection.execute(
                text(
                    """
                    INSERT INTO vault_items (
                        item_id,
                        raw_doc_path,
                        kind,
                        title,
                        source_name,
                        ingested_at,
                        canonical_url,
                        content_type,
                        extraction_confidence,
                        tags_text,
                        status,
                        doc_role,
                        index_visibility,
                        content_hash,
                        bucket,
                        total_score,
                        trend_score,
                        novelty_score,
                        lightweight_enrichment_status,
                        payload_json,
                        updated_at
                    ) VALUES (
                        :item_id,
                        :raw_doc_path,
                        :kind,
                        :title,
                        :source_name,
                        :ingested_at,
                        :canonical_url,
                        :content_type,
                        :extraction_confidence,
                        :tags_text,
                        :status,
                        :doc_role,
                        :index_visibility,
                        :content_hash,
                        :bucket,
                        :total_score,
                        :trend_score,
                        :novelty_score,
                        :lightweight_enrichment_status,
                        :payload_json,
                        :updated_at
                    )
                    """
                ),
                {
                    "item_id": "legacy-item-1",
                    "raw_doc_path": "raw/article/legacy-item-1/source.md",
                    "kind": "article",
                    "title": "Legacy duplicate URL item",
                    "source_name": "Legacy Feed",
                    "ingested_at": "2026-04-09T12:00:00Z",
                    "canonical_url": "https://example.com/duplicate-url",
                    "content_type": "ARTICLE",
                    "extraction_confidence": 0.0,
                    "tags_text": "",
                    "status": "active",
                    "doc_role": "primary",
                    "index_visibility": "visible",
                    "content_hash": "hash-1",
                    "bucket": "ARCHIVE",
                    "total_score": 0.1,
                    "trend_score": 0.0,
                    "novelty_score": 0.0,
                    "lightweight_enrichment_status": "pending",
                    "payload_json": "{}",
                    "updated_at": "2026-04-09T12:00:00Z",
                },
            )
    finally:
        legacy_engine.dispose()

    ensure_schema()

    repaired_engine = create_engine(db_url)
    try:
        with repaired_engine.begin() as connection:
            unique_indexes = []
            for row in connection.execute(text("PRAGMA index_list('vault_items')")).fetchall():
                if not row[2]:
                    continue
                columns = [
                    index_row[2]
                    for index_row in connection.execute(
                        text(f"PRAGMA index_info('{row[1]}')")
                    ).fetchall()
                ]
                unique_indexes.append(columns)

            assert ["canonical_url"] not in unique_indexes

            connection.execute(
                text(
                    """
                    INSERT INTO vault_items (
                        item_id,
                        raw_doc_path,
                        kind,
                        title,
                        source_name,
                        ingested_at,
                        canonical_url,
                        content_type,
                        extraction_confidence,
                        tags_text,
                        status,
                        doc_role,
                        index_visibility,
                        content_hash,
                        bucket,
                        total_score,
                        trend_score,
                        novelty_score,
                        lightweight_enrichment_status,
                        payload_json,
                        updated_at
                    ) VALUES (
                        :item_id,
                        :raw_doc_path,
                        :kind,
                        :title,
                        :source_name,
                        :ingested_at,
                        :canonical_url,
                        :content_type,
                        :extraction_confidence,
                        :tags_text,
                        :status,
                        :doc_role,
                        :index_visibility,
                        :content_hash,
                        :bucket,
                        :total_score,
                        :trend_score,
                        :novelty_score,
                        :lightweight_enrichment_status,
                        :payload_json,
                        :updated_at
                    )
                    """
                ),
                {
                    "item_id": "legacy-item-2",
                    "raw_doc_path": "raw/article/legacy-item-2/source.md",
                    "kind": "article",
                    "title": "Second duplicate URL item",
                    "source_name": "Legacy Feed",
                    "ingested_at": "2026-04-09T12:05:00Z",
                    "canonical_url": "https://example.com/duplicate-url",
                    "content_type": "ARTICLE",
                    "extraction_confidence": 0.0,
                    "tags_text": "",
                    "status": "active",
                    "doc_role": "primary",
                    "index_visibility": "visible",
                    "content_hash": "hash-2",
                    "bucket": "ARCHIVE",
                    "total_score": 0.2,
                    "trend_score": 0.0,
                    "novelty_score": 0.0,
                    "lightweight_enrichment_status": "pending",
                    "payload_json": "{}",
                    "updated_at": "2026-04-09T12:05:00Z",
                },
            )

            count = connection.execute(text("SELECT COUNT(*) FROM vault_items")).scalar_one()
            assert count == 2
    finally:
        repaired_engine.dispose()

    get_settings.cache_clear()
    reset_engine_cache()
