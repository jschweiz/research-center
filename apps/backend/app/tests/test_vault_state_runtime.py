from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from app.core.config import get_settings
from app.db.models import IngestionRunType, RunStatus, VaultRawDocument
from app.db.session import get_session_factory, reset_engine_cache
from app.schemas.ops import IngestionRunHistoryRead
from app.vault.models import (
    LocalBudgetDayState,
    LocalBudgetReservationState,
    LocalBudgetState,
    RawDocumentFrontmatter,
    StarredItemsState,
    VaultSourceDefinition,
    VaultSourcesConfig,
)
from app.vault.store import VaultStore


def _configure_runtime_env(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD", "change-me")
    monkeypatch.setenv("SECRET_KEY", "test-secret")
    monkeypatch.setenv("ENCRYPTION_KEY", "test-encryption")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("FRONTEND_ORIGIN", "http://localhost:5173")
    monkeypatch.setenv("VAULT_ROOT_DIR", str(tmp_path / "vault"))
    monkeypatch.setenv("LOCAL_STATE_DIR", str(tmp_path / "local-state"))
    monkeypatch.setenv("VAULT_SOURCE_PIPELINES_ENABLED", "false")
    monkeypatch.setenv("VAULT_GIT_ENABLED", "false")
    monkeypatch.setenv("SEED_DEMO_DATA", "false")
    monkeypatch.setenv("LOCAL_SERVER_BASE_URL", "http://localhost:8000")
    monkeypatch.setenv("AI_DAILY_COST_LIMIT_USD", "10.0")
    monkeypatch.setenv("AI_BUDGET_RESERVATION_TTL_MINUTES", "120")
    get_settings.cache_clear()
    reset_engine_cache()


def _legacy_run_payload(*, status: RunStatus) -> IngestionRunHistoryRead:
    timestamp = "2026-04-07T12:00:00Z"
    return IngestionRunHistoryRead.model_validate(
        {
            "id": "legacy-run-1",
            "run_type": IngestionRunType.INGEST,
            "status": status,
            "operation_kind": "raw_fetch",
            "trigger": "legacy_bootstrap",
            "title": "Legacy raw fetch",
            "summary": "Fetching configured sources into the raw vault.",
            "started_at": timestamp,
            "finished_at": None,
            "affected_edition_days": [],
            "total_titles": 0,
            "source_count": 1,
            "failed_source_count": 0,
            "created_count": 0,
            "updated_count": 0,
            "duplicate_mention_count": 0,
            "extractor_fallback_count": 0,
            "ai_prompt_tokens": 0,
            "ai_completion_tokens": 0,
            "ai_total_tokens": 0,
            "ai_cost_usd": 0.0,
            "tts_cost_usd": 0.0,
            "total_cost_usd": 0.0,
            "average_extraction_confidence": None,
            "basic_info": [{"label": "Source ID", "value": "legacy-source"}],
            "logs": [],
            "steps": [],
            "source_stats": [],
            "errors": [],
            "output_paths": [],
            "changed_file_count": 0,
        }
    )


def test_vault_runtime_bootstrap_imports_legacy_json_state(tmp_path: Path, monkeypatch) -> None:
    _configure_runtime_env(tmp_path, monkeypatch)
    settings = get_settings()

    legacy_sources = VaultSourcesConfig(
        sources=[
            VaultSourceDefinition(
                id="legacy-source",
                type="website",
                name="Legacy Source",
                enabled=True,
                raw_kind="article",
                config_json={"discovery_mode": "rss_feed"},
                max_items=5,
            )
        ]
    )
    legacy_run = _legacy_run_payload(status=RunStatus.RUNNING)
    legacy_stars = StarredItemsState(item_ids=["legacy-item"])
    legacy_budget = LocalBudgetState(
        days=[
            LocalBudgetDayState(
                budget_date=date(2026, 4, 8),
                spent_usd=1.25,
                reserved_usd=0.75,
                limit_usd=10.0,
                updated_at=datetime(2026, 4, 8, 9, 0, tzinfo=UTC),
            )
        ],
        reservations=[
            LocalBudgetReservationState(
                id="reservation-1",
                budget_date=date(2026, 4, 8),
                provider="ollama",
                operation="lightweight_enrichment",
                state="active",
                estimated_cost_usd=0.2,
                actual_cost_usd=None,
                metadata_json={"scope": "legacy"},
                created_at=datetime(2026, 4, 8, 9, 5, tzinfo=UTC),
                finalized_at=None,
            )
        ],
    )

    (settings.vault_root_dir / "system" / "config").mkdir(parents=True, exist_ok=True)
    (settings.vault_root_dir / "system" / "runs").mkdir(parents=True, exist_ok=True)
    (settings.local_state_dir / "preferences").mkdir(parents=True, exist_ok=True)
    (settings.local_state_dir / "budgets").mkdir(parents=True, exist_ok=True)

    (settings.vault_root_dir / "system" / "config" / "sources.json").write_text(
        legacy_sources.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (settings.vault_root_dir / "system" / "runs" / "run-log.jsonl").write_text(
        legacy_run.model_dump_json() + "\n",
        encoding="utf-8",
    )
    (settings.local_state_dir / "preferences" / "starred-items.json").write_text(
        legacy_stars.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (settings.local_state_dir / "budgets" / "ai-budget.json").write_text(
        legacy_budget.model_dump_json(indent=2),
        encoding="utf-8",
    )

    store = VaultStore()
    store.ensure_layout()

    imported_sources = store.load_sources_config()
    imported_runs = [IngestionRunHistoryRead.model_validate(payload) for payload in store.load_run_records()]

    assert [source.id for source in imported_sources.sources] == ["legacy-source"]
    assert imported_sources.sources[0].created_at is not None
    assert imported_sources.sources[0].updated_at is not None
    assert imported_runs[0].status == RunStatus.INTERRUPTED
    assert "interrupted" in imported_runs[0].summary.casefold()
    assert store.load_starred_items().item_ids == ["legacy-item"]
    assert store.load_ai_budget().days[0].limit_usd == 10.0

    get_settings.cache_clear()
    reset_engine_cache()


def test_raw_document_writes_sync_raw_document_projection(client) -> None:
    store = VaultStore()
    store.ensure_layout()

    frontmatter = RawDocumentFrontmatter(
        id="db-projection-doc",
        kind="article",
        title="DB Projection Test",
        source_url="https://example.com/db-projection",
        source_name="Projection Feed",
        authors=["Ada Lovelace"],
        published_at=datetime(2026, 4, 8, 8, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 4, 8, 9, 0, tzinfo=UTC),
        content_hash="content-hash-1",
        tags=["projection", "sqlite"],
        short_summary="Projection summary.",
        lightweight_enrichment_status="succeeded",
        lightweight_enriched_at=datetime(2026, 4, 8, 9, 30, tzinfo=UTC),
    )

    path = store.write_raw_document(
        kind="article",
        doc_id=frontmatter.id,
        frontmatter=frontmatter,
        body="A document body that should be mirrored into the raw document projection.",
    )

    with get_session_factory()() as db:
        row = db.get(VaultRawDocument, str(path.relative_to(store.root)))

    assert row is not None
    assert row.doc_id == "db-projection-doc"
    assert row.source_name == "Projection Feed"
    assert row.frontmatter_json["authors"] == ["Ada Lovelace"]
    assert row.frontmatter_json["short_summary"] == "Projection summary."
    assert row.payload_json["body"].startswith("A document body")
