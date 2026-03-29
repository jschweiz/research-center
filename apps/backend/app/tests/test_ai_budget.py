from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from app.core.config import get_settings
from app.db.models import (
    AIBudgetDay,
    AIBudgetReservation,
    IngestionRun,
    IngestionRunType,
    RunStatus,
)
from app.db.session import SQLITE_BUSY_TIMEOUT_MS, get_engine, get_session_factory
from app.services.ai_budget import AIBudgetExceededError, AIBudgetService


def test_ai_budget_seeds_from_existing_operation_history_and_blocks_over_limit(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setenv("AI_DAILY_COST_LIMIT_USD", "0.01")
    get_settings.cache_clear()

    with get_session_factory()() as db:
        db.add(
            IngestionRun(
                run_type=IngestionRunType.DIGEST,
                status=RunStatus.SUCCEEDED,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                metadata_json={
                    "operation_kind": "brief_generation",
                    "total_cost_usd": 0.009,
                },
            )
        )
        db.commit()

    service = AIBudgetService()
    with pytest.raises(AIBudgetExceededError):
        service.reserve_estimated_cost(
            provider="gemini",
            operation="summarize_item",
            estimated_cost_usd=0.002,
        )


def test_ai_budget_release_and_consume_update_daily_totals(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setenv("AI_DAILY_COST_LIMIT_USD", "0.01")
    get_settings.cache_clear()

    service = AIBudgetService()
    first = service.reserve_estimated_cost(
        provider="gemini",
        operation="summarize_item",
        estimated_cost_usd=0.006,
    )
    assert first is not None
    service.consume_reservation(first, actual_cost_usd=0.0045)

    second = service.reserve_estimated_cost(
        provider="google-cloud",
        operation="synthesize_audio",
        estimated_cost_usd=0.004,
    )
    assert second is not None
    service.release_reservation(second)

    with get_session_factory()() as db:
        day = db.get(AIBudgetDay, service.current_budget_date())
        assert day is not None
        assert day.spent_usd == 0.0045
        assert day.reserved_usd == 0.0

        reservations = list(
            db.scalars(
                select(AIBudgetReservation).order_by(AIBudgetReservation.created_at.asc())
            ).all()
        )
        assert len(reservations) == 2
        assert reservations[0].state == "consumed"
        assert reservations[0].actual_cost_usd == 0.0045
        assert reservations[1].state == "released"
        assert reservations[1].actual_cost_usd == 0.0


def test_sqlite_engine_enables_wal_and_busy_timeout(client: TestClient) -> None:
    engine = get_engine()

    with engine.connect() as connection:
        busy_timeout = connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one()
        foreign_keys = connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one()
        journal_mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar_one()

    assert busy_timeout == SQLITE_BUSY_TIMEOUT_MS
    assert foreign_keys == 1
    assert journal_mode == "wal"


def test_ai_budget_retries_transient_sqlite_lock(
    client: TestClient,
    monkeypatch,
) -> None:
    service = AIBudgetService()
    original = AIBudgetService._ensure_budget_day_row
    attempts = {"count": 0}

    def _flaky_ensure_budget_day_row(self, db, *, budget_date):
        if attempts["count"] == 0:
            attempts["count"] += 1
            raise OperationalError(
                "INSERT INTO ai_budget_days (...) VALUES (...)",
                {"budget_date": budget_date.isoformat()},
                sqlite3.OperationalError("database is locked"),
            )
        return original(self, db, budget_date=budget_date)

    monkeypatch.setattr(
        AIBudgetService,
        "_ensure_budget_day_row",
        _flaky_ensure_budget_day_row,
    )

    reservation = service.reserve_estimated_cost(
        provider="google-cloud",
        operation="synthesize_audio",
        estimated_cost_usd=0.001,
    )

    assert attempts["count"] == 1
    assert reservation is not None

    with get_session_factory()() as db:
        day = db.get(AIBudgetDay, service.current_budget_date())
        assert day is not None
        assert day.reserved_usd == 0.001
