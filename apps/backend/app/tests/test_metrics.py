from __future__ import annotations

import importlib
from datetime import date
from pathlib import Path

from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.core.metrics import render_metrics, reset_metrics
from app.db.session import reset_engine_cache


def test_metrics_endpoint_exposes_http_and_auth_metrics(authenticated_client: TestClient) -> None:
    response = authenticated_client.get("/api/ops/ingestion-runs")
    assert response.status_code == 200

    metrics = authenticated_client.get("/metrics")
    assert metrics.status_code == 200
    assert metrics.headers["content-type"].startswith("text/plain; version=0.0.4")
    assert (
        'research_center_http_requests_total{method="GET",'
        'path="/api/ops/ingestion-runs",status_code="200"} 1'
        in metrics.text
    )
    assert (
        'research_center_auth_events_total{event="login_succeeded"} 1'
        in metrics.text
    )


def test_metrics_endpoint_requires_token_when_configured(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "metrics.db"
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    monkeypatch.setenv("AUTO_CREATE_SCHEMA", "true")
    monkeypatch.setenv("SEED_DEMO_DATA", "false")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD", "change-me")
    monkeypatch.setenv("SECRET_KEY", "test-secret")
    monkeypatch.setenv("ENCRYPTION_KEY", "test-encryption")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("FRONTEND_ORIGIN", "http://localhost:5173")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_ID", "gmail-client-id")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRET", "gmail-client-secret")
    monkeypatch.setenv("AUDIO_CACHE_DIR", str(tmp_path / "audio-cache"))
    monkeypatch.setenv("METRICS_ENABLED", "true")
    monkeypatch.setenv("METRICS_TOKEN", "metrics-secret")
    get_settings.cache_clear()
    reset_engine_cache()
    reset_metrics()

    app_main = importlib.import_module("app.main")
    app_main = importlib.reload(app_main)
    with TestClient(app_main.create_app()) as client:
        forbidden = client.get("/metrics")
        assert forbidden.status_code == 403

        authorized = client.get("/metrics", headers={"x-metrics-token": "metrics-secret"})
        assert authorized.status_code == 200

    get_settings.cache_clear()
    reset_engine_cache()
    reset_metrics()


def test_task_metrics_capture_success_and_skip(client: TestClient, monkeypatch) -> None:
    from app.tasks.jobs import purge_raw_email_payloads_task, run_digest_task

    monkeypatch.setattr(
        "app.tasks.jobs.IngestionService.purge_old_email_payloads",
        lambda self: 3,
    )
    monkeypatch.setattr(
        "app.tasks.jobs.ScheduleService.current_profile_date",
        lambda self: date(2026, 3, 28),
    )
    monkeypatch.setattr(
        "app.tasks.jobs.ScheduleService.is_profile_digest_due",
        lambda self: False,
    )

    assert purge_raw_email_payloads_task() == 3
    assert run_digest_task(only_if_due=True) == "skipped:not_due"

    metrics = render_metrics()
    assert (
        'research_center_task_runs_total{task="research_center.purge_raw_email_payloads",'
        'outcome="success"} 1'
        in metrics
    )
    assert (
        'research_center_task_runs_total{task="research_center.run_digest",outcome="skipped"} 1'
        in metrics
    )
