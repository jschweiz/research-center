from __future__ import annotations

import importlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.core.metrics import reset_metrics
from app.core.rate_limit import reset_login_rate_limiter
from app.db.session import reset_engine_cache
from app.integrations.extractors import ExtractedContent


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
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
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_ID", "gmail-client-id")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRET", "gmail-client-secret")
    monkeypatch.setenv("AI_DAILY_COST_LIMIT_USD", "10.0")
    monkeypatch.setenv("AI_BUDGET_RESERVATION_TTL_MINUTES", "120")
    monkeypatch.setenv("AUDIO_CACHE_DIR", str(tmp_path / "audio-cache"))
    monkeypatch.delenv("GMAIL_INGEST_EMAIL", raising=False)
    monkeypatch.delenv("GMAIL_INGEST_APP_PASSWORD", raising=False)
    monkeypatch.delenv("GMAIL_INGEST_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("GOOGLE_TTS_VOICE_NAME", "en-US-Studio-O")
    monkeypatch.setenv("GOOGLE_TTS_SPEAKING_RATE", "1.0")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    monkeypatch.setenv("GOOGLE_CLOUD_TTS_CREDENTIALS_JSON", "")
    monkeypatch.setenv("METRICS_ENABLED", "true")
    monkeypatch.delenv("METRICS_TOKEN", raising=False)
    get_settings.cache_clear()
    reset_engine_cache()
    reset_metrics()
    reset_login_rate_limiter()

    app_main = importlib.import_module("app.main")
    app_main = importlib.reload(app_main)
    with TestClient(app_main.create_app()) as client:
        yield client

    get_settings.cache_clear()
    reset_engine_cache()
    reset_metrics()
    reset_login_rate_limiter()


@pytest.fixture
def authenticated_client(client: TestClient) -> TestClient:
    response = client.post(
        "/api/auth/login",
        json={"email": "admin@example.com", "password": "change-me"},
    )
    assert response.status_code == 200
    return client


@pytest.fixture
def fake_extractor(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = get_settings()
    local_timezone = ZoneInfo(settings.timezone)
    local_now = datetime.now(UTC).astimezone(local_timezone)
    coverage_day = local_now.date() - timedelta(days=1)
    published_at = datetime(
        coverage_day.year,
        coverage_day.month,
        coverage_day.day,
        12,
        0,
        tzinfo=local_timezone,
    ).astimezone(UTC)

    def _fake_extract(self, url: str) -> ExtractedContent:
        return ExtractedContent(
            title="Manual import item",
            cleaned_text=(
                "A hand-imported article about evaluation discipline, verifier routing, "
                "and ranking transparency."
            ),
            outbound_links=["https://example.com/related"],
            published_at=published_at,
            mime_type="text/html",
            extraction_confidence=0.91,
            raw_payload={"html": "<html></html>", "fetched_url": url},
        )

    monkeypatch.setattr(
        "app.integrations.extractors.ContentExtractor.extract_from_url", _fake_extract
    )
