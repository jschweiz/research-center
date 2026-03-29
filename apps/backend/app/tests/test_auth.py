import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.db.session import reset_engine_cache


def test_login_and_me(client: TestClient) -> None:
    login = client.post("/api/auth/login", json={"email": "admin@example.com", "password": "change-me"})
    assert login.status_code == 200
    assert login.json()["authenticated"] is True

    me = client.get("/api/auth/me")
    assert me.status_code == 200
    assert me.json()["email"] == "admin@example.com"

    me_alias = client.get("/api/me")
    assert me_alias.status_code == 200
    assert me_alias.json()["email"] == "admin@example.com"


def test_invalid_login_rejected(client: TestClient) -> None:
    response = client.post("/api/auth/login", json={"email": "admin@example.com", "password": "wrong"})
    assert response.status_code == 401


def test_login_is_rate_limited_after_repeated_failures(client: TestClient) -> None:
    for _ in range(4):
        response = client.post(
            "/api/auth/login",
            json={"email": "admin@example.com", "password": "wrong"},
        )
        assert response.status_code == 401

    blocked = client.post(
        "/api/auth/login",
        json={"email": "admin@example.com", "password": "wrong"},
    )
    assert blocked.status_code == 429
    assert blocked.json()["detail"] == "Too many login attempts. Try again later."

    still_blocked = client.post(
        "/api/auth/login",
        json={"email": "admin@example.com", "password": "change-me"},
    )
    assert still_blocked.status_code == 429


def test_successful_login_resets_failed_attempt_counter(client: TestClient) -> None:
    for _ in range(4):
        response = client.post(
            "/api/auth/login",
            json={"email": "admin@example.com", "password": "wrong"},
        )
        assert response.status_code == 401

    success = client.post(
        "/api/auth/login",
        json={"email": "admin@example.com", "password": "change-me"},
    )
    assert success.status_code == 200

    for _ in range(4):
        response = client.post(
            "/api/auth/login",
            json={"email": "admin@example.com", "password": "wrong"},
        )
        assert response.status_code == 401

    blocked = client.post(
        "/api/auth/login",
        json={"email": "admin@example.com", "password": "wrong"},
    )
    assert blocked.status_code == 429


def _production_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    db_path = tmp_path / "production-test.db"
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    monkeypatch.setenv("AUTO_CREATE_SCHEMA", "true")
    monkeypatch.setenv("SEED_DEMO_DATA", "false")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD", "not-change-me")
    monkeypatch.setenv("SECRET_KEY", "production-secret")
    monkeypatch.setenv("ENCRYPTION_KEY", "production-encryption")
    monkeypatch.setenv("FRONTEND_ORIGIN", "https://frontend.example.com")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_ID", "gmail-client-id")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRET", "gmail-client-secret")
    monkeypatch.setenv("HOME", str(tmp_path))
    get_settings.cache_clear()
    reset_engine_cache()

    app_main = importlib.import_module("app.main")
    app_main = importlib.reload(app_main)
    return TestClient(app_main.create_app(), base_url="https://api.example.com")


def test_login_requires_frontend_origin_in_production(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _production_client(tmp_path, monkeypatch) as client:
        blocked = client.post(
            "/api/auth/login",
            json={"email": "admin@example.com", "password": "not-change-me"},
        )
        assert blocked.status_code == 403
        assert blocked.json()["detail"] == "Request origin is not allowed."

        allowed = client.post(
            "/api/auth/login",
            json={"email": "admin@example.com", "password": "not-change-me"},
            headers={"origin": "https://frontend.example.com"},
        )
        assert allowed.status_code == 200
        assert allowed.json()["authenticated"] is True


def test_state_changing_post_requires_frontend_origin_in_production(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _production_client(tmp_path, monkeypatch) as client:
        login = client.post(
            "/api/auth/login",
            json={"email": "admin@example.com", "password": "not-change-me"},
            headers={"origin": "https://frontend.example.com"},
        )
        assert login.status_code == 200

        blocked = client.post("/api/ops/clear-content")
        assert blocked.status_code == 403
        assert blocked.json()["detail"] == "Request origin is not allowed."

        allowed = client.post(
            "/api/ops/clear-content",
            headers={"origin": "https://frontend.example.com"},
        )
        assert allowed.status_code == 200


def test_oauth_start_requires_frontend_origin_in_production(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _production_client(tmp_path, monkeypatch) as client:
        login = client.post(
            "/api/auth/login",
            json={"email": "admin@example.com", "password": "not-change-me"},
            headers={"origin": "https://frontend.example.com"},
        )
        assert login.status_code == 200

        blocked = client.get("/api/connections/gmail/oauth/start", follow_redirects=False)
        assert blocked.status_code == 403
        assert blocked.json()["detail"] == "Request origin is not allowed."

        allowed = client.get(
            "/api/connections/gmail/oauth/start",
            headers={"referer": "https://frontend.example.com/connections"},
            follow_redirects=False,
        )
        assert allowed.status_code == 307


def test_side_effecting_brief_gets_require_frontend_origin_in_production(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _production_client(tmp_path, monkeypatch) as client:
        login = client.post(
            "/api/auth/login",
            json={"email": "admin@example.com", "password": "not-change-me"},
            headers={"origin": "https://frontend.example.com"},
        )
        assert login.status_code == 200

        blocked_brief = client.get("/api/briefs/today")
        assert blocked_brief.status_code == 403
        assert blocked_brief.json()["detail"] == "Request origin is not allowed."

        allowed_brief = client.get(
            "/api/briefs/today",
            headers={"referer": "https://frontend.example.com/brief"},
        )
        assert allowed_brief.status_code != 403

        blocked_audio = client.get("/api/briefs/2026-03-28/audio")
        assert blocked_audio.status_code == 403
        assert blocked_audio.json()["detail"] == "Request origin is not allowed."

        allowed_audio = client.get(
            "/api/briefs/2026-03-28/audio",
            headers={"referer": "https://frontend.example.com/brief/2026-03-28"},
        )
        assert allowed_audio.status_code != 403


def test_item_detail_get_requires_frontend_origin_in_production(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _production_client(tmp_path, monkeypatch) as client:
        login = client.post(
            "/api/auth/login",
            json={"email": "admin@example.com", "password": "not-change-me"},
            headers={"origin": "https://frontend.example.com"},
        )
        assert login.status_code == 200

        blocked = client.get("/api/items/00000000-0000-0000-0000-000000000000")
        assert blocked.status_code == 403
        assert blocked.json()["detail"] == "Request origin is not allowed."

        allowed = client.get(
            "/api/items/00000000-0000-0000-0000-000000000000",
            headers={"referer": "https://frontend.example.com/items/00000000-0000-0000-0000-000000000000"},
        )
        assert allowed.status_code != 403


def test_zotero_connection_refresh_get_requires_frontend_origin_in_production(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _production_client(tmp_path, monkeypatch) as client:
        login = client.post(
            "/api/auth/login",
            json={"email": "admin@example.com", "password": "not-change-me"},
            headers={"origin": "https://frontend.example.com"},
        )
        assert login.status_code == 200

        blocked = client.get("/api/connections/zotero")
        assert blocked.status_code == 403
        assert blocked.json()["detail"] == "Request origin is not allowed."

        allowed = client.get(
            "/api/connections/zotero",
            headers={"referer": "https://frontend.example.com/connections"},
        )
        assert allowed.status_code != 403
