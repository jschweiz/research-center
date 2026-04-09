from fastapi.testclient import TestClient
from sqlalchemy import select

from app.db.models import Digest, DigestEntry
from app.db.session import get_session_factory


def test_profile_defaults_to_live_mode_when_demo_seed_is_off(authenticated_client: TestClient) -> None:
    response = authenticated_client.get("/api/profile")
    assert response.status_code == 200
    payload = response.json()
    assert payload["data_mode"] == "live"
    assert payload["alphaxiv_search_settings"] == {
        "topics": [],
        "organizations": [],
        "sort": "Hot",
        "interval": "30 Days",
        "source": None,
    }


def test_profile_rejects_invalid_timezone(authenticated_client: TestClient) -> None:
    response = authenticated_client.patch("/api/profile", json={"timezone": "Mars/Olympus"})
    assert response.status_code == 422


def test_profile_accepts_timezone_update(authenticated_client: TestClient) -> None:
    response = authenticated_client.patch(
        "/api/profile",
        json={"timezone": "America/New_York", "digest_time": "06:30:00"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["timezone"] == "America/New_York"
    assert payload["digest_time"].startswith("06:30")


def test_profile_accepts_data_mode_update(authenticated_client: TestClient) -> None:
    response = authenticated_client.patch("/api/profile", json={"data_mode": "seed"})
    assert response.status_code == 200
    assert response.json()["data_mode"] == "seed"


def test_profile_accepts_alphaxiv_search_settings_update(authenticated_client: TestClient) -> None:
    response = authenticated_client.patch(
        "/api/profile",
        json={
            "alphaxiv_search_settings": {
                "topics": ["agents", "reasoning"],
                "organizations": ["OpenAI", "Anthropic"],
                "sort": "Recommended",
                "interval": "90 Days",
                "source": "GitHub",
            }
        },
    )
    assert response.status_code == 200
    assert response.json()["alphaxiv_search_settings"] == {
        "topics": ["agents", "reasoning"],
        "organizations": ["OpenAI", "Anthropic"],
        "sort": "Recommended",
        "interval": "90 Days",
        "source": "GitHub",
    }


def test_profile_timezone_update_purges_cached_digests(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    assert created.status_code == 201

    digest = authenticated_client.get("/api/briefs/today")
    assert digest.status_code == 200

    with get_session_factory()() as db:
        assert list(db.scalars(select(Digest)).all())
        assert list(db.scalars(select(DigestEntry)).all())

    updated = authenticated_client.patch("/api/profile", json={"timezone": "America/New_York"})
    assert updated.status_code == 200

    with get_session_factory()() as db:
        assert list(db.scalars(select(Digest)).all()) == []
        assert list(db.scalars(select(DigestEntry)).all()) == []
