from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
from fastapi.testclient import TestClient

from app.db.models import (
    ConnectionProvider,
    ConnectionStatus,
    ContentType,
    IngestionRun,
    IngestionRunType,
    Item,
    Source,
)
from app.db.session import get_session_factory
from app.integrations.gmail import NewsletterMessage
from app.services.connections import ConnectionService


def test_create_and_list_source(authenticated_client: TestClient) -> None:
    response = authenticated_client.post(
        "/api/sources",
        json={
            "type": "rss",
            "name": "Testing Feed",
            "url": "https://example.com/feed.xml",
            "priority": 88,
            "tags": ["testing", "rss"],
            "config_json": {},
            "rules": [{"rule_type": "sender", "value": "alerts@example.com", "active": True}],
        },
    )
    assert response.status_code == 201
    payload = response.json()
    assert payload["name"] == "Testing Feed"
    assert payload["rules"][0]["value"] == "alerts@example.com"

    listing = authenticated_client.get("/api/sources")
    assert listing.status_code == 200
    assert len(listing.json()) == 1


def test_list_sources_hides_internal_manual_source(authenticated_client: TestClient) -> None:
    authenticated_client.post(
        "/api/items/import-url",
        json={"url": "https://example.com/article"},
    )

    listing = authenticated_client.get("/api/sources")
    assert listing.status_code == 200
    assert listing.json() == []


def test_list_sources_can_include_internal_manual_source_for_inbox(authenticated_client: TestClient) -> None:
    authenticated_client.post(
        "/api/items/import-url",
        json={"url": "https://example.com/article"},
    )

    listing = authenticated_client.get("/api/sources", params={"include_manual": "true"})
    assert listing.status_code == 200
    assert len(listing.json()) == 1
    assert listing.json()[0]["name"] == "Manual import"
    assert listing.json()[0]["type"] == "manual_url"


def test_create_source_rejects_blank_name(authenticated_client: TestClient) -> None:
    response = authenticated_client.post(
        "/api/sources",
        json={
            "type": "rss",
            "name": "   ",
            "url": "https://example.com/feed.xml",
        },
    )
    assert response.status_code == 422


def test_create_source_requires_url_or_query(authenticated_client: TestClient) -> None:
    response = authenticated_client.post(
        "/api/sources",
        json={
            "type": "gmail_newsletter",
            "name": "Newsletter feed",
        },
    )
    assert response.status_code == 422


def test_update_source_persists_management_changes(authenticated_client: TestClient) -> None:
    created = authenticated_client.post(
        "/api/sources",
        json={
            "type": "rss",
            "name": "Testing Feed",
            "url": "https://example.com/feed.xml",
            "priority": 88,
            "tags": ["testing"],
        },
    )
    source_id = created.json()["id"]

    updated = authenticated_client.patch(
        f"/api/sources/{source_id}",
        json={
            "name": "Testing Feed Weekly",
            "query": "cat:cs.AI",
            "url": None,
            "priority": 64,
            "active": False,
            "tags": ["testing", "weekly"],
            "description": "Weekly research digest.",
        },
    )
    assert updated.status_code == 200
    payload = updated.json()
    assert payload["name"] == "Testing Feed Weekly"
    assert payload["query"] == "cat:cs.AI"
    assert payload["url"] is None
    assert payload["priority"] == 64
    assert payload["active"] is False
    assert payload["tags"] == ["testing", "weekly"]
    assert payload["description"] == "Weekly research digest."


def test_delete_source_detaches_historical_records(authenticated_client: TestClient) -> None:
    created = authenticated_client.post(
        "/api/sources",
        json={
            "type": "rss",
            "name": "Testing Feed",
            "url": "https://example.com/feed.xml",
        },
    )
    source_id = created.json()["id"]

    session = get_session_factory()()
    try:
        item = Item(
            source_id=source_id,
            title="Historic article",
            source_name="Testing Feed",
            canonical_url="https://example.com/historic-article",
            content_type=ContentType.ARTICLE,
            content_hash="historic-article",
        )
        run = IngestionRun(source_id=source_id, run_type=IngestionRunType.INGEST)
        session.add(item)
        session.add(run)
        session.commit()
        item_id = item.id
        run_id = run.id
    finally:
        session.close()

    deleted = authenticated_client.delete(f"/api/sources/{source_id}")
    assert deleted.status_code == 204

    session = get_session_factory()()
    try:
        source = session.get(Source, source_id)
        item = session.get(Item, item_id)
        run = session.get(IngestionRun, run_id)
        assert source is None
        assert item is not None and item.source_id is None
        assert run is not None and run.source_id is None
    finally:
        session.close()


def test_probe_rss_source_returns_count_and_sample_titles(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    created = authenticated_client.post(
        "/api/sources",
        json={
            "type": "rss",
            "name": "Testing Feed",
            "url": "https://example.com/feed.xml",
        },
    )
    source_id = created.json()["id"]

    def _load_feed(self, locator: str):
        assert locator == "https://example.com/feed.xml"
        return SimpleNamespace(
            entries=[
                SimpleNamespace(title="Entry one"),
                SimpleNamespace(title="Entry two"),
            ],
            feed={"title": "Example Feed"},
            bozo=False,
        )

    monkeypatch.setattr("app.services.ingestion.IngestionService._load_feed", _load_feed)

    response = authenticated_client.post(f"/api/sources/{source_id}/probe")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_id"] == source_id
    assert payload["source_type"] == "rss"
    assert payload["total_found"] == 2
    assert payload["sample_titles"] == ["Entry one", "Entry two"]
    assert payload["detail"] == "Lightweight check found 2 items in Example Feed."
    assert payload["checked_at"]


def test_probe_website_index_source_returns_filtered_article_links(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    created = authenticated_client.post(
        "/api/sources",
        json={
            "type": "rss",
            "name": "Anthropic News",
            "url": "https://www.anthropic.com/news",
            "config_json": {
                "discovery_mode": "website_index",
                "article_path_prefixes": ["/news/"],
            },
        },
    )
    source_id = created.json()["id"]

    def _fetch(url: str, *, timeout: int, headers=None, max_redirects: int = 5) -> httpx.Response:
        assert url == "https://www.anthropic.com/news"
        assert timeout == 20
        assert headers is None
        assert max_redirects == 5
        html = """
        <html>
          <body>
            <a href="/news/claude-sonnet-4-6">Claude Sonnet 4.6</a>
            <a href="https://www.anthropic.com/news/claude-opus-4-6?utm_source=homepage">Claude Opus 4.6</a>
            <a href="/news">News index</a>
            <a href="/research">Research</a>
            <a href="https://example.com/news/offsite">Offsite</a>
          </body>
        </html>
        """
        return httpx.Response(
            200,
            text=html,
            headers={"content-type": "text/html"},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr("app.services.ingestion.fetch_safe_response", _fetch)

    response = authenticated_client.post(f"/api/sources/{source_id}/probe")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_id"] == source_id
    assert payload["source_type"] == "rss"
    assert payload["total_found"] == 2
    assert payload["sample_titles"] == ["Claude Sonnet 4.6", "Claude Opus 4.6"]
    assert (
        payload["detail"]
        == "Lightweight check found 2 items in Anthropic News from https://www.anthropic.com/news."
    )


def test_probe_source_rejects_non_http_feed_locator(authenticated_client: TestClient) -> None:
    created = authenticated_client.post(
        "/api/sources",
        json={
            "type": "rss",
            "name": "Local file feed",
            "query": "/etc/hosts",
        },
    )
    source_id = created.json()["id"]

    response = authenticated_client.post(f"/api/sources/{source_id}/probe")

    assert response.status_code == 400
    assert response.json()["detail"] == "Could not read the feed: Outbound URLs must use http or https."


def test_probe_source_rejects_private_network_feed_url(authenticated_client: TestClient) -> None:
    created = authenticated_client.post(
        "/api/sources",
        json={
            "type": "rss",
            "name": "Private feed",
            "url": "http://127.0.0.1/feed.xml",
        },
    )
    source_id = created.json()["id"]

    response = authenticated_client.post(f"/api/sources/{source_id}/probe")

    assert response.status_code == 400
    assert (
        response.json()["detail"]
        == "Could not read the feed: Outbound URLs must not target a private or local network address."
    )


def test_probe_gmail_source_returns_count_and_sample_titles(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    class StubOauthConnector:
        def __init__(self, access_token: str) -> None:
            captured["access_token"] = access_token

        def list_newsletters(self, senders=None, labels=None, raw_query=None, max_results=20, newer_than_days=7):
            captured["senders"] = senders
            captured["labels"] = labels
            captured["raw_query"] = raw_query
            captured["max_results"] = max_results
            captured["newer_than_days"] = newer_than_days
            return [
                NewsletterMessage(
                    message_id="msg-1",
                    thread_id="thread-1",
                    subject="TLDR AI issue one",
                    sender="TLDR AI <hi@tldrnewsletter.com>",
                    published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
                    text_body="Issue one body.",
                    html_body="<p>Issue one body.</p>",
                    outbound_links=["https://example.com/one"],
                    permalink="https://mail.google.com/mail/u/0/#inbox/msg-1",
                ),
                NewsletterMessage(
                    message_id="msg-2",
                    thread_id="thread-2",
                    subject="TLDR AI issue two",
                    sender="TLDR AI <hi@tldrnewsletter.com>",
                    published_at=datetime(2026, 3, 27, 9, 0, tzinfo=UTC),
                    text_body="Issue two body.",
                    html_body="<p>Issue two body.</p>",
                    outbound_links=["https://example.com/two"],
                    permalink="https://mail.google.com/mail/u/0/#inbox/msg-2",
                ),
            ]

    monkeypatch.setattr("app.services.ingestion.GmailConnector", StubOauthConnector)

    with get_session_factory()() as db:
        ConnectionService(db).store_connection(
            provider=ConnectionProvider.GMAIL,
            label="Primary Gmail",
            payload={
                "access_token": "gmail-access-token",
                "refresh_token": "gmail-refresh-token",
                "expires_at": "2030-01-01T00:00:00+00:00",
                "auth_mode": "oauth",
            },
            metadata_json={"auth_mode": "oauth", "connected_email": "reader@example.com"},
            status=ConnectionStatus.CONNECTED,
        )

    created = authenticated_client.post(
        "/api/sources",
        json={
            "type": "gmail_newsletter",
            "name": "TLDR AI",
            "query": "newsletter@tldrnewsletter.com",
        },
    )
    source_id = created.json()["id"]

    response = authenticated_client.post(f"/api/sources/{source_id}/probe")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_id"] == source_id
    assert payload["source_type"] == "gmail_newsletter"
    assert payload["total_found"] == 2
    assert payload["sample_titles"] == ["TLDR AI issue one", "TLDR AI issue two"]
    assert payload["detail"] == (
        'Lightweight inbox check found 2 messages in the last 30 days using sender newsletter@tldrnewsletter.com.'
    )
    assert captured["access_token"] == "gmail-access-token"
    assert captured["senders"] == ["newsletter@tldrnewsletter.com"]
    assert captured["labels"] == []
    assert captured["raw_query"] is None
    assert captured["max_results"] == 20
    assert captured["newer_than_days"] == 30
