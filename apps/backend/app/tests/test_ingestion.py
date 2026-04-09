import base64
from contextlib import suppress
from datetime import UTC, date, datetime

import httpx
from fastapi.testclient import TestClient
from sqlalchemy import select

from app.db.models import (
    ConnectionProvider,
    ConnectionStatus,
    ContentType,
    IngestionRun,
    Item,
    RunStatus,
    Source,
    SourceRule,
    SourceType,
)
from app.db.session import get_session_factory
from app.integrations.extractors import ExtractedContent
from app.integrations.gmail import GmailConnector, NewsletterMessage
from app.services.connections import ConnectionService
from app.services.ingestion import (
    IngestionRunItemSummary,
    IngestionService,
    SourceRunSummary,
    normalize_url,
)


def build_source_summary(
    source: Source,
    count: int,
    *,
    fallback_count: int = 0,
    ai_total_tokens: int = 0,
) -> SourceRunSummary:
    return SourceRunSummary(
        source_id=source.id,
        source_name=source.name,
        status=RunStatus.SUCCEEDED,
        extractor_fallback_count=fallback_count,
        ai_total_tokens=ai_total_tokens,
        items=[
            IngestionRunItemSummary(
                title=f"{source.name} item {index + 1}",
                outcome="created",
                content_type=ContentType.ARTICLE.value,
                extraction_confidence=0.82,
            )
            for index in range(count)
        ],
    )


def test_create_ingest_cycle_run_starts_running_with_logs(client: TestClient) -> None:
    with get_session_factory()() as db:
        run = IngestionService(db).create_ingest_cycle_run()

        assert run.status == RunStatus.RUNNING
        assert run.metadata_json["operation_kind"] == "ingest_cycle"
        assert run.metadata_json["logs"]
        assert "Ingest requested" in run.metadata_json["logs"][0]["message"]


def test_estimate_ai_cost_includes_total_only_remainder(client: TestClient) -> None:
    with get_session_factory()() as db:
        service = IngestionService(db)
        cost = service._estimate_ai_cost_usd(
            prompt_tokens=1_000,
            completion_tokens=500,
            total_tokens=2_000,
        )

    assert cost == 0.0017


def test_run_all_sources_continues_when_one_source_fails(
    client: TestClient,
    monkeypatch,
) -> None:
    with get_session_factory()() as db:
        db.add_all(
            [
                Source(
                    type=SourceType.RSS,
                    name="Live RSS",
                    url="https://example.com/feed.xml",
                    priority=90,
                    active=True,
                    tags=["rss"],
                ),
                Source(
                    type=SourceType.GMAIL,
                    name="Missing Gmail",
                    query="label:newsletters",
                    priority=70,
                    active=True,
                    tags=["gmail"],
                ),
            ]
        )
        db.commit()

        monkeypatch.setattr(
            "app.services.ingestion.IngestionService._ingest_feed_source",
            lambda self, source: build_source_summary(source, 3),
        )

        def _raise(self, source):
            raise RuntimeError("Gmail connection is missing access_token.")

        monkeypatch.setattr(
            "app.services.ingestion.IngestionService._ingest_gmail_source",
            _raise,
        )

        service = IngestionService(db)
        count = service.run_all_sources()
        assert count == 3

        statuses = {
            source.name: source.last_synced_at is not None
            for source in db.scalars(select(Source)).all()
        }
        assert statuses["Live RSS"] is True
        assert statuses["Missing Gmail"] is False

        failed_source_runs = list(
            db.scalars(
                select(IngestionRun).where(
                    IngestionRun.status == RunStatus.FAILED, IngestionRun.source_id.is_not(None)
                )
            ).all()
        )
        assert len(failed_source_runs) == 1

        cycle_runs = list(
            db.scalars(
                select(IngestionRun)
                .where(IngestionRun.source_id.is_(None))
                .order_by(IngestionRun.started_at.desc())
            ).all()
        )
        assert len(cycle_runs) == 1
        assert cycle_runs[0].status == RunStatus.FAILED
        assert cycle_runs[0].metadata_json["ingested_count"] == 3
        assert cycle_runs[0].metadata_json["failed_source_count"] == 1


def test_ingest_website_index_source_discovers_and_saves_article_links(
    client: TestClient,
    monkeypatch,
) -> None:
    def _fetch(url: str, *, timeout: int, headers=None, max_redirects: int = 5) -> httpx.Response:
        assert url == "https://www.anthropic.com/news"
        assert timeout == 20
        assert headers is None
        assert max_redirects == 5
        html = """
        <html>
          <body>
            <a href="/news/claude-sonnet-4-6">Claude Sonnet 4.6</a>
            <a href="https://www.anthropic.com/news/claude-sonnet-4-6?utm_source=homepage">Duplicate</a>
            <a href="/news/claude-opus-4-6">Claude Opus 4.6</a>
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

    def _extract(self, url: str) -> ExtractedContent:
        slug = url.rstrip("/").split("/")[-1]
        return ExtractedContent(
            title=f"Fetched {slug}",
            cleaned_text=f"Body for {slug}.",
            outbound_links=[f"{url}/related"],
            published_at=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
            mime_type="text/html",
            extraction_confidence=0.88,
            raw_payload={"fetched_url": url},
        )

    monkeypatch.setattr("app.services.ingestion.fetch_safe_response", _fetch)
    monkeypatch.setattr("app.integrations.extractors.ContentExtractor.extract_from_url", _extract)

    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Anthropic News",
            url="https://www.anthropic.com/news",
            priority=91,
            active=True,
            tags=["rss", "ai", "anthropic"],
            config_json={
                "discovery_mode": "website_index",
                "article_path_prefixes": ["/news/"],
            },
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        summary = IngestionService(db)._ingest_feed_source(source)

        assert summary.status == RunStatus.SUCCEEDED
        assert summary.ingested_count == 2
        assert summary.extractor_fallback_count == 0
        assert [item.title for item in summary.items] == [
            "Fetched claude-sonnet-4-6",
            "Fetched claude-opus-4-6",
        ]

        items = list(
            db.scalars(select(Item).where(Item.source_id == source.id).order_by(Item.canonical_url.asc())).all()
        )
        assert [item.canonical_url for item in items] == [
            "https://www.anthropic.com/news/claude-opus-4-6",
            "https://www.anthropic.com/news/claude-sonnet-4-6",
        ]
        assert all(item.metadata_json["discovery_mode"] == "website_index" for item in items)
        assert all(item.metadata_json["website_index_url"] == "https://www.anthropic.com/news" for item in items)


def test_retry_failed_runs_continues_when_source_still_fails(
    client: TestClient,
    monkeypatch,
) -> None:
    with get_session_factory()() as db:
        source = Source(
            type=SourceType.GMAIL,
            name="Broken Gmail",
            query="label:broken",
            priority=70,
            active=True,
            tags=["gmail"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        service = IngestionService(db)

        def _raise(self, source):
            raise RuntimeError("Gmail connection is missing access_token.")

        monkeypatch.setattr(
            "app.services.ingestion.IngestionService._ingest_gmail_source",
            _raise,
        )

        with suppress(RuntimeError):
            service.run_source(source)

        count = service.retry_failed_runs()
        assert count == 0

        failed_source_runs = list(
            db.scalars(
                select(IngestionRun).where(
                    IngestionRun.status == RunStatus.FAILED, IngestionRun.source_id.is_not(None)
                )
            ).all()
        )
        assert len(failed_source_runs) >= 2

        failed_cycle_runs = list(
            db.scalars(
                select(IngestionRun).where(
                    IngestionRun.status == RunStatus.FAILED, IngestionRun.source_id.is_(None)
                )
            ).all()
        )
        assert len(failed_cycle_runs) == 1


def test_normalize_url_preserves_gmail_message_fragment() -> None:
    assert (
        normalize_url("https://mail.google.com/mail/u/0/#inbox/abc123")
        == "https://mail.google.com/mail/u/0/#inbox/abc123"
    )


def test_ingest_payload_keeps_distinct_gmail_messages_by_fragment(client: TestClient) -> None:
    with get_session_factory()() as db:
        source = Source(
            type=SourceType.GMAIL,
            name="TLDR AI",
            query="label:tldr-ai",
            priority=76,
            active=True,
            tags=["newsletter", "ai"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        service = IngestionService(db)
        first = service.ingest_payload(
            source=source,
            title="TLDR AI issue one",
            canonical_url="https://mail.google.com/mail/u/0/#inbox/message-1",
            authors=["TLDR AI"],
            published_at=datetime(2026, 3, 27, 6, 0, tzinfo=UTC),
            cleaned_text="First newsletter body.",
            raw_payload={"message_id": "message-1"},
            outbound_links=[],
            extraction_confidence=0.95,
            metadata_json={"thread_id": "thread-1"},
            content_type=ContentType.NEWSLETTER,
        )
        second = service.ingest_payload(
            source=source,
            title="TLDR AI issue two",
            canonical_url="https://mail.google.com/mail/u/0/#inbox/message-2",
            authors=["TLDR AI"],
            published_at=datetime(2026, 3, 27, 7, 0, tzinfo=UTC),
            cleaned_text="Second newsletter body.",
            raw_payload={"message_id": "message-2"},
            outbound_links=[],
            extraction_confidence=0.95,
            metadata_json={"thread_id": "thread-2"},
            content_type=ContentType.NEWSLETTER,
        )

        assert first.id != second.id
        items = list(
            db.scalars(
                select(Item).where(Item.source_id == source.id).order_by(Item.published_at.asc())
            ).all()
        )
        assert len(items) == 2
        assert [item.canonical_url for item in items] == [
            "https://mail.google.com/mail/u/0/#inbox/message-1",
            "https://mail.google.com/mail/u/0/#inbox/message-2",
        ]


def test_gmail_connector_parses_published_at_from_date_header() -> None:
    connector = GmailConnector(access_token="gmail-access-token")
    text_body = base64.urlsafe_b64encode(
        b"Newsletter body.\nRead more at https://example.com/plain.\n"
    ).decode("utf-8")
    html_body = base64.urlsafe_b64encode(
        b'<p>Newsletter body. <a href="https://example.com/html">Read online</a></p>'
    ).decode("utf-8")

    message = {
        "id": "msg-1",
        "threadId": "thread-1",
        "payload": {
            "headers": [
                {"name": "Subject", "value": "TLDR AI"},
                {"name": "From", "value": "TLDR AI <hi@tldrnewsletter.com>"},
                {"name": "Date", "value": "Fri, 27 Mar 2026 08:00:00 +0100"},
            ],
            "mimeType": "multipart/alternative",
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {"data": text_body},
                },
                {
                    "mimeType": "text/html",
                    "body": {"data": html_body},
                },
            ],
        },
    }

    parsed = connector._parse_message(message)

    assert parsed is not None
    assert parsed.published_at == datetime(2026, 3, 27, 7, 0, tzinfo=UTC)
    assert parsed.outbound_links == [
        "https://example.com/html",
        "https://example.com/plain",
    ]


def test_ingest_payload_reports_old_and_new_affected_edition_days_when_source_date_changes(
    client: TestClient,
) -> None:
    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Date Move Feed",
            url="https://example.com/date-move.xml",
            priority=70,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        service = IngestionService(db)
        first = service._ingest_payload_with_result(
            source=source,
            title="Date move item",
            canonical_url="https://example.com/date-move-item",
            authors=["Researcher"],
            published_at=datetime(2026, 3, 26, 8, 0, tzinfo=UTC),
            cleaned_text="Original source date.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
        )
        updated = service._ingest_payload_with_result(
            source=source,
            title="Date move item",
            canonical_url="https://example.com/date-move-item",
            authors=["Researcher"],
            published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
            cleaned_text="Updated source date.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
        )

        assert first.affected_edition_days == [date(2026, 3, 27)]
        assert updated.created is False
        assert updated.affected_edition_days == [date(2026, 3, 27), date(2026, 3, 28)]


def test_ingest_payload_uses_identity_hash_for_typo_fix_updates_without_links(
    client: TestClient,
) -> None:
    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Identity Feed",
            url="https://example.com/identity.xml",
            priority=70,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        service = IngestionService(db)
        first = service._ingest_payload_with_result(
            source=source,
            title="Identity keyed article",
            canonical_url=f"{source.id}-0",
            authors=["Researcher"],
            published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
            cleaned_text="This summary contains teh original typo.",
            raw_payload={
                "feed_entry": {
                    "id": "feed-entry-1",
                    "title": "Identity keyed article",
                    "link": "",
                    "published": "2026-03-27T08:00:00+00:00",
                    "summary": "This summary contains teh original typo.",
                }
            },
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={"feed_entry_id": "feed-entry-1"},
            content_type=ContentType.ARTICLE,
        )
        original_item_id = first.item.id
        original_identity_hash = first.item.identity_hash
        original_revision_hash = first.item.content_hash
        original_canonical_url = first.item.canonical_url

        updated = service._ingest_payload_with_result(
            source=source,
            title="Identity keyed article",
            canonical_url=f"{source.id}-7",
            authors=["Researcher"],
            published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
            cleaned_text="This summary contains the corrected typo.",
            raw_payload={
                "feed_entry": {
                    "id": "feed-entry-1",
                    "title": "Identity keyed article",
                    "link": "",
                    "published": "2026-03-27T08:00:00+00:00",
                    "summary": "This summary contains the corrected typo.",
                }
            },
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={"feed_entry_id": "feed-entry-1"},
            content_type=ContentType.ARTICLE,
        )

        items = db.scalars(select(Item).where(Item.source_id == source.id)).all()

        assert updated.created is False
        assert updated.item.id == original_item_id
        assert len(items) == 1
        assert original_identity_hash is not None
        assert updated.item.identity_hash == original_identity_hash
        assert updated.item.content_hash != original_revision_hash
        assert updated.item.canonical_url == original_canonical_url
        assert updated.item.canonical_url == f"source://{source.id}/{original_identity_hash}"
        assert updated.item.content is not None
        assert updated.item.content.cleaned_text == "This summary contains the corrected typo."


def test_ingest_gmail_source_supports_app_password_connection(
    client: TestClient,
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    class StubImapConnector:
        def __init__(self, *, email_address: str, app_password: str) -> None:
            captured["email_address"] = email_address
            captured["app_password"] = app_password

        def list_newsletters(self, senders=None, labels=None, raw_query=None, max_results=20):
            captured["senders"] = senders
            captured["labels"] = labels
            captured["raw_query"] = raw_query
            captured["max_results"] = max_results
            return [
                NewsletterMessage(
                    message_id="msg-1",
                    thread_id="thread-1",
                    subject="TLDR AI manual auth issue",
                    sender="TLDR AI <hi@tldrnewsletter.com>",
                    published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
                    text_body="Newsletter body from IMAP.",
                    html_body="<p>Newsletter body from IMAP.</p>",
                    outbound_links=["https://example.com/story"],
                    permalink="https://mail.google.com/mail/u/0/#search/rfc822msgid%3Amsg-1",
                )
            ]

    monkeypatch.setattr("app.services.ingestion.GmailImapConnector", StubImapConnector)

    with get_session_factory()() as db:
        ConnectionService(db).store_connection(
            provider=ConnectionProvider.GMAIL,
            label="Primary Gmail",
            payload={
                "auth_mode": "app_password",
                "email": "reader@example.com",
                "app_password": "gmail-app-password",
            },
            metadata_json={
                "auth_mode": "app_password",
                "connected_email": "reader@example.com",
                "labels": ["tldr-ai"],
            },
            status=ConnectionStatus.CONNECTED,
        )
        source = Source(
            type=SourceType.GMAIL,
            name="TLDR AI",
            query="label:tldr-ai",
            priority=76,
            active=True,
            tags=["newsletter", "ai"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        summary = IngestionService(db)._ingest_gmail_source(source)

        assert summary.status == RunStatus.SUCCEEDED
        assert len(summary.items) == 1
        assert summary.items[0].title == "TLDR AI manual auth issue"
        assert captured["email_address"] == "reader@example.com"
        assert captured["app_password"] == "gmail-app-password"
        assert captured["labels"] == []
        assert captured["raw_query"] == "label:tldr-ai"


def test_gmail_source_query_email_is_used_as_sender_filter_and_global_senders_are_ignored(
    client: TestClient,
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    class StubOauthConnector:
        def __init__(self, access_token: str) -> None:
            captured["access_token"] = access_token

        def list_newsletters(self, senders=None, labels=None, raw_query=None, max_results=20):
            captured["senders"] = senders
            captured["labels"] = labels
            captured["raw_query"] = raw_query
            captured["max_results"] = max_results
            return []

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
            metadata_json={
                "auth_mode": "oauth",
                "connected_email": "reader@example.com",
                "senders": ["global@example.com"],
                "labels": ["global-label"],
            },
            status=ConnectionStatus.CONNECTED,
        )
        source = Source(
            type=SourceType.GMAIL,
            name="TLDR AI",
            query="newsletter@tldrnewsletter.com",
            priority=76,
            active=True,
            tags=["newsletter", "ai"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        summary = IngestionService(db)._ingest_gmail_source(source)

        assert summary.status == RunStatus.SUCCEEDED
        assert captured["access_token"] == "gmail-access-token"
        assert captured["senders"] == ["newsletter@tldrnewsletter.com"]
        assert captured["labels"] == []
        assert captured["raw_query"] is None


def test_gmail_source_query_raw_search_is_forwarded_to_connector(
    client: TestClient,
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    class StubOauthConnector:
        def __init__(self, access_token: str) -> None:
            captured["access_token"] = access_token

        def list_newsletters(self, senders=None, labels=None, raw_query=None, max_results=20):
            captured["senders"] = senders
            captured["labels"] = labels
            captured["raw_query"] = raw_query
            return []

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
        source = Source(
            type=SourceType.GMAIL,
            name="The Batch",
            query="from:updates@deeplearning.ai label:the-batch",
            priority=80,
            active=True,
            tags=["newsletter", "ai"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        summary = IngestionService(db)._ingest_gmail_source(source)

        assert summary.status == RunStatus.SUCCEEDED
        assert captured["senders"] == []
        assert captured["labels"] == []
        assert captured["raw_query"] == "from:updates@deeplearning.ai label:the-batch"


def test_gmail_source_query_overrides_legacy_source_rules(
    client: TestClient,
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    class StubOauthConnector:
        def __init__(self, access_token: str) -> None:
            captured["access_token"] = access_token

        def list_newsletters(self, senders=None, labels=None, raw_query=None, max_results=20):
            captured["senders"] = senders
            captured["labels"] = labels
            captured["raw_query"] = raw_query
            return []

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
        source = Source(
            type=SourceType.GMAIL,
            name="TLDR AI",
            query="from:dan@tldrnewsletter.com",
            priority=76,
            active=True,
            tags=["newsletter", "ai"],
        )
        source.rules.append(SourceRule(rule_type="label", value="tldr-ai", active=True))
        db.add(source)
        db.commit()
        db.refresh(source)

        summary = IngestionService(db)._ingest_gmail_source(source)

        assert summary.status == RunStatus.SUCCEEDED
        assert captured["access_token"] == "gmail-access-token"
        assert captured["senders"] == []
        assert captured["labels"] == []
        assert captured["raw_query"] == "from:dan@tldrnewsletter.com"


def test_ingestion_history_endpoint_returns_cycle_summaries(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="History Feed",
            url="https://example.com/history.xml",
            priority=88,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()

    monkeypatch.setattr(
        "app.services.ingestion.IngestionService._ingest_feed_source",
        lambda self, source: build_source_summary(source, 2, fallback_count=1, ai_total_tokens=420),
    )

    run_response = authenticated_client.post("/api/ops/ingest-now")
    assert run_response.status_code == 200
    assert run_response.json()["operation_run_id"]

    history_response = authenticated_client.get("/api/ops/ingestion-runs")
    assert history_response.status_code == 200
    payload = history_response.json()
    assert len(payload) == 1
    assert payload[0]["title"] == "Ingest cycle"
    assert payload[0]["operation_kind"] == "ingest_cycle"
    assert payload[0]["total_titles"] == 2
    assert payload[0]["source_count"] == 1
    assert payload[0]["extractor_fallback_count"] == 1
    assert payload[0]["ai_total_tokens"] == 420
    assert payload[0]["ai_cost_usd"] > 0
    assert payload[0]["tts_cost_usd"] == 0
    assert payload[0]["total_cost_usd"] == payload[0]["ai_cost_usd"]
    assert payload[0]["logs"]
    assert any("Starting source: History Feed" in entry["message"] for entry in payload[0]["logs"])
    assert payload[0]["source_stats"][0]["source_name"] == "History Feed"
    assert payload[0]["source_stats"][0]["ai_total_tokens"] == 420
    assert payload[0]["source_stats"][0]["items"][0]["title"] == "History Feed item 1"


def test_run_all_sources_logs_intermediate_gmail_source_steps(
    client: TestClient,
    monkeypatch,
) -> None:
    class StubOauthConnector:
        def __init__(self, access_token: str) -> None:
            assert access_token == "gmail-access-token"

        def list_newsletters(self, senders=None, labels=None, raw_query=None, max_results=20):
            assert senders == ["newsletter@tldrnewsletter.com"]
            assert labels == []
            assert raw_query is None
            links = [f"https://example.com/story-{index}" for index in range(1, 12)]
            return [
                NewsletterMessage(
                    message_id="msg-1",
                    thread_id="thread-1",
                    subject="TLDR AI",
                    sender="TLDR AI <hi@tldrnewsletter.com>",
                    published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
                    text_body="Newsletter body.",
                    html_body="<p>Newsletter body.</p>",
                    outbound_links=links,
                    permalink="https://mail.google.com/mail/u/0/#inbox/msg-1",
                )
            ]

    monkeypatch.setattr("app.services.ingestion.GmailConnector", StubOauthConnector)
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.split_newsletter_message",
        lambda self, newsletter: {
            "generation_mode": "remote",
            "_usage": {"total_tokens": 912},
            "facts": [
                {
                    "headline": f"Fact {index}",
                    "summary": f"Summary {index}",
                    "why_it_matters": f"Why {index}",
                    "relevant_links": [f"https://example.com/story-{index}"],
                }
                for index in range(1, 12)
            ],
        },
    )

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
        source = Source(
            type=SourceType.GMAIL,
            name="TLDR AI",
            query="newsletter@tldrnewsletter.com",
            priority=76,
            active=True,
            tags=["newsletter", "ai"],
        )
        db.add(source)
        db.commit()

        service = IngestionService(db)
        cycle_run = service.create_ingest_cycle_run()

        ingested_count, _ = service.run_all_sources_with_affected_edition_days(
            cycle_run_id=cycle_run.id
        )

        assert ingested_count == 11
        history = service.list_recent_ingestion_cycles()
        assert len(history) == 1
        log_messages = [entry["message"] for entry in history[0]["logs"]]

        assert any(
            message
            == "TLDR AI: Fetching Gmail messages using sender newsletter@tldrnewsletter.com."
            for message in log_messages
        )
        assert any(
            "TLDR AI: Loaded 1 newsletter message from Gmail in " in message
            for message in log_messages
        )
        assert any(
            message == 'TLDR AI: Message "TLDR AI" (1/1): splitting newsletter into facts.'
            for message in log_messages
        )
        assert any(
            'TLDR AI: Message "TLDR AI" (1/1): identified 11 facts in ' in message
            for message in log_messages
        )
        assert any("mode=remote, tokens=912" in message for message in log_messages)
        assert any(
            'TLDR AI: Message "TLDR AI" (1/1): saved 10/11 facts in ' in message
            for message in log_messages
        )
        assert any(
            'TLDR AI: Message "TLDR AI" (1/1): stored 11 facts in ' in message
            and "(11 new, 0 refreshed)." in message
            for message in log_messages
        )
        assert any(
            message == "Completed source: TLDR AI (11 titles; 11 new; 0 refreshed; 912 AI tokens)."
            for message in log_messages
        )


def test_ingest_gmail_source_splits_newsletter_into_multiple_fast_reads(
    client: TestClient,
    monkeypatch,
) -> None:
    class StubOauthConnector:
        def __init__(self, access_token: str) -> None:
            assert access_token == "gmail-access-token"

        def list_newsletters(self, senders=None, labels=None, raw_query=None, max_results=20):
            return [
                NewsletterMessage(
                    message_id="msg-1",
                    thread_id="thread-1",
                    subject="TLDR AI",
                    sender="TLDR AI <hi@tldrnewsletter.com>",
                    published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
                    text_body=(
                        "A new model release focuses on lower-latency tool use. "
                        "A new paper argues verifier routing improves research triage."
                    ),
                    html_body="<p>Newsletter body.</p>",
                    outbound_links=["https://example.com/model", "https://example.com/paper"],
                    permalink="https://mail.google.com/mail/u/0/#inbox/msg-1",
                )
            ]

    monkeypatch.setattr("app.services.ingestion.GmailConnector", StubOauthConnector)
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.split_newsletter_message",
        lambda self, newsletter: {
            "generation_mode": "remote",
            "_usage": {"total_tokens": 912},
            "facts": [
                {
                    "headline": "Model launch to watch",
                    "summary": "A new model release focuses on lower-latency tool use.",
                    "why_it_matters": "faster tool use, lower latency",
                },
                {
                    "headline": "Paper worth a skim",
                    "summary": "A new paper argues verifier routing improves research triage.",
                    "why_it_matters": "better triage, verifier routing",
                },
            ],
        },
    )

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
        source = Source(
            type=SourceType.GMAIL,
            name="TLDR AI",
            query="newsletter@tldrnewsletter.com",
            priority=76,
            active=True,
            tags=["newsletter", "ai"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        service = IngestionService(db)
        summary = service._ingest_gmail_source(source)

        assert summary.status == RunStatus.SUCCEEDED
        assert summary.ai_total_tokens == 912
        assert len(summary.items) == 2
        assert summary.items[0].title == "Model launch to watch"
        assert summary.items[1].title == "Paper worth a skim"

        base_item = db.scalar(
            select(Item).where(
                Item.canonical_url == "https://mail.google.com/mail/u/0/#inbox/msg-1"
            )
        )
        assert base_item is not None
        assert base_item.title == "Model launch to watch"
        assert base_item.insight is not None
        assert base_item.insight.short_summary == (
            "A new model release focuses on lower-latency tool use."
        )
        assert base_item.insight.whats_new == (
            "A new model release focuses on lower-latency tool use."
        )
        assert base_item.insight.follow_up_questions
        assert base_item.content is not None
        assert "Newsletter: TLDR AI" in base_item.content.cleaned_text
        assert "Relevant links:\n- https://example.com/model" in base_item.content.cleaned_text
        assert base_item.content.outbound_links == ["https://example.com/model"]
        assert base_item.content.raw_payload["html"] == "<p>Newsletter body.</p>"
        assert base_item.content.raw_payload["fact"]["relevant_links"] == [
            "https://example.com/model"
        ]

        second_url = service._build_newsletter_fact_permalink(
            "https://mail.google.com/mail/u/0/#inbox/msg-1",
            2,
        )
        second_item = db.scalar(select(Item).where(Item.canonical_url == normalize_url(second_url)))
        assert second_item is not None
        assert second_item.insight is not None
        assert second_item.insight.short_summary == (
            "A new paper argues verifier routing improves research triage."
        )
        assert second_item.insight.follow_up_questions
        assert second_item.content is not None
        assert second_item.content.outbound_links == ["https://example.com/paper"]
        assert second_item.content.raw_payload["fact"]["relevant_links"] == [
            "https://example.com/paper"
        ]
        assert "html" not in second_item.content.raw_payload


def test_ingest_gmail_source_reuses_original_permalink_for_first_fact_and_is_idempotent(
    client: TestClient,
    monkeypatch,
) -> None:
    class StubOauthConnector:
        def __init__(self, access_token: str) -> None:
            assert access_token == "gmail-access-token"

        def list_newsletters(self, senders=None, labels=None, raw_query=None, max_results=20):
            return [
                NewsletterMessage(
                    message_id="msg-1",
                    thread_id="thread-1",
                    subject="TLDR AI",
                    sender="TLDR AI <hi@tldrnewsletter.com>",
                    published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
                    text_body="Legacy newsletter body.",
                    html_body="<p>Legacy newsletter body.</p>",
                    outbound_links=["https://example.com/model"],
                    permalink="https://mail.google.com/mail/u/0/#inbox/msg-1",
                )
            ]

    monkeypatch.setattr("app.services.ingestion.GmailConnector", StubOauthConnector)
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.split_newsletter_message",
        lambda self, newsletter: {
            "generation_mode": "remote",
            "facts": [
                {
                    "headline": "Lead item",
                    "summary": "The lead item now becomes the first fast read.",
                    "why_it_matters": "lead item, first fast read",
                },
                {
                    "headline": "Second item",
                    "summary": "A second fast read is created from the same email.",
                    "why_it_matters": "second read, same email",
                },
            ],
        },
    )

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
        source = Source(
            type=SourceType.GMAIL,
            name="TLDR AI",
            query="newsletter@tldrnewsletter.com",
            priority=76,
            active=True,
            tags=["newsletter", "ai"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        service = IngestionService(db)
        legacy = service.ingest_payload(
            source=source,
            title="TLDR AI",
            canonical_url="https://mail.google.com/mail/u/0/#inbox/msg-1",
            authors=["TLDR AI <hi@tldrnewsletter.com>"],
            published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
            cleaned_text="Legacy newsletter body.",
            raw_payload={"message_id": "msg-1"},
            outbound_links=[],
            extraction_confidence=0.95,
            metadata_json={"thread_id": "thread-1", "sender": "TLDR AI <hi@tldrnewsletter.com>"},
            content_type=ContentType.NEWSLETTER,
        )

        first_run = service._ingest_gmail_source(source)
        second_run = service._ingest_gmail_source(source)

        items = list(db.scalars(select(Item).where(Item.source_id == source.id)).all())
        assert len(items) == 2

        first_item = db.scalar(
            select(Item).where(
                Item.canonical_url == "https://mail.google.com/mail/u/0/#inbox/msg-1"
            )
        )
        assert first_item is not None
        assert first_item.id == legacy.id
        assert first_item.title == "Lead item"
        assert first_run.items[0].outcome == "updated"
        assert first_run.items[1].outcome == "created"
        assert [item.outcome for item in second_run.items] == ["updated", "updated"]


def test_ingest_gmail_source_handles_missing_message_date_deterministically(
    client: TestClient,
    monkeypatch,
) -> None:
    class StubOauthConnector:
        def __init__(self, access_token: str) -> None:
            assert access_token == "gmail-access-token"

        def list_newsletters(self, senders=None, labels=None, raw_query=None, max_results=20):
            return [
                NewsletterMessage(
                    message_id="msg-undated",
                    thread_id="thread-undated",
                    subject="TLDR AI",
                    sender="TLDR AI <hi@tldrnewsletter.com>",
                    published_at=None,
                    text_body="A new model release focuses on lower-latency tool use.",
                    html_body="<p>Newsletter body.</p>",
                    outbound_links=["https://example.com/model"],
                    permalink="https://mail.google.com/mail/u/0/#inbox/msg-undated",
                )
            ]

    monkeypatch.setattr("app.services.ingestion.GmailConnector", StubOauthConnector)
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.split_newsletter_message",
        lambda self, newsletter: {
            "generation_mode": "remote",
            "facts": [
                {
                    "headline": "Model launch to watch",
                    "summary": "A new model release focuses on lower-latency tool use.",
                    "why_it_matters": "faster tool use, lower latency",
                }
            ],
        },
    )

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
        source = Source(
            type=SourceType.GMAIL,
            name="TLDR AI",
            query="newsletter@tldrnewsletter.com",
            priority=76,
            active=True,
            tags=["newsletter", "ai"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        service = IngestionService(db)
        summary = service._ingest_gmail_source(source)
        item = db.scalar(
            select(Item).where(
                Item.canonical_url == "https://mail.google.com/mail/u/0/#inbox/msg-undated"
            )
        )

        assert summary.status == RunStatus.SUCCEEDED
        assert len(summary.items) == 1
        assert item is not None
        assert item.published_at is None
        assert item.title == "Model launch to watch"
