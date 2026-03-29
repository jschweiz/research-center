from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.core.config import get_settings
from app.db.models import ActionType, ContentType, Item, Source, SourceType, UserAction
from app.db.session import get_session_factory
from app.integrations.zotero import ZoteroExportResult
from app.services.brief_dates import edition_day_for_datetimes
from app.services.ingestion import IngestionService


def _expected_manual_import_published_at() -> datetime:
    settings = get_settings()
    local_timezone = ZoneInfo(settings.timezone)
    local_now = datetime.now(UTC).astimezone(local_timezone)
    coverage_day = local_now.date() - timedelta(days=1)
    return datetime(
        coverage_day.year,
        coverage_day.month,
        coverage_day.day,
        12,
        0,
        tzinfo=local_timezone,
    ).astimezone(UTC)


def _expected_manual_import_edition_day() -> str:
    settings = get_settings()
    return edition_day_for_datetimes(
        published_at=_expected_manual_import_published_at(),
        first_seen_at=None,
        timezone_name=settings.timezone,
    ).isoformat()


def test_manual_import_is_idempotent(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    first = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    assert first.status_code == 201
    second = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    assert second.status_code == 201

    items = authenticated_client.get("/api/items")
    assert items.status_code == 200
    payload = items.json()
    assert len(payload) == 1
    assert payload[0]["title"] == "Manual import item"
    assert payload[0]["published_at"] == _expected_manual_import_published_at().isoformat()


def test_manual_import_dedupes_exact_content_hash_across_different_urls(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    first = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    assert first.status_code == 201

    second = authenticated_client.post("/api/items/import-url", json={"url": "https://mirror.example.com/article"})
    assert second.status_code == 201

    items = authenticated_client.get("/api/items")
    assert items.status_code == 200
    payload = items.json()
    assert len(payload) == 1
    assert payload[0]["also_mentioned_in_count"] == 1

    detail = authenticated_client.get(f"/api/items/{payload[0]['id']}")
    assert detail.status_code == 200
    related = detail.json()["also_mentioned_in"]
    assert len(related) == 1
    assert related[0]["canonical_url"] == "https://mirror.example.com/article"


def test_manual_import_dedupes_http_https_variants_without_overwriting_source(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Frontier Feed",
            url="https://example.com/feed.xml",
            priority=90,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        item = IngestionService(db).ingest_payload(
            source=source,
            title="Manual import item",
            canonical_url="http://example.com/article",
            authors=["Reporter"],
            published_at=None,
            cleaned_text="A hand-imported article about evaluation discipline, verifier routing, and ranking transparency.",
            raw_payload={"html": "<html></html>"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={"seeded": True},
            content_type=ContentType.ARTICLE,
        )
        assert item.source_name == "Frontier Feed"

    imported = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    assert imported.status_code == 201
    payload = imported.json()
    assert payload["source_name"] == "Frontier Feed"

    items = authenticated_client.get("/api/items")
    assert items.status_code == 200
    assert len(items.json()) == 1


def test_manual_import_updates_published_at_on_canonical_match(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    original_published_at = datetime(2026, 3, 25, 17, 54, 10, tzinfo=UTC)

    with get_session_factory()() as db:
        source = Source(
            type=SourceType.ARXIV,
            name="Frontier AI Papers",
            url="https://export.arxiv.org/api/query?search_query=all:hallucination",
            priority=95,
            active=True,
            tags=["arxiv"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        item = IngestionService(db).ingest_payload(
            source=source,
            title="[2603.24579v1] MARCH: Multi-Agent Reinforced Self-Check for LLM Hallucination",
            canonical_url="https://arxiv.org/abs/2603.24579v1",
            authors=["Zhuo Li"],
            published_at=original_published_at,
            cleaned_text="A paper about reducing hallucinations in RAG systems with multi-agent verification.",
            raw_payload={"html": "<html></html>"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={"seeded": True},
            content_type=ContentType.PAPER,
        )
        assert item.title == "MARCH: Multi-Agent Reinforced Self-Check for LLM Hallucination"
        item_id = item.id

    imported = authenticated_client.post("/api/items/import-url", json={"url": "https://arxiv.org/abs/2603.24579v1"})
    assert imported.status_code == 201
    payload = imported.json()
    assert payload["id"] == item_id
    assert payload["title"] == "MARCH: Multi-Agent Reinforced Self-Check for LLM Hallucination"
    assert payload["published_at"] == _expected_manual_import_published_at().isoformat()


def test_items_newest_sort_uses_hour_within_same_day(authenticated_client: TestClient) -> None:
    with get_session_factory()() as db:
        db.add(
            Item(
                title="Earlier item",
                source_name="Frontier Feed",
                authors=["Reporter"],
                published_at=datetime(2026, 3, 27, 9, 15, 0, tzinfo=UTC),
                canonical_url="https://example.com/earlier-item",
                content_type=ContentType.ARTICLE,
                content_hash="earlier-item",
            )
        )
        db.add(
            Item(
                title="Later item",
                source_name="Frontier Feed",
                authors=["Reporter"],
                published_at=datetime(2026, 3, 27, 14, 45, 0, tzinfo=UTC),
                canonical_url="https://example.com/later-item",
                content_type=ContentType.ARTICLE,
                content_hash="later-item",
            )
        )
        db.commit()

    response = authenticated_client.get("/api/items", params={"sort": "newest"})
    assert response.status_code == 200
    payload = response.json()
    assert [item["title"] for item in payload] == ["Later item", "Earlier item"]
    assert payload[0]["published_at"] == "2026-03-27T14:45:00+00:00"
    assert payload[1]["published_at"] == "2026-03-27T09:15:00+00:00"


def test_ignore_similar_persists_profile_topic_hint(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    assert created.status_code == 201
    item_id = created.json()["id"]

    ignored = authenticated_client.post(f"/api/items/{item_id}/ignore-similar")
    assert ignored.status_code == 200

    profile = authenticated_client.get("/api/profile")
    assert profile.status_code == 200
    assert "manual import item" in profile.json()["ignored_topics"]


def test_ignore_similar_strips_arxiv_prefix_from_profile_topic_hint(authenticated_client: TestClient) -> None:
    with get_session_factory()() as db:
        source = Source(
            type=SourceType.ARXIV,
            name="Frontier AI Papers",
            url="https://export.arxiv.org/api/query?search_query=all:hallucination",
            priority=95,
            active=True,
            tags=["arxiv"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        item = IngestionService(db).ingest_payload(
            source=source,
            title="[2603.24579v1] MARCH: Multi-Agent Reinforced Self-Check for LLM Hallucination",
            canonical_url="https://arxiv.org/abs/2603.24579v1",
            authors=["Zhuo Li"],
            published_at=datetime(2026, 3, 25, 17, 54, 10, tzinfo=UTC),
            cleaned_text="A paper about reducing hallucinations in RAG systems with multi-agent verification.",
            raw_payload={"html": "<html></html>"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={"seeded": True},
            content_type=ContentType.PAPER,
        )
        item_id = item.id

    ignored = authenticated_client.post(f"/api/items/{item_id}/ignore-similar")
    assert ignored.status_code == 200

    profile = authenticated_client.get("/api/profile")
    assert profile.status_code == 200
    assert "[2603.24579v1] march multi-agent" not in profile.json()["ignored_topics"]
    assert "march multi-agent reinforced self-check" in profile.json()["ignored_topics"]


def test_archived_items_drop_out_of_default_inbox_but_remain_in_archived_filter(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    assert created.status_code == 201
    item_id = created.json()["id"]

    archived = authenticated_client.post(f"/api/items/{item_id}/archive")
    assert archived.status_code == 200

    default_listing = authenticated_client.get("/api/items")
    assert default_listing.status_code == 200
    assert default_listing.json() == []

    archived_listing = authenticated_client.get("/api/items", params={"status": "archived"})
    assert archived_listing.status_code == 200
    assert len(archived_listing.json()) == 1
    assert archived_listing.json()[0]["id"] == item_id


def test_items_list_can_filter_by_source_id(authenticated_client: TestClient) -> None:
    with get_session_factory()() as db:
        first_source = Source(
            type=SourceType.RSS,
            name="Frontier Feed",
            url="https://example.com/frontier.xml",
            priority=90,
            active=True,
            tags=["rss"],
        )
        second_source = Source(
            type=SourceType.ARXIV,
            name="Paper Feed",
            url="https://export.arxiv.org/api/query?search_query=cat:cs.AI",
            priority=95,
            active=True,
            tags=["arxiv"],
        )
        db.add(first_source)
        db.add(second_source)
        db.commit()
        db.refresh(first_source)
        db.refresh(second_source)

        db.add(
            Item(
                source_id=first_source.id,
                title="Frontier article",
                source_name=first_source.name,
                authors=["Reporter"],
                canonical_url="https://example.com/frontier-article",
                content_type=ContentType.ARTICLE,
                content_hash="frontier-article",
            )
        )
        db.add(
            Item(
                source_id=second_source.id,
                title="Paper article",
                source_name=second_source.name,
                authors=["Researcher"],
                canonical_url="https://example.com/paper-article",
                content_type=ContentType.PAPER,
                content_hash="paper-article",
            )
        )
        db.commit()

    response = authenticated_client.get("/api/items", params={"source_id": first_source.id})
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["title"] == "Frontier article"
    assert payload[0]["source_name"] == "Frontier Feed"


def test_item_detail_returns_summary_and_records_open_action(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    assert created.status_code == 201
    item_id = created.json()["id"]

    listing = authenticated_client.get("/api/items")
    assert listing.status_code == 200
    assert listing.json()[0]["short_summary"]

    detail = authenticated_client.get(f"/api/items/{item_id}")
    assert detail.status_code == 200
    assert detail.json()["insight"]["short_summary"]
    assert detail.json()["insight"]["why_it_matters"] == "evaluation discipline, verifier routing, ranking transparency"

    with get_session_factory()() as db:
        actions = list(
            db.scalars(
                select(UserAction).where(
                    UserAction.item_id == item_id,
                    UserAction.action_type == ActionType.OPENED,
                )
            ).all()
        )
    assert len(actions) == 1


def test_items_list_respects_profile_data_mode(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Seed Feed",
            url="https://example.com/seed.xml",
            priority=80,
            active=True,
            tags=["seed"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        seeded_item = IngestionService(db).ingest_payload(
            source=source,
            title="Seed-only demo item",
            canonical_url="https://example.com/seed-item",
            authors=["Demo Author"],
            published_at=datetime(2026, 3, 24, 8, 0, 0, tzinfo=UTC),
            cleaned_text="Seeded content about research workflows.",
            raw_payload={"seeded": True},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={"seeded": True},
            content_type=ContentType.ARTICLE,
        )

    imported = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    assert imported.status_code == 201
    live_item_id = imported.json()["id"]

    seed_mode = authenticated_client.patch("/api/profile", json={"data_mode": "seed"})
    assert seed_mode.status_code == 200
    seed_items = authenticated_client.get("/api/items")
    assert seed_items.status_code == 200
    assert [item["id"] for item in seed_items.json()] == [seeded_item.id]

    live_mode = authenticated_client.patch("/api/profile", json={"data_mode": "live"})
    assert live_mode.status_code == 200
    live_items = authenticated_client.get("/api/items")
    assert live_items.status_code == 200
    assert [item["id"] for item in live_items.json()] == [live_item_id]


def test_save_to_zotero_without_connection_moves_item_to_review(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    item_id = created.json()["id"]

    exported = authenticated_client.post(f"/api/items/{item_id}/save-to-zotero", json={"tags": ["paper"]})
    assert exported.status_code == 200
    assert exported.json()["triage_status"] == "needs_review"


def test_save_to_zotero_network_failure_moves_item_to_review(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )
    authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "12345"},
            "metadata_json": {"library_type": "users"},
        },
    )
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    item_id = created.json()["id"]

    def _raise(*args, **kwargs):
        raise RuntimeError("zotero unavailable")

    monkeypatch.setattr("app.integrations.zotero.ZoteroClient.save_item", _raise)

    exported = authenticated_client.post(f"/api/items/{item_id}/save-to-zotero", json={"tags": ["paper"]})
    assert exported.status_code == 200
    assert exported.json()["triage_status"] == "needs_review"
    assert "Needs Review" in exported.json()["detail"]


def test_save_to_zotero_success_marks_item_saved(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )
    authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "12345"},
            "metadata_json": {"library_type": "users"},
        },
    )
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    item_id = created.json()["id"]

    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.save_item",
        lambda self, **kwargs: ZoteroExportResult(
            success=True,
            confidence_score=0.92,
            detail="Saved to Zotero.",
            response_payload={"successful": {"0": "ABCD1234"}},
        ),
    )

    exported = authenticated_client.post(
        f"/api/items/{item_id}/save-to-zotero",
        json={"tags": ["paper"], "note_prefix": "Research Center"},
    )
    assert exported.status_code == 200
    assert exported.json()["triage_status"] == "saved"
    assert exported.json()["detail"] == "Saved to Zotero."


def test_save_to_zotero_uses_configured_collection_name(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )
    authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "12345"},
            "metadata_json": {"library_type": "users", "collection_name": "Research Center / Papers"},
        },
    )
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    item_id = created.json()["id"]

    captured: dict[str, str | None] = {"collection_name": None}

    def _save(self, **kwargs):
        captured["collection_name"] = kwargs.get("collection_name")
        return ZoteroExportResult(
            success=True,
            confidence_score=0.92,
            detail="Saved to Zotero.",
            response_payload={"successful": {"0": "ABCD1234"}},
        )

    monkeypatch.setattr("app.integrations.zotero.ZoteroClient.save_item", _save)

    exported = authenticated_client.post(f"/api/items/{item_id}/save-to-zotero", json={"tags": ["paper"]})
    assert exported.status_code == 200
    assert exported.json()["triage_status"] == "saved"
    assert captured["collection_name"] == "Research Center / Papers"


def test_save_to_zotero_merges_manual_and_auto_tags(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )
    authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "12345"},
            "metadata_json": {
                "library_type": "users",
                "auto_tag_vocabulary": ["area/agents", "method/tool_use", "type/framework"],
            },
        },
    )
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    item_id = created.json()["id"]

    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.suggest_zotero_tags_with_usage",
        lambda self, item, text, allowed_tags, **kwargs: (
            ["area/agents", "method/tool_use"],
            None,
        ),
    )

    captured: dict[str, list[str] | None] = {"tags": None}

    def _save(self, **kwargs):
        captured["tags"] = kwargs.get("tags")
        return ZoteroExportResult(
            success=True,
            confidence_score=0.92,
            detail="Saved to Zotero.",
            response_payload={"successful": {"0": "ABCD1234"}},
        )

    monkeypatch.setattr("app.integrations.zotero.ZoteroClient.save_item", _save)

    exported = authenticated_client.post(f"/api/items/{item_id}/save-to-zotero", json={"tags": ["research-center"]})
    assert exported.status_code == 200
    assert exported.json()["triage_status"] == "saved"
    assert captured["tags"] == ["research-center", "area/agents", "method/tool_use"]


def test_save_to_zotero_uses_default_auto_tag_vocabulary_when_not_configured(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )
    authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "12345"},
            "metadata_json": {"library_type": "users"},
        },
    )
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    item_id = created.json()["id"]

    captured: dict[str, list[str] | None] = {"allowed_tags": None}

    def _suggest(self, item, text, allowed_tags, **kwargs):
        captured["allowed_tags"] = allowed_tags
        return ["area/llm"], None

    monkeypatch.setattr("app.integrations.llm.LLMClient.suggest_zotero_tags_with_usage", _suggest)
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.save_item",
        lambda self, **kwargs: ZoteroExportResult(
            success=True,
            confidence_score=0.92,
            detail="Saved to Zotero.",
            response_payload={"successful": {"0": "ABCD1234"}},
        ),
    )

    exported = authenticated_client.post(f"/api/items/{item_id}/save-to-zotero", json={"tags": ["research-center"]})
    assert exported.status_code == 200
    assert exported.json()["triage_status"] == "saved"
    assert captured["allowed_tags"] is not None
    assert "area/llm" in captured["allowed_tags"]
    assert "hook/replication_candidate" in captured["allowed_tags"]


def test_item_detail_records_insight_generation_operation_history(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.summarize_item",
        lambda self, item, text: {
            "short_summary": "A concise summary.",
            "why_it_matters": "Why this matters.",
            "whats_new": "What changed.",
            "caveats": "Check the source.",
            "follow_up_questions": [],
            "contribution": None,
            "method": None,
            "result": None,
            "limitation": None,
            "possible_extension": None,
            "_usage": {"prompt_tokens": 240, "completion_tokens": 60, "total_tokens": 300},
        },
    )
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    item_id = created.json()["id"]

    with get_session_factory()() as db:
        item = db.scalar(select(Item).where(Item.id == item_id))
        assert item is not None
        assert item.insight is not None
        item.insight.short_summary = None
        db.add(item.insight)
        db.commit()

    detail = authenticated_client.get(f"/api/items/{item_id}")
    assert detail.status_code == 200
    assert detail.json()["insight"]["short_summary"] == "A concise summary."

    history = authenticated_client.get("/api/ops/ingestion-runs")
    assert history.status_code == 200
    entry = next(item for item in history.json() if item["operation_kind"] == "item_insight_generation")

    assert entry["status"] == "succeeded"
    assert entry["affected_edition_days"] == [_expected_manual_import_edition_day()]
    assert entry["ai_total_tokens"] == 300
    assert entry["ai_cost_usd"] > 0
    assert entry["tts_cost_usd"] == 0
    assert any(info["label"] == "Item" and info["value"] == "Manual import item" for info in entry["basic_info"])


def test_save_to_zotero_records_operation_history_with_llm_usage(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )
    authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "12345"},
            "metadata_json": {
                "library_type": "users",
                "auto_tag_vocabulary": ["area/agents", "method/tool_use", "type/framework"],
            },
        },
    )
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.summarize_item",
        lambda self, item, text: {
            "short_summary": "A concise summary.",
            "why_it_matters": "Why this matters.",
            "whats_new": "What changed.",
            "caveats": "Check the source.",
            "follow_up_questions": [],
            "contribution": None,
            "method": None,
            "result": None,
            "limitation": None,
            "possible_extension": None,
            "_usage": {"prompt_tokens": 320, "completion_tokens": 80, "total_tokens": 400},
        },
    )
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.suggest_zotero_tags_with_usage",
        lambda self, item, text, allowed_tags, **kwargs: (
            ["area/agents", "method/tool_use"],
            {"prompt_tokens": 150, "completion_tokens": 50, "total_tokens": 200},
        ),
    )
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.save_item",
        lambda self, **kwargs: ZoteroExportResult(
            success=True,
            confidence_score=0.92,
            detail="Saved to Zotero.",
            response_payload={"successful": {"0": "ABCD1234"}},
        ),
    )
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    item_id = created.json()["id"]

    with get_session_factory()() as db:
        item = db.scalar(select(Item).where(Item.id == item_id))
        assert item is not None
        assert item.insight is not None
        item.insight.short_summary = None
        db.add(item.insight)
        db.commit()

    exported = authenticated_client.post(f"/api/items/{item_id}/save-to-zotero", json={"tags": ["research-center"]})
    assert exported.status_code == 200
    assert exported.json()["triage_status"] == "saved"

    history = authenticated_client.get("/api/ops/ingestion-runs")
    assert history.status_code == 200
    entry = next(item for item in history.json() if item["operation_kind"] == "zotero_export")

    assert entry["status"] == "succeeded"
    assert entry["affected_edition_days"] == [_expected_manual_import_edition_day()]
    assert entry["ai_total_tokens"] == 600
    assert entry["ai_cost_usd"] > 0
    assert any(info["label"] == "Applied tags" and info["value"] == "3" for info in entry["basic_info"])


def test_generate_deeper_summary_records_operation_history(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.deepen_item",
        lambda self, item, text: {
            "deeper_summary": "A more detailed summary.",
            "experiment_ideas": ["Try a stronger baseline.", "Stress-test the retrieval pipeline."],
            "_usage": {"prompt_tokens": 210, "completion_tokens": 90, "total_tokens": 300},
        },
    )
    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    item_id = created.json()["id"]

    queued = authenticated_client.post(f"/api/items/{item_id}/generate-deeper-summary")
    assert queued.status_code == 200
    assert queued.json()["queued"] is True

    with get_session_factory()() as db:
        item = db.scalar(select(Item).where(Item.id == item_id))
        assert item is not None
        assert item.insight is not None
        assert item.insight.deeper_summary == "A more detailed summary."

    history = authenticated_client.get("/api/ops/ingestion-runs")
    assert history.status_code == 200
    entry = next(item for item in history.json() if item["operation_kind"] == "deeper_summary_generation")

    assert entry["status"] == "succeeded"
    assert entry["affected_edition_days"] == [_expected_manual_import_edition_day()]
    assert entry["ai_total_tokens"] == 300
    assert any(info["label"] == "Experiment ideas" and info["value"] == "2" for info in entry["basic_info"])
