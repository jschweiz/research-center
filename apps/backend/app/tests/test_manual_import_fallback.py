from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.models import ContentType, Item, Source, SourceType
from app.db.session import get_session_factory
from app.services.ingestion import IngestionService


def test_manual_import_uses_existing_item_when_extractor_fails(
    authenticated_client,
    monkeypatch,
) -> None:
    canonical_url = "https://openai.com/index/our-approach-to-the-model-spec"
    original_published_at = datetime(2026, 3, 25, 10, 0, 0, tzinfo=UTC)

    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="OpenAI News",
            url="https://openai.com/news/rss.xml",
            priority=90,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        IngestionService(db).ingest_payload(
            source=source,
            title="Inside our approach to the Model Spec",
            canonical_url=canonical_url,
            authors=["OpenAI"],
            published_at=original_published_at,
            cleaned_text="An article about the current Model Spec.",
            raw_payload={"html": "<html></html>"},
            outbound_links=[],
            extraction_confidence=0.8,
            metadata_json={"mime_type": "text/html"},
            content_type=ContentType.ARTICLE,
        )

    def _raise(self, url: str):
        raise RuntimeError("403 Forbidden")

    monkeypatch.setattr("app.integrations.extractors.ContentExtractor.extract_from_url", _raise)

    imported = authenticated_client.post("/api/items/import-url", json={"url": canonical_url})
    assert imported.status_code == 201
    payload = imported.json()
    assert payload["title"] == "Inside our approach to the Model Spec"
    assert payload["published_at"] == "2026-03-25T10:00:00+00:00"
    assert payload["cleaned_text"] == "An article about the current Model Spec."


def test_manual_import_creates_placeholder_when_extractor_fails_for_new_url(
    authenticated_client,
    monkeypatch,
) -> None:
    def _raise(self, url: str):
        raise RuntimeError("403 Forbidden")

    monkeypatch.setattr("app.integrations.extractors.ContentExtractor.extract_from_url", _raise)

    imported = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/protected-article"})
    assert imported.status_code == 201
    payload = imported.json()
    assert payload["title"] == "Protected Article"
    assert "could not extract the full text" in payload["cleaned_text"]
    assert payload["published_at"] is None

    with get_session_factory()() as db:
        stored = db.scalar(
            select(Item)
            .options(selectinload(Item.content))
            .where(Item.id == payload["id"])
        )
        assert stored is not None
        assert stored.content is not None
        assert stored.content.raw_payload["fallback"] == "placeholder"


def test_manual_import_rejects_private_network_targets(authenticated_client) -> None:
    imported = authenticated_client.post(
        "/api/items/import-url",
        json={"url": "http://127.0.0.1/internal"},
    )

    assert imported.status_code == 400
    assert imported.json()["detail"] == "Outbound URLs must not target a private or local network address."
