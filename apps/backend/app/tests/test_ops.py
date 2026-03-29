from datetime import UTC, date, datetime

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from app.db.models import (
    ActionType,
    ContentType,
    DataMode,
    Digest,
    DigestEntry,
    DigestSection,
    IngestionRun,
    IngestionRunType,
    Item,
    ItemCluster,
    ItemContent,
    ItemInsight,
    ItemMention,
    ItemScore,
    RunStatus,
    Source,
    SourceType,
    UserAction,
    ZoteroExport,
    ZoteroMatch,
)
from app.db.session import get_session_factory
from app.services.ingestion import IngestionService


def test_clear_content_endpoint_removes_item_records_but_keeps_sources(
    authenticated_client: TestClient,
) -> None:
    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Resettable Feed",
            url="https://example.com/feed.xml",
            priority=80,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        item = IngestionService(db).ingest_payload(
            source=source,
            title="Stored article slated for deletion",
            canonical_url="https://example.com/stored-article",
            authors=["Research Desk"],
            published_at=datetime(2026, 3, 26, 8, 0, tzinfo=UTC),
            cleaned_text=(
                "A stored article about clearing item-derived records without "
                "touching source configuration."
            ),
            raw_payload={"html": "<html></html>"},
            outbound_links=["https://example.com/reference"],
            extraction_confidence=0.93,
            metadata_json={},
            content_type=ContentType.ARTICLE,
            insight_payload={
                "short_summary": "A stored article about clearing item-derived records.",
                "why_it_matters": (
                    "The reset needs to preserve sources while removing saved content."
                ),
            },
        )

        db.add(UserAction(item_id=item.id, action_type=ActionType.OPENED, metadata_json={}))
        db.add(
            ZoteroMatch(
                item_id=item.id,
                library_item_key="LIB-123",
                title=item.title,
                similarity_score=0.88,
                metadata_json={},
            )
        )
        db.add(
            ZoteroExport(
                item_id=item.id,
                status=RunStatus.SUCCEEDED,
                confidence_score=0.91,
                response_payload={"item_key": "LIB-123"},
            )
        )
        digest = Digest(
            brief_date=date(2026, 3, 27),
            data_mode=DataMode.LIVE,
            title="Test Digest",
        )
        db.add(digest)
        db.flush()
        db.add(
            DigestEntry(
                digest_id=digest.id,
                item_id=item.id,
                section=DigestSection.HEADLINES,
                rank=1,
                note="Lead item before reset.",
            )
        )
        history_service = IngestionService(db)
        operation_run = history_service.start_operation_run(
            run_type=IngestionRunType.DIGEST,
            operation_kind="brief_generation",
            trigger="manual_test",
            metadata={
                "title": "Manual history entry",
                "summary": "A prior operation kept in history.",
            },
        )
        history_service.finalize_operation_run(
            operation_run,
            status=RunStatus.SUCCEEDED,
        )
        db.commit()

        assert db.scalar(select(func.count()).select_from(Source)) == 1
        assert db.scalar(select(func.count()).select_from(Item)) == 1
        assert db.scalar(select(func.count()).select_from(ItemContent)) == 1
        assert db.scalar(select(func.count()).select_from(ItemScore)) == 1
        assert db.scalar(select(func.count()).select_from(ItemInsight)) == 1
        assert db.scalar(select(func.count()).select_from(ItemMention)) == 1
        assert db.scalar(select(func.count()).select_from(ItemCluster)) == 1
        assert db.scalar(select(func.count()).select_from(UserAction)) == 1
        assert db.scalar(select(func.count()).select_from(ZoteroMatch)) == 1
        assert db.scalar(select(func.count()).select_from(ZoteroExport)) == 1
        assert db.scalar(select(func.count()).select_from(Digest)) == 1
        assert db.scalar(select(func.count()).select_from(DigestEntry)) == 1
        assert db.scalar(select(func.count()).select_from(IngestionRun)) == 1

    cleared = authenticated_client.post("/api/ops/clear-content")
    assert cleared.status_code == 200
    payload = cleared.json()
    assert payload["queued"] is False
    assert payload["task_name"] == "clear_content"
    assert payload["operation_run_id"] is None
    assert (
        payload["detail"]
        == "Cleared 1 stored item and reset 1 generated brief. "
        "Removed 1 operation history. Sources, connections, and profile settings "
        "were left untouched."
    )

    with get_session_factory()() as db:
        assert db.scalar(select(func.count()).select_from(Source)) == 1
        assert db.scalar(select(func.count()).select_from(Item)) == 0
        assert db.scalar(select(func.count()).select_from(ItemContent)) == 0
        assert db.scalar(select(func.count()).select_from(ItemScore)) == 0
        assert db.scalar(select(func.count()).select_from(ItemInsight)) == 0
        assert db.scalar(select(func.count()).select_from(ItemMention)) == 0
        assert db.scalar(select(func.count()).select_from(ItemCluster)) == 0
        assert db.scalar(select(func.count()).select_from(UserAction)) == 0
        assert db.scalar(select(func.count()).select_from(ZoteroMatch)) == 0
        assert db.scalar(select(func.count()).select_from(ZoteroExport)) == 0
        assert db.scalar(select(func.count()).select_from(Digest)) == 0
        assert db.scalar(select(func.count()).select_from(DigestEntry)) == 0
        assert db.scalar(select(func.count()).select_from(IngestionRun)) == 0

    items = authenticated_client.get("/api/items")
    assert items.status_code == 200
    assert items.json() == []

    profile = authenticated_client.get("/api/profile")
    assert profile.status_code == 200

    history = authenticated_client.get("/api/ops/ingestion-runs")
    assert history.status_code == 200
    assert history.json() == []


def test_api_responses_disable_http_caching(authenticated_client: TestClient) -> None:
    history = authenticated_client.get("/api/ops/ingestion-runs")

    assert history.status_code == 200
    assert history.headers["cache-control"] == "no-store"
    assert history.headers["pragma"] == "no-cache"
    assert history.headers["expires"] == "0"
