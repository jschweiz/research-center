from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.db.models import IngestionRun, Item, RunStatus, ScoreBucket, Source, SourceType
from app.db.session import get_session_factory
from app.integrations.extractors import ExtractedContent
from app.services.briefs import BriefService
from app.services.ingestion import IngestionService
from app.services.item_enrichment import ENRICHMENT_METADATA_KEY, ItemEnrichmentService


def _seed_item(
    db,
    *,
    source: Source,
    index: int,
    title: str,
    authors: list[str],
    canonical_url: str | None = None,
) -> Item:
    return IngestionService(db).ingest_payload(
        source=source,
        title=title,
        canonical_url=canonical_url or f"https://example.com/items/{index}",
        authors=authors,
        published_at=datetime(2026, 3, 20, 8, 0, tzinfo=UTC),
        cleaned_text="A generic systems note with little direct profile overlap.",
        raw_payload={"index": index},
        outbound_links=[],
        extraction_confidence=0.9,
        metadata_json={},
    )


def test_item_enrichment_service_batches_and_persists_metadata(
    client: TestClient,
    monkeypatch,
) -> None:
    batch_sizes: list[int] = []

    def _fake_batch(self, items, profile):
        batch_sizes.append(len(items))
        return {
            "_usage": {
                "prompt_tokens": len(items) * 10,
                "completion_tokens": len(items) * 2,
                "total_tokens": len(items) * 12,
            },
            "items": [
                {
                    "item_id": item["item_id"],
                    "relevance_score": 0.9,
                    "reason": "Matches the user profile better than the heuristic score suggests.",
                    "tags": ["agents", "triage"],
                    "authors": ["Alex Researcher"],
                }
                for item in items
            ],
        }

    monkeypatch.setattr(ItemEnrichmentService, "_gemini_configured", lambda self: True)
    monkeypatch.setattr("app.services.ingestion.LLMClient.batch_enrich_items", _fake_batch)

    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Batch Feed",
            url="https://example.com/feed.xml",
            priority=50,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        preserved_item = _seed_item(
            db,
            source=source,
            index=0,
            title="Existing author item",
            authors=["Existing Writer"],
        )
        filled_item = _seed_item(
            db,
            source=source,
            index=1,
            title="Empty author item",
            authors=[],
        )
        for index in range(2, 23):
            _seed_item(
                db,
                source=source,
                index=index,
                title=f"Generic item {index}",
                authors=[],
            )
        db.commit()

        assert preserved_item.score is not None
        assert preserved_item.score.bucket == ScoreBucket.ARCHIVE

        result = ItemEnrichmentService(db).enrich_all_items()
        db.expire_all()

        reloaded_preserved = db.get(Item, preserved_item.id)
        reloaded_filled = db.get(Item, filled_item.id)
        assert reloaded_preserved is not None
        assert reloaded_filled is not None

        assert batch_sizes == [10, 10, 3]
        assert result.status == RunStatus.SUCCEEDED
        assert result.updated_count == 23
        assert result.author_fill_count == 22
        assert result.operation_run_id is not None

        preserved_enrichment = reloaded_preserved.metadata_json[ENRICHMENT_METADATA_KEY]
        filled_enrichment = reloaded_filled.metadata_json[ENRICHMENT_METADATA_KEY]

        assert reloaded_preserved.authors == ["Existing Writer"]
        assert preserved_enrichment["author_applied"] is False
        assert preserved_enrichment["suggested_authors"] == ["Alex Researcher"]
        assert reloaded_filled.authors == ["Alex Researcher"]
        assert filled_enrichment["author_applied"] is True
        assert filled_enrichment["relevance_score"] == 0.9
        assert filled_enrichment["tags"] == ["agents", "triage"]
        assert filled_enrichment["operation_run_id"] == result.operation_run_id
        assert reloaded_filled.score is not None
        assert reloaded_filled.score.relevance_score == 0.9
        assert reloaded_filled.score.bucket == ScoreBucket.WORTH_A_SKIM
        assert reloaded_filled.score.reason_trace["scoring_mode"] == "llm"

        operation_run = db.get(IngestionRun, result.operation_run_id)
        assert operation_run is not None
        assert operation_run.status == RunStatus.SUCCEEDED


def test_item_enrichment_service_keeps_heuristic_scores_when_batch_fails(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(ItemEnrichmentService, "_gemini_configured", lambda self: True)
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.batch_enrich_items",
        lambda self, items, profile: (_ for _ in ()).throw(RuntimeError("bad batch")),
    )

    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Failure Feed",
            url="https://example.com/failure.xml",
            priority=50,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        item = _seed_item(
            db,
            source=source,
            index=0,
            title="Failure item",
            authors=[],
            canonical_url="https://example.com/failure-item",
        )
        db.commit()
        original_total = item.score.total_score if item.score else 0.0
        original_bucket = item.score.bucket if item.score else ScoreBucket.ARCHIVE

        result = ItemEnrichmentService(db).enrich_item_ids([item.id])
        db.expire_all()

        reloaded = db.get(Item, item.id)
        assert reloaded is not None
        assert result.status == RunStatus.FAILED
        assert result.failed_batch_count == 1
        assert result.updated_count == 0
        assert ENRICHMENT_METADATA_KEY not in reloaded.metadata_json
        assert reloaded.score is not None
        assert reloaded.score.total_score == original_total
        assert reloaded.score.bucket == original_bucket


def test_manual_enrichment_backfill_endpoint_runs_and_refreshes_current_digest(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    refresh_triggers: list[str] = []

    monkeypatch.setattr(ItemEnrichmentService, "_gemini_configured", lambda self: True)
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.batch_enrich_items",
        lambda self, items, profile: {
            "_usage": {"prompt_tokens": 40, "completion_tokens": 12, "total_tokens": 52},
            "items": [
                {
                    "item_id": item["item_id"],
                    "relevance_score": 0.8,
                    "reason": "Strong user-profile fit.",
                    "tags": ["triage"],
                    "authors": ["Alex Researcher"],
                }
                for item in items
            ],
        },
    )

    original_refresh = BriefService.refresh_current_edition_day

    def _record_refresh(self, *, data_mode=None, trigger="ingest_refresh"):
        refresh_triggers.append(trigger)
        return original_refresh(self, data_mode=data_mode, trigger=trigger)

    monkeypatch.setattr(BriefService, "refresh_current_edition_day", _record_refresh)

    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Backfill Feed",
            url="https://example.com/backfill.xml",
            priority=50,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)
        item = _seed_item(
            db,
            source=source,
            index=0,
            title="Backfill item",
            authors=[],
            canonical_url="https://example.com/backfill-item",
        )
        db.commit()
        item_id = item.id

    response = authenticated_client.post("/api/ops/enrich-all")
    assert response.status_code == 200
    payload = response.json()
    assert payload["queued"] is True
    assert payload["task_name"] == "enrich_all"
    assert payload["operation_run_id"] is not None

    with get_session_factory()() as db:
        reloaded = db.get(Item, item_id)
        operation_run = db.get(IngestionRun, payload["operation_run_id"])
        assert reloaded is not None
        assert reloaded.metadata_json[ENRICHMENT_METADATA_KEY]["relevance_score"] == 0.8
        assert operation_run is not None
        assert operation_run.status == RunStatus.SUCCEEDED
        assert operation_run.metadata_json["operation_kind"] == "corpus_enrichment_backfill"

    assert refresh_triggers == ["enrichment_refresh"]


def test_post_ingest_enrichment_reorders_current_digest(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    with get_session_factory()() as db:
        current_edition = BriefService(db).current_edition_date()
    coverage_day = current_edition - timedelta(days=1)

    def _published_timestamp(hour: int) -> datetime:
        return datetime(coverage_day.year, coverage_day.month, coverage_day.day, hour, 0, tzinfo=UTC)

    def _feed_published(hour: int) -> str:
        return _published_timestamp(hour).strftime("%a, %d %b %Y %H:%M:%S +0000")

    def _fake_load_feed(self, locator):
        return SimpleNamespace(
            feed={"title": "Digest Feed"},
            entries=[
                SimpleNamespace(
                    title="Verifier routing field notes",
                    link="https://example.com/verifier-routing",
                    published=_feed_published(8),
                    summary="Detailed notes about verifier routing for research triage.",
                    authors=[],
                ),
                SimpleNamespace(
                    title="Generic systems note",
                    link="https://example.com/generic-systems",
                    published=_feed_published(9),
                    summary="General infrastructure maintenance notes.",
                    authors=[],
                ),
            ],
            bozo=False,
        )

    def _fake_extract(self, url: str) -> ExtractedContent:
        if "verifier-routing" in url:
            title = "Verifier routing field notes"
            text = "A close read on verifier routing and research triage."
            published_at = _published_timestamp(8)
        else:
            title = "Generic systems note"
            text = "A broad systems update with little direct user-profile overlap."
            published_at = _published_timestamp(9)
        return ExtractedContent(
            title=title,
            cleaned_text=text,
            outbound_links=[],
            published_at=published_at,
            mime_type="text/html",
            extraction_confidence=0.93,
            raw_payload={"fetched_url": url},
        )

    def _fake_batch(self, items, profile):
        return {
            "_usage": {"prompt_tokens": 50, "completion_tokens": 10, "total_tokens": 60},
            "items": [
                {
                    "item_id": item["item_id"],
                    "relevance_score": 0.95 if item["title"] == "Verifier routing field notes" else 0.05,
                    "reason": "Strong profile fit." if item["title"] == "Verifier routing field notes" else "Weak profile fit.",
                    "tags": ["routing"] if item["title"] == "Verifier routing field notes" else ["systems"],
                    "authors": [],
                }
                for item in items
            ],
        }

    monkeypatch.setattr("app.services.ingestion.IngestionService._load_feed", _fake_load_feed)
    monkeypatch.setattr(
        "app.integrations.extractors.ContentExtractor.extract_from_url",
        _fake_extract,
    )
    monkeypatch.setattr(ItemEnrichmentService, "_gemini_configured", lambda self: True)
    monkeypatch.setattr("app.services.ingestion.LLMClient.batch_enrich_items", _fake_batch)

    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Digest Feed",
            url="https://example.com/digest.xml",
            priority=88,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()

    ingest = authenticated_client.post("/api/ops/ingest-now")
    assert ingest.status_code == 200

    digest = authenticated_client.get("/api/briefs/today")
    assert digest.status_code == 200
    digest_payload = digest.json()
    ordered_titles = [
        entry["item"]["title"]
        for section in (
            digest_payload["editorial_shortlist"],
            digest_payload["headlines"],
            digest_payload["interesting_side_signals"],
            digest_payload["remaining_reads"],
        )
        for entry in section
    ]
    assert "Verifier routing field notes" in ordered_titles
    assert "Generic systems note" in ordered_titles
    assert ordered_titles.index("Verifier routing field notes") < ordered_titles.index("Generic systems note")

    history = authenticated_client.get("/api/ops/ingestion-runs")
    assert history.status_code == 200
    assert any(entry["operation_kind"] == "post_ingest_enrichment" for entry in history.json())
