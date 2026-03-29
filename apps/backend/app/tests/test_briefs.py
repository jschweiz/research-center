from datetime import UTC, date, datetime, timedelta

import httpx
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.db.models import (
    ContentType,
    DataMode,
    Digest,
    DigestEntry,
    DigestSection,
    Item,
    ItemContent,
    ItemInsight,
    ItemScore,
    RunStatus,
    ScoreBucket,
    Source,
    SourceType,
    ZoteroMatch,
)
from app.db.session import get_session_factory
from app.services.briefs import BriefService
from app.services.ingestion import IngestionRunItemSummary, IngestionService, SourceRunSummary
from app.services.presenters import compute_paper_credibility_score

SECTION_NAMES = ("headlines", "editorial_shortlist", "interesting_side_signals", "remaining_reads")
DISPLAY_SECTION_NAMES = (*SECTION_NAMES, "papers_table")


def _timestamp_for_day(day: date, *, hour: int = 12, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


def _digest_item_ids(payload: dict) -> set[str]:
    return {
        entry["item"]["id"]
        for section in DISPLAY_SECTION_NAMES
        for entry in payload[section]
    }


def _digest_item_titles(payload: dict) -> set[str]:
    return {
        entry["item"]["title"]
        for section in DISPLAY_SECTION_NAMES
        for entry in payload[section]
    }


def test_digest_generation_with_seeded_item(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    regenerate = authenticated_client.post("/api/ops/regenerate-brief")
    assert regenerate.status_code == 200

    digest = authenticated_client.get("/api/briefs/today")
    assert digest.status_code == 200
    payload = digest.json()
    assert "headlines" in payload
    assert "editorial_shortlist" in payload
    assert "interesting_side_signals" in payload
    assert "remaining_reads" in payload
    assert "papers_table" in payload
    assert "top_items" not in payload
    assert "notable_papers" not in payload
    assert "worth_a_skim" not in payload
    assert "interesting_signals" not in payload
    assert payload["title"].startswith("Morning Brief")
    assert len(_digest_item_ids(payload)) >= 1


def test_digest_editorial_note_is_two_sentence_item_summary(client: TestClient) -> None:
    with get_session_factory()() as db:
        brief_date = BriefService(db).current_edition_date()
        coverage_day = brief_date - timedelta(days=1)
        source = Source(
            type=SourceType.RSS,
            name="Overview Feed",
            url="https://example.com/overview.xml",
            priority=90,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        first = IngestionService(db).ingest_payload(
            source=source,
            title="Latency-focused model release",
            canonical_url="https://example.com/model-release",
            authors=["Researcher"],
            published_at=_timestamp_for_day(coverage_day, hour=8),
            cleaned_text="A model release focuses on lower-latency tool use.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
            insight_payload={
                "short_summary": "A model release focuses on lower-latency tool use.",
                "why_it_matters": "Lower-latency tool use could change how often agents are practical in production.",
            },
        )
        second = IngestionService(db).ingest_payload(
            source=source,
            title="Verifier routing paper",
            canonical_url="https://example.com/verifier-routing-paper",
            authors=["Researcher"],
            published_at=_timestamp_for_day(coverage_day, hour=9),
            cleaned_text="A paper argues verifier routing improves research triage.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.PAPER,
            insight_payload={
                "short_summary": "A paper argues verifier routing improves research triage.",
                "why_it_matters": "Verifier routing could reduce wasted reading time.",
            },
        )
        assert first.score is not None
        assert second.score is not None
        first.score.total_score = 0.95
        first.score.bucket = ScoreBucket.MUST_READ
        second.score.total_score = 0.9
        second.score.bucket = ScoreBucket.MUST_READ
        db.add_all([first, second])
        db.commit()

        digest = BriefService(db).generate_digest(brief_date=brief_date, force=True)

        assert digest.editorial_note == (
            "This edition highlights verifier routing paper. "
            "The lead set combines 1 paper from 1 distinct source."
        )


def test_digest_generation_uses_editorial_note_llm_when_generating_summary(
    client: TestClient,
    monkeypatch,
) -> None:
    captured_digest: dict = {}

    def fake_compose_editorial_note(self, digest):
        captured_digest.clear()
        captured_digest.update(digest)
        return {
            "note": "Verifier routing and lower-latency tool releases set the tone today.",
            "_usage": {"prompt_tokens": 120, "completion_tokens": 24, "total_tokens": 144},
        }

    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.compose_editorial_note",
        fake_compose_editorial_note,
    )
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.compose_editorial_note",
        fake_compose_editorial_note,
    )

    with get_session_factory()() as db:
        brief_date = BriefService(db).current_edition_date()
        coverage_day = brief_date - timedelta(days=1)
        source = Source(
            type=SourceType.RSS,
            name="Editorial LLM Feed",
            url="https://example.com/editorial-llm.xml",
            priority=90,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        first = IngestionService(db).ingest_payload(
            source=source,
            title="Latency-focused model release",
            canonical_url="https://example.com/model-release",
            authors=["Researcher"],
            published_at=_timestamp_for_day(coverage_day, hour=8),
            cleaned_text="A model release focuses on lower-latency tool use.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
            insight_payload={
                "short_summary": "A model release focuses on lower-latency tool use.",
                "why_it_matters": "Lower-latency tool use could change how often agents are practical in production.",
            },
        )
        second = IngestionService(db).ingest_payload(
            source=source,
            title="Verifier routing paper",
            canonical_url="https://example.com/verifier-routing-paper",
            authors=["Researcher"],
            published_at=_timestamp_for_day(coverage_day, hour=9),
            cleaned_text="A paper argues verifier routing improves research triage.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.PAPER,
            insight_payload={
                "short_summary": "A paper argues verifier routing improves research triage.",
                "why_it_matters": "Verifier routing could reduce wasted reading time.",
            },
        )
        assert first.score is not None
        assert second.score is not None
        first.score.total_score = 0.95
        first.score.bucket = ScoreBucket.MUST_READ
        second.score.total_score = 0.9
        second.score.bucket = ScoreBucket.MUST_READ
        db.add_all([first, second])
        db.commit()

        digest = BriefService(db).generate_digest(brief_date=brief_date, force=True)

        assert digest.editorial_note == "Verifier routing and lower-latency tool releases set the tone today."
        assert captured_digest["brief_date"] == brief_date.isoformat()
        assert captured_digest["editorial_shortlist"]
        assert captured_digest["headlines"]


def test_ingest_refresh_preserves_existing_editorial_note(client: TestClient, monkeypatch) -> None:
    initial_note = "Verifier routing and lower-latency tool releases set the tone today."

    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.compose_editorial_note",
        lambda self, digest: {"note": initial_note},
    )
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.compose_editorial_note",
        lambda self, digest: {"note": initial_note},
    )

    with get_session_factory()() as db:
        brief_service = BriefService(db)
        brief_date = brief_service.current_edition_date()
        coverage_day = brief_date - timedelta(days=1)
        source = Source(
            type=SourceType.RSS,
            name="Preserve Feed",
            url="https://example.com/preserve.xml",
            priority=90,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        item = IngestionService(db).ingest_payload(
            source=source,
            title="Preserved editorial note item",
            canonical_url="https://example.com/preserve-item",
            authors=["Researcher"],
            published_at=_timestamp_for_day(coverage_day, hour=8),
            cleaned_text="A release about verifier routing and lower-latency tool use.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
            insight_payload={
                "short_summary": "A release about verifier routing and lower-latency tool use.",
                "why_it_matters": "It sets the tone for the rest of the day's coverage.",
            },
        )
        assert item.score is not None
        item.score.total_score = 0.95
        item.score.bucket = ScoreBucket.MUST_READ
        db.add(item)
        db.commit()

        digest = brief_service.generate_digest(brief_date=brief_date, force=True)
        assert digest.editorial_note == initial_note

        def fail_compose_editorial_note(self, digest):
            raise AssertionError("editorial note should be preserved during ingest refresh")

        monkeypatch.setattr(
            "app.integrations.llm.LLMClient.compose_editorial_note",
            fail_compose_editorial_note,
        )
        monkeypatch.setattr(
            "app.services.ingestion.LLMClient.compose_editorial_note",
            fail_compose_editorial_note,
        )

        brief_service.refresh_current_edition_day(trigger="ingest_refresh")
        refreshed = brief_service._get_digest_model_by_date(brief_date, data_mode=DataMode.LIVE)
        assert refreshed is not None
        assert refreshed.editorial_note == initial_note


def test_loading_digest_backfills_missing_editorial_note(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    backfilled_note = "Verifier routing and lower-latency tool releases set the tone today."

    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.compose_editorial_note",
        lambda self, digest: {"note": backfilled_note},
    )
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.compose_editorial_note",
        lambda self, digest: {"note": backfilled_note},
    )

    with get_session_factory()() as db:
        brief_service = BriefService(db)
        brief_date = brief_service.current_edition_date()
        coverage_day = brief_date - timedelta(days=1)
        source = Source(
            type=SourceType.RSS,
            name="Backfill Feed",
            url="https://example.com/backfill.xml",
            priority=90,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        item = IngestionService(db).ingest_payload(
            source=source,
            title="Backfilled editorial note item",
            canonical_url="https://example.com/backfill-item",
            authors=["Researcher"],
            published_at=_timestamp_for_day(coverage_day, hour=8),
            cleaned_text="A release about verifier routing and lower-latency tool use.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
            insight_payload={
                "short_summary": "A release about verifier routing and lower-latency tool use.",
                "why_it_matters": "It sets the tone for the rest of the day's coverage.",
            },
        )
        assert item.score is not None
        item.score.total_score = 0.95
        item.score.bucket = ScoreBucket.MUST_READ
        db.add(item)
        db.commit()

        digest = brief_service.generate_digest(
            brief_date=brief_date,
            force=True,
            trigger="ingest_refresh",
            editorial_note_mode="preserve",
        )
        assert digest.editorial_note is None

    response = authenticated_client.get(f"/api/briefs/{brief_date.isoformat()}")
    assert response.status_code == 200
    assert response.json()["editorial_note"] == backfilled_note

    with get_session_factory()() as db:
        digest = BriefService(db)._get_digest_model_by_date(brief_date, data_mode=DataMode.LIVE)
        assert digest is not None
        assert digest.editorial_note == backfilled_note


def test_digest_item_exposes_best_effort_organization_name(
    authenticated_client: TestClient,
) -> None:
    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Research Lab Feed",
            url="https://example.com/feed.xml",
            priority=90,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        IngestionService(db).ingest_payload(
            source=source,
            title="Affiliation-backed paper",
            canonical_url="https://example.com/affiliation-paper",
            authors=["Ada Lovelace"],
            published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
            cleaned_text="A paper about evaluation and agent reliability.",
            raw_payload={
                "html": """
                <html>
                  <head>
                    <meta name="citation_author_institution" content="ETH Zurich" />
                    <script type="application/ld+json">
                      {"publisher": {"name": "Example Press"}}
                    </script>
                  </head>
                </html>
                """
            },
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={"crossref_publisher": "Example Press"},
            content_type=ContentType.PAPER,
        )

    digest = authenticated_client.get("/api/briefs/2026-03-28")
    assert digest.status_code == 200

    payload = digest.json()
    organization_names = {
        entry["item"]["organization_name"]
        for section in DISPLAY_SECTION_NAMES
        for entry in payload[section]
    }
    assert "ETH Zurich" in organization_names


def test_ingest_now_refreshes_current_edition_day_only(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    with get_session_factory()() as db:
        current_edition = BriefService(db).current_edition_date()
        current_coverage = current_edition - timedelta(days=1)
        tomorrow_edition = current_edition + timedelta(days=1)
        source = Source(
            type=SourceType.RSS,
            name="Live RSS",
            url="https://example.com/feed.xml",
            priority=90,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        baseline = IngestionService(db).ingest_payload(
            source=source,
            title="Baseline digest item",
            canonical_url="https://example.com/baseline",
            authors=["Researcher"],
            published_at=_timestamp_for_day(current_coverage, hour=8),
            cleaned_text="Baseline content about research tooling and evaluation practice.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={"feed_title": "Live RSS"},
            content_type=ContentType.ARTICLE,
        )

    regenerate = authenticated_client.post("/api/ops/regenerate-brief")
    assert regenerate.status_code == 200

    digest_before = authenticated_client.get("/api/briefs/today")
    assert digest_before.status_code == 200
    before_payload = digest_before.json()
    assert baseline.id in _digest_item_ids(before_payload)

    def _ingest_feed_source(self, source: Source) -> SourceRunSummary:
        item = self.ingest_payload(
            source=source,
            title="Fresh ingest item",
            canonical_url="https://example.com/fresh-ingest",
            authors=["Researcher"],
            published_at=_timestamp_for_day(current_edition, hour=12),
            cleaned_text="Fresh content about verifier routing and newsletter signal ranking.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.95,
            metadata_json={"feed_title": "Live RSS"},
            content_type=ContentType.ARTICLE,
        )
        return SourceRunSummary(
            source_id=source.id,
            source_name=source.name,
            status=RunStatus.SUCCEEDED,
            affected_edition_days=[tomorrow_edition],
            items=[
                IngestionRunItemSummary(
                    title=item.title,
                    outcome="created",
                    content_type=item.content_type.value,
                    extraction_confidence=item.extraction_confidence,
                )
            ],
        )

    monkeypatch.setattr(
        "app.services.ingestion.IngestionService._ingest_feed_source",
        _ingest_feed_source,
    )

    ingest = authenticated_client.post("/api/ops/ingest-now")
    assert ingest.status_code == 200

    digest_today = authenticated_client.get("/api/briefs/today")
    assert digest_today.status_code == 200
    today_payload = digest_today.json()
    assert today_payload["id"] != before_payload["id"]
    assert "Fresh ingest item" not in _digest_item_titles(today_payload)

    with get_session_factory()() as db:
        tomorrow_digest = db.scalar(
            select(Digest).where(Digest.brief_date == tomorrow_edition)
        )
        assert tomorrow_digest is None

    history_response = authenticated_client.get("/api/ops/ingestion-runs")
    assert history_response.status_code == 200
    history = history_response.json()
    ingest_refresh_entries = [
        entry
        for entry in history
        if entry["operation_kind"] == "brief_generation"
        and entry["trigger"] == "ingest_refresh"
    ]
    assert len(ingest_refresh_entries) == 1
    assert ingest_refresh_entries[0]["affected_edition_days"] == [current_edition.isoformat()]


def test_digest_generation_compacts_verbose_why_it_matters_notes(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    created = authenticated_client.post(
        "/api/items/import-url", json={"url": "https://example.com/article"}
    )
    assert created.status_code == 201
    item_id = created.json()["id"]

    with get_session_factory()() as db:
        item = db.scalar(select(Item).where(Item.id == item_id))
        assert item is not None
        insight = item.insight or ItemInsight(item=item)
        insight.short_summary = "Evaluation discipline, verifier routing, and ranking transparency."
        insight.why_it_matters = (
            "This surfaced because it intersects with the current research profile and "
            "looks material enough to influence what to read next."
        )
        insight.follow_up_questions = []
        insight.experiment_ideas = []
        db.add(insight)
        db.commit()

    regenerate = authenticated_client.post("/api/ops/regenerate-brief")
    assert regenerate.status_code == 200

    digest = authenticated_client.get("/api/briefs/today")
    assert digest.status_code == 200
    note = digest.json()["editorial_shortlist"][0]["note"]
    assert note == "Evaluation discipline, verifier routing, ranking transparency"


def test_archived_items_are_hidden_from_existing_digest(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    created = authenticated_client.post(
        "/api/items/import-url", json={"url": "https://example.com/article"}
    )
    item_id = created.json()["id"]
    authenticated_client.post("/api/ops/regenerate-brief")

    archived = authenticated_client.post(f"/api/items/{item_id}/archive")
    assert archived.status_code == 200

    digest = authenticated_client.get("/api/briefs/today")
    assert digest.status_code == 200
    item_ids = _digest_item_ids(digest.json())
    assert item_id not in item_ids


def test_paper_enrichment_failure_does_not_block_digest_generation(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.papers.PaperMetadataClient._crossref",
        lambda self, doi: (_ for _ in ()).throw(RuntimeError("crossref unavailable")),
    )
    monkeypatch.setattr(
        "app.integrations.papers.PaperMetadataClient._semantic_scholar",
        lambda self, doi: {},
    )

    with get_session_factory()() as db:
        current_edition = BriefService(db).current_edition_date()
        source = Source(
            type=SourceType.ARXIV,
            name="arXiv AI",
            url="https://export.arxiv.org/rss/cs.AI",
            priority=90,
            active=True,
            tags=["papers"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        item = IngestionService(db).ingest_payload(
            source=source,
            title="A paper with DOI",
            canonical_url="https://arxiv.org/abs/2501.00001",
            authors=["Researcher"],
            published_at=_timestamp_for_day(current_edition - timedelta(days=1)),
            cleaned_text="We study verifier routing. DOI: 10.1000/182",
            raw_payload={"feed": "arxiv"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={"abstract_text": "Abstract-first summary input."},
            content_type=ContentType.PAPER,
        )
        assert item.metadata_json["doi"] == "10.1000/182"
        assert item.metadata_json["crossref_error"] == "unavailable"

    regenerate = authenticated_client.post("/api/ops/regenerate-brief")
    assert regenerate.status_code == 200

    digest = authenticated_client.get("/api/briefs/today")
    assert digest.status_code == 200
    payload = digest.json()
    assert any(entry["item"]["content_type"] == "paper" for entry in payload["papers_table"])


def test_digest_partitions_items_into_new_signal_lanes(client: TestClient) -> None:
    with get_session_factory()() as db:
        brief_date = BriefService(db).current_edition_date()
        coverage_day = brief_date - timedelta(days=1)
        source = Source(
            type=SourceType.RSS,
            name="Research Desk",
            url="https://example.com/signals.xml",
            priority=82,
            active=True,
            tags=["signals"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        ingestion = IngestionService(db)

        def ingest_ranked_item(
            *,
            title: str,
            canonical_url: str,
            cleaned_text: str,
            content_type: ContentType,
            hour: int,
            total_score: float,
            bucket: ScoreBucket,
            word_count: int | None = None,
        ) -> Item:
            item = ingestion.ingest_payload(
                source=source,
                title=title,
                canonical_url=canonical_url,
                authors=["Researcher"],
                published_at=_timestamp_for_day(coverage_day, hour=hour),
                cleaned_text=cleaned_text,
                raw_payload={"feed": "rss"},
                outbound_links=[],
                extraction_confidence=0.9,
                metadata_json={},
                content_type=content_type,
            )
            assert item.score is not None
            assert item.content is not None
            item.score.total_score = total_score
            item.score.bucket = bucket
            if word_count is not None:
                item.content.word_count = word_count
            db.add(item)
            db.commit()
            db.refresh(item)
            return item

        headline = ingest_ranked_item(
            title="API announcement from Example Labs",
            canonical_url="https://example.com/headline",
            cleaned_text="A compact company announcement about API availability and rollout changes.",
            content_type=ContentType.ARTICLE,
            hour=8,
            total_score=0.96,
            bucket=ScoreBucket.MUST_READ,
        )
        paper = ingest_ranked_item(
            title="Verifier routing paper",
            canonical_url="https://example.com/paper",
            cleaned_text="A paper about verifier routing and research triage.",
            content_type=ContentType.PAPER,
            hour=9,
            total_score=0.91,
            bucket=ScoreBucket.MUST_READ,
        )
        longform = ingest_ranked_item(
            title="Open source launch analysis",
            canonical_url="https://example.com/longform",
            cleaned_text="A deep analysis of a launch and its downstream implications for teams.",
            content_type=ContentType.ARTICLE,
            hour=10,
            total_score=0.69,
            bucket=ScoreBucket.WORTH_A_SKIM,
            word_count=2200,
        )
        backfill = ingest_ranked_item(
            title="Evaluation discipline deep dive",
            canonical_url="https://example.com/backfill",
            cleaned_text="A long-form article about evaluation discipline and benchmarking practice.",
            content_type=ContentType.ARTICLE,
            hour=11,
            total_score=0.68,
            bucket=ScoreBucket.WORTH_A_SKIM,
        )
        side_signal = ingest_ranked_item(
            title="Ecosystem shift in agent tooling",
            canonical_url="https://example.com/side-signal",
            cleaned_text="A newsletter note about ecosystem shift, developer adoption, and tooling drift.",
            content_type=ContentType.NEWSLETTER,
            hour=12,
            total_score=0.64,
            bucket=ScoreBucket.WORTH_A_SKIM,
        )
        remaining = ingest_ranked_item(
            title="Quiet reading queue note",
            canonical_url="https://example.com/remaining",
            cleaned_text="A strong but quieter article about replication discipline and study design.",
            content_type=ContentType.ARTICLE,
            hour=13,
            total_score=0.63,
            bucket=ScoreBucket.WORTH_A_SKIM,
        )

        digest = BriefService(db).get_or_generate_by_date(brief_date, data_mode=DataMode.LIVE)
        assert digest is not None

        assert [entry.item.id for entry in digest.headlines] == [headline.id]
        assert all(entry.item.id != longform.id for entry in digest.headlines)
        assert [entry.item.id for entry in digest.editorial_shortlist] == [
            paper.id,
            longform.id,
            backfill.id,
        ]
        assert [entry.item.id for entry in digest.interesting_side_signals] == [side_signal.id]
        assert [entry.item.id for entry in digest.remaining_reads] == [remaining.id]


def test_papers_table_ranks_top_five_and_uses_best_zotero_match(client: TestClient) -> None:
    with get_session_factory()() as db:
        brief_date = BriefService(db).current_edition_date()
        coverage_day = brief_date - timedelta(days=1)
        source = Source(
            type=SourceType.ARXIV,
            name="Paper Feed",
            url="https://example.com/papers.xml",
            priority=88,
            active=True,
            tags=["papers"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        ingestion = IngestionService(db)

        def ingest_ranked_paper(
            *,
            title: str,
            canonical_url: str,
            hour: int,
            total_score: float,
            source_quality_score: float,
            metadata_json: dict | None = None,
        ) -> Item:
            item = ingestion.ingest_payload(
                source=source,
                title=title,
                canonical_url=canonical_url,
                authors=["Researcher"],
                published_at=_timestamp_for_day(coverage_day, hour=hour),
                cleaned_text="A paper about agents, evaluation, and research workflows.",
                raw_payload={"feed": "arxiv"},
                outbound_links=[],
                extraction_confidence=0.9,
                metadata_json=metadata_json or {},
                content_type=ContentType.PAPER,
            )
            assert item.score is not None
            item.score.total_score = total_score
            item.score.bucket = ScoreBucket.MUST_READ if total_score >= 0.72 else ScoreBucket.WORTH_A_SKIM
            item.score.source_quality_score = source_quality_score
            db.add(item)
            db.commit()
            db.refresh(item)
            return item

        paper_a = ingest_ranked_paper(
            title="Highest ranked paper",
            canonical_url="https://example.com/paper-a",
            hour=8,
            total_score=0.95,
            source_quality_score=0.25,
        )
        paper_b = ingest_ranked_paper(
            title="Mistral systems paper",
            canonical_url="https://example.com/paper-b",
            hour=9,
            total_score=0.9,
            source_quality_score=0.8,
            metadata_json={
                "organization_name": "Mistral AI",
                "semantic_scholar_citation_count": 99,
            },
        )
        paper_c = ingest_ranked_paper(
            title="Venue-backed paper",
            canonical_url="https://example.com/paper-c",
            hour=10,
            total_score=0.9,
            source_quality_score=0.4,
            metadata_json={"semantic_scholar_venue": "ICLR"},
        )
        paper_d = ingest_ranked_paper(
            title="Fourth paper",
            canonical_url="https://example.com/paper-d",
            hour=11,
            total_score=0.88,
            source_quality_score=0.6,
        )
        paper_e = ingest_ranked_paper(
            title="Fifth paper",
            canonical_url="https://example.com/paper-e",
            hour=12,
            total_score=0.87,
            source_quality_score=0.5,
        )
        paper_f = ingest_ranked_paper(
            title="Sixth paper",
            canonical_url="https://example.com/paper-f",
            hour=13,
            total_score=0.86,
            source_quality_score=0.5,
        )

        db.add_all(
            [
                ZoteroMatch(
                    item_id=paper_b.id,
                    library_item_key="lower-match",
                    title="Lower match",
                    similarity_score=0.42,
                    metadata_json={"tags": ["ignore/me"]},
                ),
                ZoteroMatch(
                    item_id=paper_b.id,
                    library_item_key="best-match",
                    title="Best match",
                    similarity_score=0.96,
                    metadata_json={
                        "tags": [
                            "lab/mistral",
                            "method/tool_use",
                            "type/framework",
                            "eval/reasoning",
                            "extra/tag",
                        ]
                    },
                ),
            ]
        )
        db.commit()
        db.refresh(paper_b)

        digest = BriefService(db).get_or_generate_by_date(brief_date, data_mode=DataMode.LIVE)
        assert digest is not None

        assert [entry.item.id for entry in digest.papers_table] == [
            paper_a.id,
            paper_b.id,
            paper_c.id,
            paper_d.id,
            paper_e.id,
        ]
        assert paper_a.id in {entry.item.id for entry in digest.editorial_shortlist}
        assert paper_a.id in {entry.item.id for entry in digest.papers_table}

        papers_by_id = {entry.item.id: entry for entry in digest.papers_table}
        assert papers_by_id[paper_b.id].zotero_tags == [
            "lab/mistral",
            "method/tool_use",
            "type/framework",
            "eval/reasoning",
        ]
        assert papers_by_id[paper_b.id].credibility_score == compute_paper_credibility_score(paper_b)
        assert papers_by_id[paper_c.id].credibility_score == compute_paper_credibility_score(paper_c)
        assert papers_by_id[paper_b.id].credibility_score > papers_by_id[paper_c.id].credibility_score
        assert paper_f.id not in papers_by_id


def test_digest_generation_handles_five_hundred_items(client: TestClient) -> None:
    with get_session_factory()() as db:
        brief_date = BriefService(db).current_edition_date()
        coverage_day = brief_date - timedelta(days=1)
        source = db.scalar(select(Source).where(Source.name == "Batch Feed"))
        if not source:
            source = Source(
                type=SourceType.RSS,
                name="Batch Feed",
                url="https://example.com/feed.xml",
                priority=75,
                active=True,
                tags=["batch"],
            )
            db.add(source)
            db.commit()
            db.refresh(source)

        for index in range(500):
            item = Item(
                source=source,
                title=f"Research signal {index}",
                source_name=source.name,
                canonical_url=f"https://example.com/items/{index}",
                authors=[f"Author {index % 7}"],
                published_at=_timestamp_for_day(coverage_day),
                content_type=ContentType.ARTICLE,
                content_hash=f"batch-{index}",
                extraction_confidence=0.82,
                metadata_json={"batch": True},
            )
            item.content = ItemContent(
                item=item,
                raw_payload={"index": index},
                cleaned_text=f"Signal {index} about benchmark quality and transparent ranking.",
                extracted_text=f"Signal {index} about benchmark quality and transparent ranking.",
                outbound_links=[],
                word_count=9,
            )
            item.score = ItemScore(
                item=item,
                relevance_score=0.7,
                novelty_score=0.6,
                source_quality_score=0.5,
                author_match_score=0.3,
                topic_match_score=0.7,
                zotero_affinity_score=0.1,
                total_score=0.45 + ((500 - index) / 1000),
                bucket=ScoreBucket.MUST_READ if index < 3 else ScoreBucket.WORTH_A_SKIM,
                reason_trace={"batch": True, "rank": index},
            )
            db.add(item)

        db.commit()

        digest = BriefService(db).generate_digest(brief_date=brief_date, force=True)
        assert digest.title.startswith("Morning Brief")
        assert len(digest.entries) >= 3


def test_digest_generation_is_separate_per_data_mode(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    with get_session_factory()() as db:
        current_edition = BriefService(db).current_edition_date()
        source = Source(
            type=SourceType.RSS,
            name="Seed Feed",
            url="https://example.com/seed.xml",
            priority=70,
            active=True,
            tags=["seed"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        seeded_item = IngestionService(db).ingest_payload(
            source=source,
            title="Seed-only digest item",
            canonical_url="https://example.com/seed-digest-item",
            authors=["Demo Author"],
            published_at=_timestamp_for_day(current_edition - timedelta(days=1), hour=9),
            cleaned_text="Seeded digest content about demo data separation.",
            raw_payload={"seeded": True},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={"seeded": True},
            content_type=ContentType.ARTICLE,
        )

    imported = authenticated_client.post(
        "/api/items/import-url", json={"url": "https://example.com/article"}
    )
    assert imported.status_code == 201
    live_item_id = imported.json()["id"]

    seed_mode = authenticated_client.patch("/api/profile", json={"data_mode": "seed"})
    assert seed_mode.status_code == 200
    regenerate_seed = authenticated_client.post("/api/ops/regenerate-brief")
    assert regenerate_seed.status_code == 200

    seed_digest = authenticated_client.get("/api/briefs/today")
    assert seed_digest.status_code == 200
    seed_payload = seed_digest.json()
    seed_item_ids = _digest_item_ids(seed_payload)
    assert seed_payload["data_mode"] == "seed"
    assert seeded_item.id in seed_item_ids

    live_mode = authenticated_client.patch("/api/profile", json={"data_mode": "live"})
    assert live_mode.status_code == 200
    regenerate_live = authenticated_client.post("/api/ops/regenerate-brief")
    assert regenerate_live.status_code == 200

    live_digest = authenticated_client.get("/api/briefs/today")
    assert live_digest.status_code == 200
    live_payload = live_digest.json()
    live_item_ids = _digest_item_ids(live_payload)
    assert live_payload["data_mode"] == "live"
    assert live_item_id in live_item_ids
    assert live_payload["id"] != seed_payload["id"]

    back_to_seed = authenticated_client.patch("/api/profile", json={"data_mode": "seed"})
    assert back_to_seed.status_code == 200
    seed_digest_again = authenticated_client.get("/api/briefs/today")
    assert seed_digest_again.status_code == 200
    assert seed_digest_again.json()["id"] == seed_payload["id"]


def test_digest_generation_recovers_from_unique_brief_race(client: TestClient, monkeypatch) -> None:
    brief_date = datetime.now(UTC).date()

    with get_session_factory()() as db:
        service = BriefService(db)
        original_flush = db.flush
        triggered = {"done": False}
        monkeypatch.setattr(service.ingestion_service, "start_operation_run", lambda *args, **kwargs: object())
        monkeypatch.setattr(service.ingestion_service, "finalize_operation_run", lambda *args, **kwargs: None)

        def _racing_flush(*args, **kwargs):
            if not triggered["done"]:
                triggered["done"] = True
                with get_session_factory()() as other:
                    other.add(
                        Digest(
                            brief_date=brief_date,
                            data_mode=DataMode.LIVE,
                            status=RunStatus.SUCCEEDED,
                            title=f"Morning Brief • {brief_date.isoformat()}",
                            editorial_note="Precreated by competing request.",
                            suggested_follow_ups=[],
                        )
                    )
                    other.commit()
                raise IntegrityError("insert", {}, Exception("duplicate brief"))
            return original_flush(*args, **kwargs)

        monkeypatch.setattr(db, "flush", _racing_flush)
        digest = service.generate_digest(brief_date, force=True)

        assert digest.brief_date == brief_date


def test_item_dated_on_march_27_appears_in_march_28_edition(
    authenticated_client: TestClient,
) -> None:
    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Date Shift Feed",
            url="https://example.com/date-shift.xml",
            priority=80,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        item = IngestionService(db).ingest_payload(
            source=source,
            title="March 27 source item",
            canonical_url="https://example.com/march-27-item",
            authors=["Researcher"],
            published_at=datetime(2026, 3, 27, 8, 0, tzinfo=UTC),
            cleaned_text="An item dated March 27 should appear in the March 28 edition.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
        )

    march_27 = authenticated_client.get("/api/briefs/2026-03-27")
    assert march_27.status_code == 200
    assert item.id not in _digest_item_ids(march_27.json())

    march_28 = authenticated_client.get("/api/briefs/2026-03-28")
    assert march_28.status_code == 200
    assert item.id in _digest_item_ids(march_28.json())
    assert march_28.json()["coverage_start"] == "2026-03-27"
    assert march_28.json()["coverage_end"] == "2026-03-27"


def test_brief_availability_enables_days_and_completed_weeks_only(
    authenticated_client: TestClient,
) -> None:
    with get_session_factory()() as db:
        current_edition = BriefService(db).current_edition_date()
        current_week_start = current_edition - timedelta(days=current_edition.weekday())
        completed_week_start = current_week_start - timedelta(days=7)
        completed_week_day = completed_week_start + timedelta(days=4)
        tomorrow_edition = current_edition + timedelta(days=1)

        source = Source(
            type=SourceType.RSS,
            name="Availability Feed",
            url="https://example.com/availability.xml",
            priority=75,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        IngestionService(db).ingest_payload(
            source=source,
            title="Completed week item",
            canonical_url="https://example.com/completed-week-item",
            authors=["Researcher"],
            published_at=_timestamp_for_day(completed_week_day - timedelta(days=1), hour=10),
            cleaned_text="An item that belongs to a completed edition week.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
        )
        IngestionService(db).ingest_payload(
            source=source,
            title="Tomorrow edition item",
            canonical_url="https://example.com/tomorrow-item",
            authors=["Researcher"],
            published_at=_timestamp_for_day(current_edition, hour=11),
            cleaned_text="An item that should enable tomorrow's edition.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
        )

    availability = authenticated_client.get("/api/briefs/availability")
    assert availability.status_code == 200
    payload = availability.json()
    day_dates = [day["brief_date"] for day in payload["days"]]
    week_starts = [week["week_start"] for week in payload["weeks"]]

    assert tomorrow_edition.isoformat() in day_dates
    assert completed_week_day.isoformat() in day_dates
    assert payload["default_day"] == tomorrow_edition.isoformat()
    assert completed_week_start.isoformat() in week_starts
    assert current_week_start.isoformat() not in week_starts


def test_weekly_brief_aggregates_daily_digests_from_persisted_days(
    authenticated_client: TestClient,
) -> None:
    week_start = date(2026, 3, 16)
    day_one = week_start + timedelta(days=1)
    day_two = week_start + timedelta(days=2)

    with get_session_factory()() as db:
        source = Source(
            type=SourceType.RSS,
            name="Weekly Feed",
            url="https://example.com/weekly.xml",
            priority=82,
            active=True,
            tags=["rss"],
        )
        db.add(source)
        db.commit()
        db.refresh(source)

        item_a = IngestionService(db).ingest_payload(
            source=source,
            title="Promoted weekly item",
            canonical_url="https://example.com/promoted-weekly-item",
            authors=["Researcher"],
            published_at=_timestamp_for_day(day_one - timedelta(days=1), hour=9),
            cleaned_text="Promoted weekly item content.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
        )
        item_b = IngestionService(db).ingest_payload(
            source=source,
            title="Stable top item",
            canonical_url="https://example.com/stable-top-item",
            authors=["Researcher"],
            published_at=_timestamp_for_day(day_one - timedelta(days=1), hour=10),
            cleaned_text="Stable top item content.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
        )
        item_c = IngestionService(db).ingest_payload(
            source=source,
            title="High-rank tie breaker",
            canonical_url="https://example.com/high-rank-tie-breaker",
            authors=["Researcher"],
            published_at=_timestamp_for_day(day_two - timedelta(days=1), hour=11),
            cleaned_text="High-rank tie breaker content.",
            raw_payload={"feed": "rss"},
            outbound_links=[],
            extraction_confidence=0.9,
            metadata_json={},
            content_type=ContentType.ARTICLE,
        )

        assert item_a.score is not None
        assert item_b.score is not None
        assert item_c.score is not None
        item_a.score.total_score = 0.95
        item_a.score.bucket = ScoreBucket.MUST_READ
        item_b.score.total_score = 0.8
        item_b.score.bucket = ScoreBucket.MUST_READ
        item_c.score.total_score = 0.8
        item_c.score.bucket = ScoreBucket.MUST_READ

        digest_one = Digest(
            brief_date=day_one,
            data_mode=DataMode.LIVE,
            status=RunStatus.SUCCEEDED,
            title=f"Morning Brief • {day_one.isoformat()}",
            editorial_note="Day one",
            suggested_follow_ups=["ignored"],
        )
        digest_one.entries = [
            DigestEntry(
                item=item_a,
                section=DigestSection.REMAINING_READS,
                rank=3,
                note="Seen in remaining reads first.",
            ),
            DigestEntry(
                item=item_b,
                section=DigestSection.HEADLINES,
                rank=2,
                note="Stable headline item.",
            ),
        ]
        digest_two = Digest(
            brief_date=day_two,
            data_mode=DataMode.LIVE,
            status=RunStatus.SUCCEEDED,
            title=f"Morning Brief • {day_two.isoformat()}",
            editorial_note="Day two",
            suggested_follow_ups=["also ignored"],
        )
        digest_two.entries = [
            DigestEntry(
                item=item_a,
                section=DigestSection.EDITORIAL_SHORTLIST,
                rank=3,
                note="Promoted into the editorial shortlist.",
            ),
            DigestEntry(
                item=item_c,
                section=DigestSection.EDITORIAL_SHORTLIST,
                rank=1,
                note="Wins the rank tie break.",
            ),
        ]
        db.add_all([digest_one, digest_two, item_a, item_b, item_c])
        db.commit()

    weekly = authenticated_client.get(f"/api/briefs/weeks/{week_start.isoformat()}")
    assert weekly.status_code == 200
    payload = weekly.json()

    assert payload["period_type"] == "week"
    assert payload["week_start"] == week_start.isoformat()
    assert payload["week_end"] == (week_start + timedelta(days=6)).isoformat()
    assert payload["coverage_start"] == "2026-03-15"
    assert payload["coverage_end"] == "2026-03-21"
    assert payload["audio_brief"] is None
    assert payload["suggested_follow_ups"] == []
    assert [entry["item"]["id"] for entry in payload["editorial_shortlist"]] == [item_a.id, item_c.id]
    assert payload["editorial_shortlist"][0]["note"]
    assert [entry["item"]["id"] for entry in payload["headlines"]] == [item_b.id]
    assert payload["interesting_side_signals"] == []
    assert payload["remaining_reads"] == []
    assert payload["papers_table"] == []


def test_generate_audio_summary_persists_script_and_chapters(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.voice.VoiceClient.synthesize_to_bytes",
        lambda self, script: b"fake-mp3-audio",
    )
    imported = authenticated_client.post(
        "/api/items/import-url", json={"url": "https://example.com/article"}
    )
    assert imported.status_code == 201

    digest = authenticated_client.get("/api/briefs/today")
    assert digest.status_code == 200
    brief_date = digest.json()["brief_date"]

    audio_response = authenticated_client.post(f"/api/briefs/{brief_date}/generate-audio-summary")
    assert audio_response.status_code == 200
    payload = audio_response.json()

    assert payload["status"] == "succeeded"
    assert payload["script"]
    assert payload["estimated_duration_seconds"] > 0
    assert payload["metadata"]["generation_mode"] == "heuristic"
    assert payload["metadata"]["estimated_tts_cost_usd"] > 0
    assert payload["metadata"]["estimated_total_cost_usd"] >= payload["metadata"]["estimated_tts_cost_usd"]
    assert payload["metadata"]["voice_pricing_tier"] == "studio"
    assert payload["provider"] == "google-cloud"
    assert payload["voice"] == "en-US-Studio-O"
    assert len(payload["chapters"]) >= 1
    assert payload["chapters"][0]["item_id"] == imported.json()["id"]
    assert payload["chapters"][0]["offset_seconds"] >= 0

    refreshed = authenticated_client.get("/api/briefs/today")
    assert refreshed.status_code == 200
    refreshed_payload = refreshed.json()
    assert refreshed_payload["audio_brief"]["status"] == "succeeded"
    assert refreshed_payload["audio_brief"]["script"] == payload["script"]
    assert refreshed_payload["audio_brief"]["chapters"][0]["item_id"] == imported.json()["id"]
    assert refreshed_payload["audio_brief"]["provider"] == "google-cloud"


def test_generate_audio_summary_passes_richer_context_and_skips_spoken_headlines(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.voice.VoiceClient.synthesize_to_bytes",
        lambda self, script: b"fake-mp3-audio",
    )
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.summarize_item",
        lambda self, item, text: {
            "short_summary": "A concise summary of the imported article.",
            "why_it_matters": "Lower-latency tool use matters in production.",
            "whats_new": "The release narrows response time under load.",
            "caveats": "Benchmarks still need external validation.",
            "follow_up_questions": [
                "Which baseline matters most here?",
                "What would change the current interpretation?",
            ],
            "_usage": {"prompt_tokens": 320, "completion_tokens": 80, "total_tokens": 400},
        },
    )
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.summarize_item",
        lambda self, item, text: {
            "short_summary": "A concise summary of the imported article.",
            "why_it_matters": "Lower-latency tool use matters in production.",
            "whats_new": "The release narrows response time under load.",
            "caveats": "Benchmarks still need external validation.",
            "follow_up_questions": [
                "Which baseline matters most here?",
                "What would change the current interpretation?",
            ],
            "_usage": {"prompt_tokens": 320, "completion_tokens": 80, "total_tokens": 400},
        },
    )
    captured_digest: dict = {}

    def fake_compose_audio_brief(self, digest):
        captured_digest.clear()
        captured_digest.update(digest)
        return {
            "intro": "Good morning.",
            "outro": "That is the briefing.",
            "chapters": [
                {
                    "item_id": digest["shortlisted_items"][0]["item_id"],
                    "headline": "Title-like opener",
                    "narration": "This paragraph starts like natural speech and carries the key detail.",
                }
            ],
            "generation_mode": "remote",
        }

    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.compose_audio_brief",
        fake_compose_audio_brief,
    )
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.compose_audio_brief",
        fake_compose_audio_brief,
    )

    imported = authenticated_client.post(
        "/api/items/import-url", json={"url": "https://example.com/article"}
    )
    assert imported.status_code == 201

    with get_session_factory()() as db:
        brief_date = BriefService(db).schedule_service.current_profile_date().isoformat()

    regenerate = authenticated_client.post("/api/ops/regenerate-brief", json={"brief_date": brief_date})
    assert regenerate.status_code == 200

    audio_response = authenticated_client.post(f"/api/briefs/{brief_date}/generate-audio-summary")
    assert audio_response.status_code == 200
    payload = audio_response.json()

    assert captured_digest["shortlisted_items"]
    first_item = captured_digest["shortlisted_items"][0]
    assert first_item["why_it_matters"] == "Lower-latency tool use matters in production."
    assert first_item["whats_new"] == "The release narrows response time under load."
    assert first_item["caveats"] == "Benchmarks still need external validation."
    assert first_item["follow_up_questions"] == [
        "Which baseline matters most here?",
        "What would change the current interpretation?",
    ]
    assert first_item["source_excerpt"].startswith(
        "A hand-imported article about evaluation discipline, verifier routing,"
    )
    assert payload["chapters"][0]["headline"] == "Title-like opener."
    assert "Title-like opener" not in payload["script"]
    assert "This paragraph starts like natural speech and carries the key detail." in payload["script"]


def test_generate_audio_summary_creates_digest_when_missing(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.voice.VoiceClient.synthesize_to_bytes",
        lambda self, script: b"fake-mp3-audio",
    )
    imported = authenticated_client.post(
        "/api/items/import-url", json={"url": "https://example.com/article"}
    )
    assert imported.status_code == 201
    imported_item_id = imported.json()["id"]

    def fake_representatives(self, brief_date, *, data_mode):
        item = self.db.scalar(select(Item).where(Item.id == imported_item_id))
        assert item is not None
        return [item]

    monkeypatch.setattr(
        "app.services.briefs.BriefService._representative_items_for_edition_day",
        fake_representatives,
    )

    with get_session_factory()() as db:
        brief_date = BriefService(db).schedule_service.current_profile_date().isoformat()

    audio_response = authenticated_client.post(f"/api/briefs/{brief_date}/generate-audio-summary")
    assert audio_response.status_code == 200
    payload = audio_response.json()

    assert payload["status"] == "succeeded"
    assert payload["chapters"]
    assert payload["chapters"][0]["item_id"] == imported.json()["id"]


def test_operation_history_includes_brief_and_audio_runs_with_ai_cost(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.voice.VoiceClient.synthesize_to_bytes",
        lambda self, script: b"fake-mp3-audio",
    )
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.summarize_item",
        lambda self, item, text: {
            "short_summary": f"{item['title']} summary",
            "why_it_matters": "Why this matters.",
            "whats_new": "What changed.",
            "caveats": "Check the source.",
            "follow_up_questions": [],
            "_usage": {"prompt_tokens": 320, "completion_tokens": 80, "total_tokens": 400},
        },
    )
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.summarize_item",
        lambda self, item, text: {
            "short_summary": f"{item['title']} summary",
            "why_it_matters": "Why this matters.",
            "whats_new": "What changed.",
            "caveats": "Check the source.",
            "follow_up_questions": [],
            "_usage": {"prompt_tokens": 320, "completion_tokens": 80, "total_tokens": 400},
        },
    )
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.compose_audio_brief",
        lambda self, digest: {
            "intro": "Here is your morning update.",
            "outro": "That is the shortlist for today.",
            "chapters": [
                {
                    "item_id": item["item_id"],
                    "headline": item["title"],
                    "narration": item.get("short_summary") or item["title"],
                }
                for item in digest.get("shortlisted_items", [])
            ],
            "generation_mode": "remote",
            "_usage": {"prompt_tokens": 500, "completion_tokens": 140, "total_tokens": 640},
        },
    )
    monkeypatch.setattr(
        "app.services.ingestion.LLMClient.compose_audio_brief",
        lambda self, digest: {
            "intro": "Here is your morning update.",
            "outro": "That is the shortlist for today.",
            "chapters": [
                {
                    "item_id": item["item_id"],
                    "headline": item["title"],
                    "narration": item.get("short_summary") or item["title"],
                }
                for item in digest.get("shortlisted_items", [])
            ],
            "generation_mode": "remote",
            "_usage": {"prompt_tokens": 500, "completion_tokens": 140, "total_tokens": 640},
        },
    )

    imported = authenticated_client.post(
        "/api/items/import-url", json={"url": "https://example.com/article"}
    )
    assert imported.status_code == 201

    with get_session_factory()() as db:
        brief_date = BriefService(db).schedule_service.current_profile_date().isoformat()

    regenerate = authenticated_client.post("/api/ops/regenerate-brief", json={"brief_date": brief_date})
    assert regenerate.status_code == 200

    audio_response = authenticated_client.post(f"/api/briefs/{brief_date}/generate-audio-summary")
    assert audio_response.status_code == 200

    history_response = authenticated_client.get("/api/ops/ingestion-runs")
    assert history_response.status_code == 200
    history = history_response.json()

    digest_entries = [entry for entry in history if entry["operation_kind"] == "brief_generation"]
    digest_entry = max(
        (
            entry
            for entry in digest_entries
            if entry["affected_edition_days"] == [brief_date]
        ),
        key=lambda entry: entry["ai_total_tokens"],
    )
    audio_entry = next(entry for entry in history if entry["operation_kind"] == "audio_generation")

    assert digest_entry["affected_edition_days"] == [brief_date]
    assert digest_entry["ai_total_tokens"] >= 0
    assert digest_entry["ai_cost_usd"] >= 0
    assert digest_entry["tts_cost_usd"] == 0
    assert digest_entry["total_cost_usd"] == digest_entry["ai_cost_usd"]
    assert any(info["label"] == "Entries" for info in digest_entry["basic_info"])
    assert any(info["label"] == "Insights generated" for info in digest_entry["basic_info"])

    assert audio_entry["affected_edition_days"] == [brief_date]
    assert audio_entry["ai_total_tokens"] >= 640
    assert audio_entry["ai_cost_usd"] > 0
    assert audio_entry["tts_cost_usd"] > 0
    assert audio_entry["total_cost_usd"] > audio_entry["ai_cost_usd"]
    assert any(info["label"] == "Generation mode" and info["value"] == "remote" for info in audio_entry["basic_info"])
    assert any(info["label"] == "Voice tier" and info["value"] == "studio" for info in audio_entry["basic_info"])


def test_audio_summary_route_returns_audio_file(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.voice.VoiceClient.synthesize_to_bytes",
        lambda self, script: b"fake-mp3-audio",
    )
    created = authenticated_client.post(
        "/api/items/import-url", json={"url": "https://example.com/article"}
    )
    assert created.status_code == 201

    digest = authenticated_client.get("/api/briefs/today")
    assert digest.status_code == 200
    brief_date = digest.json()["brief_date"]

    generated = authenticated_client.post(f"/api/briefs/{brief_date}/generate-audio-summary")
    assert generated.status_code == 200

    audio_response = authenticated_client.get(f"/api/briefs/{brief_date}/audio")
    assert audio_response.status_code == 200
    assert audio_response.headers["content-type"] == "audio/mpeg"
    assert audio_response.content == b"fake-mp3-audio"


def test_stale_audio_summary_without_provider_metadata_is_not_marked_playable(
    authenticated_client: TestClient,
    fake_extractor: None,
) -> None:
    imported = authenticated_client.post(
        "/api/items/import-url", json={"url": "https://example.com/article"}
    )
    assert imported.status_code == 201

    digest = authenticated_client.get("/api/briefs/today")
    assert digest.status_code == 200
    digest_id = digest.json()["id"]

    with get_session_factory()() as db:
        digest_model = db.scalar(select(Digest).where(Digest.id == digest_id))
        assert digest_model is not None
        digest_model.audio_brief_status = RunStatus.SUCCEEDED.value
        digest_model.audio_brief_script = "A stale script-only summary."
        digest_model.audio_brief_chapters = []
        digest_model.audio_artifact_url = None
        digest_model.audio_artifact_provider = None
        digest_model.audio_artifact_voice = None
        digest_model.audio_duration_seconds = None
        digest_model.audio_brief_error = None
        db.add(digest_model)
        db.commit()

    refreshed = authenticated_client.get("/api/briefs/today")
    assert refreshed.status_code == 200
    assert refreshed.json()["audio_brief"]["status"] == "pending"


def test_audio_summary_route_surfaces_provider_http_errors(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    created = authenticated_client.post(
        "/api/items/import-url", json={"url": "https://example.com/article"}
    )
    assert created.status_code == 201

    digest = authenticated_client.get("/api/briefs/today")
    assert digest.status_code == 200
    payload = digest.json()
    digest_id = payload["id"]
    brief_date = payload["brief_date"]

    with get_session_factory()() as db:
        digest_model = db.scalar(select(Digest).where(Digest.id == digest_id))
        assert digest_model is not None
        digest_model.audio_brief_status = RunStatus.SUCCEEDED.value
        digest_model.audio_brief_script = "A summary that requires provider-backed audio."
        digest_model.audio_brief_chapters = []
        digest_model.audio_artifact_url = None
        digest_model.audio_artifact_provider = "google-cloud"
        digest_model.audio_artifact_voice = "en-US-Studio-O"
        digest_model.audio_duration_seconds = 42
        digest_model.audio_brief_error = None
        db.add(digest_model)
        db.commit()

    request = httpx.Request("POST", "https://texttospeech.googleapis.com/v1/text:synthesize")
    response = httpx.Response(401, request=request)
    error = httpx.HTTPStatusError("401 Unauthorized", request=request, response=response)
    monkeypatch.setattr(
        "app.integrations.voice.VoiceClient.ensure_cached_audio",
        lambda self, digest_id, script: (_ for _ in ()).throw(error),
    )

    audio_response = authenticated_client.get(f"/api/briefs/{brief_date}/audio")
    assert audio_response.status_code == 503
    assert "401 Unauthorized" in audio_response.json()["detail"]
