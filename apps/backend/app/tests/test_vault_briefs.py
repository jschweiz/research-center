from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from app.db.models import DataMode
from app.schemas.briefs import DigestEntryRead, DigestRead
from app.services.brief_dates import coverage_day_for_edition, iso_week_end, iso_week_start
from app.services.vault_briefs import VaultBriefService
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_runtime import content_hash, to_item_list_entry
from app.vault.models import LightweightJudgeScore, RawDocumentFrontmatter
from app.vault.store import VaultStore


def _seed_raw_item(
    *,
    item_id: str,
    title: str,
    published_at: datetime,
    source_name: str = "Weekly Feed",
    authors: list[str] | None = None,
    tags: list[str] | None = None,
    score: LightweightJudgeScore | None = None,
) -> None:
    store = VaultStore()
    body = f"{title} body text for weekly aggregation coverage."
    frontmatter = RawDocumentFrontmatter(
        id=item_id,
        kind="article",
        title=title,
        source_url=f"https://example.com/{item_id}",
        source_name=source_name,
        authors=authors or ["Research Center"],
        published_at=published_at,
        ingested_at=published_at + timedelta(minutes=30),
        content_hash=content_hash(title, body),
        tags=tags or ["weekly"],
        status="active",
        asset_paths=[],
        source_id="weekly-feed",
        source_pipeline_id="weekly-feed",
        external_key=f"https://example.com/{item_id}",
        canonical_url=f"https://example.com/{item_id}",
        doc_role="primary",
        parent_id=None,
        index_visibility="visible",
        fetched_at=published_at + timedelta(minutes=15),
        short_summary=f"{title} summary.",
        lightweight_enrichment_status="succeeded",
        lightweight_enriched_at=published_at + timedelta(minutes=45),
        lightweight_enrichment_model="test",
        lightweight_enrichment_input_hash=content_hash(title, body),
        lightweight_enrichment_error=None,
        lightweight_scoring_model="test-judge",
        lightweight_scoring_input_hash=content_hash(title, f"score::{body}"),
        lightweight_score=score,
    )
    store.write_raw_document(kind=frontmatter.kind, doc_id=frontmatter.id, frontmatter=frontmatter, body=body)


def _write_digest(digest: DigestRead) -> None:
    store = VaultStore()
    store.write_bytes(
        store.brief_dir_for_date(digest.brief_date) / "brief.json",
        digest.model_dump_json(indent=2).encode("utf-8"),
    )


def _empty_digest(*, brief_date, generated_at: datetime, title: str) -> DigestRead:
    coverage_day = coverage_day_for_edition(brief_date)
    return DigestRead(
        id=f"day:{brief_date.isoformat()}",
        period_type="day",
        brief_date=brief_date,
        week_start=None,
        week_end=None,
        coverage_start=coverage_day,
        coverage_end=coverage_day,
        data_mode=DataMode.LIVE,
        title=title,
        editorial_note=None,
        suggested_follow_ups=[],
        audio_brief=None,
        generated_at=generated_at,
        editorial_shortlist=[],
        headlines=[],
        interesting_side_signals=[],
        remaining_reads=[],
        papers_table=[],
    )


def test_weekly_brief_aggregates_persisted_daily_digest_entries(
    authenticated_client: TestClient,
) -> None:
    service = VaultBriefService()
    week_start = iso_week_start(service.current_edition_date()) - timedelta(days=7)
    day_one = week_start + timedelta(days=1)
    day_two = week_start + timedelta(days=2)

    _seed_raw_item(
        item_id="weekly-promoted-item",
        title="Promoted weekly item",
        published_at=datetime.combine(day_one, datetime.min.time(), tzinfo=UTC) + timedelta(hours=9),
    )
    _seed_raw_item(
        item_id="weekly-stable-item",
        title="Stable top item",
        published_at=datetime.combine(day_one, datetime.min.time(), tzinfo=UTC) + timedelta(hours=10),
    )
    _seed_raw_item(
        item_id="weekly-tie-breaker",
        title="High-rank tie breaker",
        published_at=datetime.combine(day_two, datetime.min.time(), tzinfo=UTC) + timedelta(hours=11),
    )
    VaultIngestionService().rebuild_items_index(trigger="test_weekly_digest")
    item_lookup = {item.id: item for item in VaultStore().load_items_index().items}

    digest_one = _empty_digest(
        brief_date=day_one,
        generated_at=datetime.combine(day_one, datetime.min.time(), tzinfo=UTC) + timedelta(hours=7),
        title=f"Research Brief · {day_one.isoformat()}",
    ).model_copy(
        update={
            "headlines": [
                DigestEntryRead(
                    item=to_item_list_entry(item_lookup["weekly-stable-item"]),
                    note="Stable headline item.",
                    rank=2,
                )
            ],
            "remaining_reads": [
                DigestEntryRead(
                    item=to_item_list_entry(item_lookup["weekly-promoted-item"]),
                    note="Seen in remaining reads first.",
                    rank=3,
                )
            ],
        }
    )
    digest_two = _empty_digest(
        brief_date=day_two,
        generated_at=datetime.combine(day_two, datetime.min.time(), tzinfo=UTC) + timedelta(hours=7),
        title=f"Research Brief · {day_two.isoformat()}",
    ).model_copy(
        update={
            "editorial_shortlist": [
                DigestEntryRead(
                    item=to_item_list_entry(item_lookup["weekly-promoted-item"]),
                    note="Promoted into the editorial shortlist.",
                    rank=3,
                ),
                DigestEntryRead(
                    item=to_item_list_entry(item_lookup["weekly-tie-breaker"]),
                    note="Wins the rank tie break.",
                    rank=1,
                ),
            ]
        }
    )
    _write_digest(digest_one)
    _write_digest(digest_two)

    weekly = authenticated_client.get(f"/api/briefs/weeks/{week_start.isoformat()}")
    assert weekly.status_code == 200
    payload = weekly.json()

    assert payload["period_type"] == "week"
    assert payload["week_start"] == week_start.isoformat()
    assert payload["week_end"] == iso_week_end(week_start).isoformat()
    assert payload["audio_brief"] is None
    assert payload["suggested_follow_ups"] == []
    assert [entry["item"]["id"] for entry in payload["editorial_shortlist"]] == [
        "weekly-promoted-item",
        "weekly-tie-breaker",
    ]
    assert payload["editorial_shortlist"][0]["note"] == "Promoted into the editorial shortlist."
    assert [entry["item"]["id"] for entry in payload["headlines"]] == ["weekly-stable-item"]
    assert payload["interesting_side_signals"] == []
    assert payload["remaining_reads"] == []
    assert payload["papers_table"] == []


def test_rebuilt_vault_scores_drive_daily_brief_priority(
    authenticated_client: TestClient,
) -> None:
    service = VaultBriefService()
    brief_date = service.current_edition_date()

    _seed_raw_item(
        item_id="high-score-older-item",
        title="Verifier routing field notes",
        published_at=datetime.combine(brief_date, datetime.min.time(), tzinfo=UTC) - timedelta(hours=20),
        source_name="Example Research",
        authors=["Casey Researcher"],
        tags=["verifier routing", "triage"],
        score=LightweightJudgeScore(
            relevance_score=0.92,
            source_fit_score=0.8,
            topic_fit_score=0.93,
            author_fit_score=0.72,
            evidence_fit_score=0.83,
            confidence_score=0.78,
            bucket_hint="must_read",
            reason="Direct fit for the current research workflow.",
            evidence_quotes=["verifier routing", "research triage"],
        ),
    )
    _seed_raw_item(
        item_id="low-score-newer-item",
        title="General AI industry roundup",
        published_at=datetime.combine(brief_date, datetime.min.time(), tzinfo=UTC) - timedelta(hours=2),
        source_name="Generic Industry Feed",
        authors=["Market Writer"],
        tags=["roundup"],
        score=LightweightJudgeScore(
            relevance_score=0.22,
            source_fit_score=0.25,
            topic_fit_score=0.2,
            author_fit_score=0.1,
            evidence_fit_score=0.32,
            confidence_score=0.7,
            bucket_hint="archive",
            reason="Generic industry coverage with weak profile fit.",
            evidence_quotes=["industry roundup"],
        ),
    )

    VaultIngestionService().rebuild_items_index(trigger="test_scored_brief_priority")

    items = authenticated_client.get("/api/items")
    assert items.status_code == 200
    payload = items.json()

    assert payload[0]["id"] == "high-score-older-item"
    assert payload[0]["bucket"] == "must_read"
    assert payload[0]["total_score"] > payload[1]["total_score"]

    digest = authenticated_client.get("/api/briefs/today")
    assert digest.status_code == 200
    shortlist_ids = [entry["item"]["id"] for entry in digest.json()["editorial_shortlist"]]
    assert shortlist_ids[0] == "high-score-older-item"


def test_daily_brief_filters_to_previous_local_coverage_day(
    authenticated_client: TestClient,
) -> None:
    service = VaultBriefService()
    brief_date = service.current_edition_date()
    coverage_day = coverage_day_for_edition(brief_date)

    _seed_raw_item(
        item_id="coverage-day-item",
        title="Coverage day signal",
        published_at=datetime.combine(coverage_day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=10),
        score=LightweightJudgeScore(
            relevance_score=0.55,
            source_fit_score=0.55,
            topic_fit_score=0.55,
            author_fit_score=0.4,
            evidence_fit_score=0.6,
            confidence_score=0.7,
            bucket_hint="worth_a_skim",
            reason="Falls inside the expected brief window.",
            evidence_quotes=["coverage day"],
        ),
    )
    _seed_raw_item(
        item_id="same-day-item",
        title="Same day signal",
        published_at=datetime.combine(brief_date, datetime.min.time(), tzinfo=UTC) + timedelta(hours=10),
        score=LightweightJudgeScore(
            relevance_score=0.99,
            source_fit_score=0.9,
            topic_fit_score=0.9,
            author_fit_score=0.8,
            evidence_fit_score=0.9,
            confidence_score=0.9,
            bucket_hint="must_read",
            reason="Would have ranked highly without the date window.",
            evidence_quotes=["same day"],
        ),
    )
    _seed_raw_item(
        item_id="historical-item",
        title="Historical signal",
        published_at=datetime.combine(coverage_day - timedelta(days=14), datetime.min.time(), tzinfo=UTC)
        + timedelta(hours=10),
        score=LightweightJudgeScore(
            relevance_score=0.99,
            source_fit_score=0.9,
            topic_fit_score=0.9,
            author_fit_score=0.8,
            evidence_fit_score=0.9,
            confidence_score=0.9,
            bucket_hint="must_read",
            reason="Would have ranked highly without the date window.",
            evidence_quotes=["historical"],
        ),
    )

    VaultIngestionService().rebuild_items_index(trigger="test_daily_brief_coverage_window")

    digest = authenticated_client.get(f"/api/briefs/{brief_date.isoformat()}")
    assert digest.status_code == 200
    payload = digest.json()
    digest_item_ids = {
        entry["item"]["id"]
        for section in (
            "editorial_shortlist",
            "headlines",
            "interesting_side_signals",
            "remaining_reads",
            "papers_table",
        )
        for entry in payload[section]
    }

    assert payload["coverage_start"] == coverage_day.isoformat()
    assert payload["coverage_end"] == coverage_day.isoformat()
    assert "coverage-day-item" in digest_item_ids
    assert "same-day-item" not in digest_item_ids
    assert "historical-item" not in digest_item_ids


def test_brief_availability_lists_only_completed_weeks(
    authenticated_client: TestClient,
) -> None:
    service = VaultBriefService()
    current_day = service.current_edition_date()
    current_week_start = iso_week_start(current_day)
    completed_week_start = current_week_start - timedelta(days=7)
    completed_week_day = completed_week_start + timedelta(days=1)

    _write_digest(
        _empty_digest(
            brief_date=completed_week_day,
            generated_at=datetime.combine(completed_week_day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=7),
            title=f"Research Brief · {completed_week_day.isoformat()}",
        )
    )
    _write_digest(
        _empty_digest(
            brief_date=current_day,
            generated_at=datetime.combine(current_day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=7),
            title=f"Research Brief · {current_day.isoformat()}",
        )
    )

    availability = authenticated_client.get("/api/briefs/availability")
    assert availability.status_code == 200
    payload = availability.json()

    assert payload["default_day"] == current_day.isoformat()
    assert completed_week_day.isoformat() in [day["brief_date"] for day in payload["days"]]
    assert current_day.isoformat() in [day["brief_date"] for day in payload["days"]]
    assert completed_week_start.isoformat() in [week["week_start"] for week in payload["weeks"]]
    assert current_week_start.isoformat() not in [week["week_start"] for week in payload["weeks"]]
