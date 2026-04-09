from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from app.core.config import get_settings
from app.services.brief_dates import coverage_day_for_edition
from app.services.vault_briefs import VaultBriefService
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_publishing import VaultPublisherService
from app.vault.models import RawDocumentFrontmatter
from app.vault.store import VaultStore


def seed_publishable_vault() -> dict[str, object]:
    store = VaultStore()
    store.ensure_layout()
    brief_service = VaultBriefService()
    brief_date = brief_service.current_edition_date()
    coverage_day = coverage_day_for_edition(brief_date)
    local_timezone = ZoneInfo(get_settings().timezone)
    published_at = datetime(
        coverage_day.year,
        coverage_day.month,
        coverage_day.day,
        12,
        0,
        tzinfo=local_timezone,
    ).astimezone(UTC)
    frontmatter = RawDocumentFrontmatter(
        id="publish-fixture-item",
        kind="article",
        title="Signal from the publishing test feed",
        source_url="https://example.com/publishing-test-item",
        source_name="Publishing Feed",
        authors=["Research Center"],
        published_at=published_at,
        ingested_at=published_at + timedelta(minutes=30),
        content_hash="",
        tags=["publishing", "vault"],
        status="active",
        asset_paths=[],
    )
    store.write_raw_document(
        kind="article",
        doc_id=frontmatter.id,
        frontmatter=frontmatter,
        body="A compact article used to verify vault publication and pairing flows.",
    )
    ingestion = VaultIngestionService()
    ingestion.rebuild_items_index(trigger="test_seed")
    digest = brief_service.generate_digest(brief_date, force=True, trigger="test_seed")
    summary = VaultPublisherService().publish_date(brief_date)
    return {
        "brief_date": brief_date,
        "digest": digest,
        "summary": summary,
    }
