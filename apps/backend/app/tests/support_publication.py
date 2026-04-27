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


def seed_published_shell() -> None:
    settings = get_settings()
    shell_dir = settings.published_web_dist_dir
    (shell_dir / "assets").mkdir(parents=True, exist_ok=True)
    (shell_dir / "assets" / "index-test.js").write_text("console.log('published shell');\n", encoding="utf-8")
    (shell_dir / "assets" / "index-test.css").write_text("body { background: #ece4d3; }\n", encoding="utf-8")
    (shell_dir / "index.html").write_text(
        """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Research Center</title>
    <link rel="manifest" href="./manifest.webmanifest" />
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="./assets/index-test.js"></script>
  </body>
</html>
""",
        encoding="utf-8",
    )
    (shell_dir / "manifest.webmanifest").write_text(
        '{"name":"Research Center","short_name":"RC","start_url":"./","scope":"./","display":"standalone"}\n',
        encoding="utf-8",
    )
    (shell_dir / "registerSW.js").write_text("console.log('register sw');\n", encoding="utf-8")
    (shell_dir / "sw.js").write_text("self.addEventListener('fetch', () => {});\n", encoding="utf-8")
    (shell_dir / "workbox-test.js").write_text("console.log('workbox');\n", encoding="utf-8")
    for filename in ("apple-touch-icon.png", "icon-192.png", "icon-512.png", "icon.svg"):
        (shell_dir / filename).write_bytes(b"test-asset")


def seed_publishable_vault() -> dict[str, object]:
    seed_published_shell()
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
