from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from app.services.vault_alphaxiv import AlphaXivPaperResolver
from app.services.vault_briefs import VaultBriefService
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_items import VaultItemService
from app.services.vault_publishing import VaultPublisherService
from app.vault.models import RawDocumentFrontmatter
from app.vault.store import VaultStore


def _write_json(path, payload) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _seed_alphaxiv_paper(
    *,
    store: VaultStore,
    doc_id: str,
    title: str,
    canonical_url: str,
    short_summary: str,
    overview_summary: dict,
    similar_papers: list[dict],
    podcast_url: str | None = None,
    body: str = "Original raw body that should not appear as the filed text.",
) -> None:
    now = datetime.now(UTC)
    frontmatter = RawDocumentFrontmatter(
        id=doc_id,
        kind="paper",
        title=title,
        source_url=canonical_url,
        source_name="alphaXiv Papers",
        authors=["Research Center"],
        published_at=now - timedelta(hours=6),
        ingested_at=now,
        content_hash=f"hash-{doc_id}",
        tags=["paper", "alphaxiv", "research", "summary"],
        status="active",
        asset_paths=[
            "alphaxiv-metadata.json",
            "alphaxiv-overview.json",
            "alphaxiv-preview.json",
            "alphaxiv-similar-papers.json",
        ],
        source_id="alphaxiv-paper",
        source_pipeline_id="alphaxiv-paper",
        external_key=canonical_url,
        canonical_url=canonical_url,
        short_summary=short_summary,
        fetched_at=now,
    )
    path = store.write_raw_document(
        kind="paper",
        doc_id=doc_id,
        frontmatter=frontmatter,
        body=body,
    )
    source_dir = path.parent
    _write_json(
        source_dir / "alphaxiv-overview.json",
        {
            "abstract": "AlphaXiv abstract text.",
            "summary": overview_summary,
            "summarySectionTitles": {
                "abstract": "Abstract",
                "problem": "Problem",
                "method": "Method",
                "results": "Results",
                "summary": "Summary",
                "takeaways": "Takeaways",
            },
        },
    )
    _write_json(
        source_dir / "alphaxiv-preview.json",
        {
            "abstract": "Preview abstract text.",
            "paper_summary": overview_summary | {"summary": short_summary},
            "title": title,
            "universal_paper_id": canonical_url.rsplit("/", 1)[-1],
        },
    )
    _write_json(
        source_dir / "alphaxiv-metadata.json",
        {
            "abstract": "Metadata abstract text.",
            "short_summary": short_summary,
            "podcast_url": podcast_url,
        },
    )
    _write_json(source_dir / "alphaxiv-similar-papers.json", similar_papers)


def test_alphaxiv_resolver_builds_summary_audio_and_linkable_similar_papers(client) -> None:
    store = VaultStore()
    store.ensure_layout()

    overview_summary = {
        "originalProblem": [
            "Problem point one.",
            "Problem point two.",
        ],
        "solution": [
            "Method step one.",
            "Method step two.",
        ],
        "results": [
            "Result point one.",
            "Result point two.",
        ],
        "summary": "Short AlphaXiv summary.",
        "keyInsights": [
            "Takeaway one.",
            "Takeaway two.",
        ],
    }

    _seed_alphaxiv_paper(
        store=store,
        doc_id="main-alphaxiv-paper",
        title="Main AlphaXiv Paper",
        canonical_url="https://www.alphaxiv.org/abs/2604.06169",
        short_summary="Short AlphaXiv summary.",
        overview_summary=overview_summary,
        similar_papers=[
            {
                "title": "Matched Similar Paper",
                "authors": ["Author A"],
                "paper_summary": {"summary": "Matched paper summary."},
                "universal_paper_id": "2604.09999",
            },
            {
                "title": "Matched Similar Paper",
                "authors": ["Author A"],
                "paper_summary": {"summary": "Duplicate entry should collapse."},
                "universal_paper_id": "2604.09999",
            },
            {
                "title": "External Similar Paper",
                "authors": ["Author B"],
                "paper_summary": {"summary": "External paper summary."},
                "universal_paper_id": "2604.08888",
            },
            {
                "title": "Broken Asset Route",
                "authors": ["Asset Bot"],
                "paper_summary": {"summary": "Should be filtered out."},
                "universal_paper_id": "2604.08888/index-bundle.js",
            },
        ],
        podcast_url="https://paper-podcasts.alphaxiv.org/group-123/podcast.mp3",
    )
    _seed_alphaxiv_paper(
        store=store,
        doc_id="matched-similar-paper",
        title="Matched Similar Paper",
        canonical_url="https://www.alphaxiv.org/abs/2604.09999",
        short_summary="Matched item summary.",
        overview_summary=overview_summary,
        similar_papers=[],
    )

    index = VaultIngestionService().rebuild_items_index(trigger="test_alphaxiv_resolver")
    main_item = next(item for item in index.items if item.id == "main-alphaxiv-paper")
    matched_item = next(item for item in index.items if item.id == "matched-similar-paper")
    raw = store.read_raw_document_relative(main_item.raw_doc_path)
    assert raw is not None

    resolver = AlphaXivPaperResolver(store=store, items=index.items)
    paper = resolver.resolve(main_item, raw_document=raw)

    assert paper is not None
    assert paper.short_summary == "Short AlphaXiv summary."
    assert paper.audio_url == "https://paper-podcasts.alphaxiv.org/group-123/podcast.mp3"
    assert paper.filed_text is not None
    assert "## Abstract" in paper.filed_text
    assert "## Problem" in paper.filed_text
    assert "## Method" in paper.filed_text
    assert "## Results" in paper.filed_text
    assert "## Summary" in paper.filed_text
    assert "## Takeaways" in paper.filed_text
    assert "Original raw body" not in paper.filed_text
    assert [entry.title for entry in paper.similar_papers] == ["Matched Similar Paper", "External Similar Paper"]
    assert paper.similar_papers[0].app_item_id == matched_item.id
    assert paper.similar_papers[0].canonical_url == "https://www.alphaxiv.org/abs/2604.09999"
    assert paper.similar_papers[1].app_item_id is None
    assert paper.similar_papers[1].canonical_url == "https://www.alphaxiv.org/abs/2604.08888"


def test_alphaxiv_details_flow_into_item_detail_and_published_manifest(client) -> None:
    store = VaultStore()
    store.ensure_layout()

    overview_summary = {
        "originalProblem": ["Problem detail."],
        "solution": ["Method detail."],
        "results": ["Result detail."],
        "summary": "Manifest summary.",
        "keyInsights": ["Takeaway detail."],
    }

    _seed_alphaxiv_paper(
        store=store,
        doc_id="manifest-alphaxiv-paper",
        title="Manifest AlphaXiv Paper",
        canonical_url="https://www.alphaxiv.org/abs/2604.07777",
        short_summary="Manifest summary.",
        overview_summary=overview_summary,
        similar_papers=[],
        podcast_url="https://paper-podcasts.alphaxiv.org/group-999/podcast.mp3",
    )

    index = VaultIngestionService().rebuild_items_index(trigger="test_alphaxiv_manifest")
    main_item = next(item for item in index.items if item.id == "manifest-alphaxiv-paper")

    detail = VaultItemService().get_item_detail(main_item.id)
    assert detail is not None
    assert detail.alphaxiv is not None
    assert detail.alphaxiv.short_summary == "Manifest summary."
    assert "## Problem" in (detail.alphaxiv.filed_text or "")
    assert detail.alphaxiv.audio_url == "https://paper-podcasts.alphaxiv.org/group-999/podcast.mp3"

    brief_date = VaultBriefService().current_edition_date()
    digest = VaultBriefService().generate_digest(brief_date, force=True, trigger="test_alphaxiv_manifest")
    manifest = VaultPublisherService().build_manifest(digest)

    published = manifest.items[main_item.id]
    assert published.alphaxiv is not None
    assert published.alphaxiv.short_summary == "Manifest summary."
    assert published.insight.short_summary == "Manifest summary."
    assert "## Method" in (published.alphaxiv.filed_text or "")
    paper_entry = next(entry for entry in manifest.digest.papers_table if entry.item.id == main_item.id)
    assert paper_entry.item.short_summary == "Manifest summary."
