from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.services.vault_advanced_enrichment import VaultAdvancedEnrichmentService
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_runtime import content_hash
from app.services.vault_wiki import VaultWikiService
from app.vault.models import RawDocumentFrontmatter, WikiPageFrontmatter
from app.vault.store import VaultStore


def _seed_raw_document(
    *,
    doc_id: str,
    title: str,
    body: str,
    tags: list[str],
    published_at: datetime,
    source_name: str,
    source_id: str,
    kind: str = "paper",
) -> None:
    store = VaultStore()
    frontmatter = RawDocumentFrontmatter(
        id=doc_id,
        kind=kind,
        title=title,
        source_url=f"https://example.com/{doc_id}",
        source_name=source_name,
        authors=["Research Team"],
        published_at=published_at,
        ingested_at=published_at,
        content_hash=content_hash(title, body),
        tags=tags,
        status="active",
        asset_paths=[],
        source_id=source_id,
        source_pipeline_id=source_id,
        external_key=doc_id,
        canonical_url=f"https://example.com/{doc_id}",
        doc_role="primary",
        parent_id=None,
        index_visibility="visible",
        fetched_at=published_at,
        short_summary=body[:200],
        lightweight_enrichment_status="succeeded",
        lightweight_enriched_at=published_at,
        lightweight_enrichment_model="ollama:test",
        lightweight_enrichment_input_hash=content_hash(title, body),
        lightweight_enrichment_error=None,
    )
    store.write_raw_document(
        kind=kind,
        doc_id=doc_id,
        frontmatter=frontmatter,
        body=body,
    )


def _seed_research_cluster() -> None:
    now = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)
    _seed_raw_document(
        doc_id="2026-04-07-agent-evals",
        title="Agent evals with tool use",
        body=(
            "A paper about multi-agent evaluation workflows, verifier routing, tool calling, "
            "and test-time reasoning loops."
        ),
        tags=["agents", "evals", "tool calling", "cs.AI"],
        published_at=now - timedelta(days=1),
        source_name="alphaXiv Papers",
        source_id="alphaxiv",
    )
    _seed_raw_document(
        doc_id="2026-04-06-agent-memory",
        title="Multi-agent memory systems",
        body=(
            "This work studies agents with long-term memory, retrieval-augmented generation, "
            "and research graph planning."
        ),
        tags=["agents", "memory", "rag", "cs.LG"],
        published_at=now - timedelta(days=2),
        source_name="OpenAI",
        source_id="openai-website",
        kind="blog-post",
    )
    _seed_raw_document(
        doc_id="2026-02-01-alignment-note",
        title="Alignment note on reward modeling",
        body="An older note on alignment, reward modeling, safety, and oversight loops.",
        tags=["alignment", "safety"],
        published_at=now - timedelta(days=66),
        source_name="Anthropic",
        source_id="anthropic-research",
        kind="blog-post",
    )


def test_rebuild_items_index_persists_topic_refs_and_insights_index(client) -> None:
    _seed_research_cluster()

    index = VaultIngestionService().rebuild_items_index(trigger="test_insights_index")

    agents_item = next(item for item in index.items if item.id == "2026-04-07-agent-evals")
    assert any(ref.label == "Agents" for ref in agents_item.topic_refs)
    assert any(ref.label == "Evaluations" for ref in agents_item.topic_refs)
    assert agents_item.trend_score > 0

    insights = VaultStore().load_insights_index()
    topic_lookup = {topic.id: topic for topic in insights.topics}
    assert "agents" in topic_lookup
    assert "alignment" in topic_lookup
    assert topic_lookup["agents"].recent_item_count_7d == 2
    assert topic_lookup["agents"].trend_score > topic_lookup["alignment"].trend_score
    assert "agents" in insights.rising_topic_ids[:5]


def test_wiki_compile_materializes_topic_map_and_trend_pages(client) -> None:
    _seed_research_cluster()
    VaultIngestionService().rebuild_items_index(trigger="test_wiki_materialization")

    pages = VaultWikiService().compile(trigger="test_wiki_materialization")

    page_paths = {page.path for page in pages.pages}
    assert "wiki/topics/agents.md" in page_paths
    assert "wiki/trends/rising-topics.md" in page_paths
    assert "wiki/maps/global-ai-research.md" in page_paths
    assert any(path.startswith("wiki/sources/") for path in page_paths)

    store = VaultStore()
    agents_page = (store.wiki_dir / "topics" / "agents.md").read_text(encoding="utf-8")
    assert "## Representative Signals" in agents_page
    assert "[Rising Topics](../trends/rising-topics.md)" in agents_page

    graph = store.load_graph_index()
    assert any(edge.target == "topic:agents" and edge.edge_type == "wiki_link" for edge in graph.edges)


def test_wiki_compile_self_heals_missing_insights_and_uses_explicit_topic_links(client) -> None:
    _seed_research_cluster()
    index = VaultIngestionService().rebuild_items_index(trigger="test_wiki_self_heal")
    store = VaultStore()

    # Simulate an upgraded vault with an items index but no usable insight metadata yet.
    store.save_items_index(
        index.model_copy(
            update={
                "items": [item.model_copy(update={"topic_refs": [], "trend_score": 0.0, "novelty_score": 0.0}) for item in index.items]
            }
        )
    )
    store.save_insights_index(store.load_insights_index().model_copy(update={"topics": [], "connections": [], "rising_topic_ids": []}))

    store.write_wiki_page(
        namespace="concepts",
        slug="agents",
        frontmatter=WikiPageFrontmatter(
            id="wiki:concepts:agents",
            page_type="concept",
            title="Agents",
            aliases=[],
            source_refs=[],
            backlinks=[],
            updated_at=datetime(2026, 4, 8, 12, 0, tzinfo=UTC),
            managed=False,
        ),
        body="# Agents\n\nA manual page with the same title as the system topic page.\n",
    )

    pages = VaultWikiService().compile(trigger="test_wiki_self_heal")

    page_paths = {page.path for page in pages.pages}
    assert "wiki/topics/agents.md" in page_paths

    refreshed_items = VaultStore().load_items_index().items
    source_item = next(item for item in refreshed_items if item.id == "2026-04-07-agent-evals")
    assert source_item.topic_refs

    source_page = next(page.path for page in pages.pages if page.id == "page:2026-04-07-agent-evals")
    source_page_text = (store.root / source_page).read_text(encoding="utf-8")
    assert "[Agents](../topics/agents.md)" in source_page_text

    graph = store.load_graph_index()
    source_page_entry = next(page for page in pages.pages if page.path == source_page)
    assert any(
        edge.source == source_page_entry.id and edge.target == "topic:agents" and edge.edge_type == "wiki_link"
        for edge in graph.edges
    )


def test_compile_manifest_includes_candidate_topics_and_topic_annotated_raw_docs(client) -> None:
    _seed_research_cluster()
    VaultIngestionService().rebuild_items_index(trigger="test_compile_manifest")
    VaultWikiService().compile(trigger="test_compile_manifest")

    service = VaultAdvancedEnrichmentService()
    documents = service._select_compile_documents(source_id=None, doc_id=None, limit=5)  # noqa: SLF001
    pages = service._compile_candidate_pages(documents)  # noqa: SLF001
    manifest = service._build_compile_manifest(run_id="run-compile", documents=documents, pages=pages)  # noqa: SLF001

    assert any(topic.label == "Agents" for topic in manifest.candidate_topics)
    assert manifest.rising_topics
    assert any(doc.topic_refs for doc in manifest.candidate_raw_docs)
    assert any(page.path == "wiki/topics/agents.md" for page in manifest.candidate_wiki_pages)

    search = service.search_vault(query="agents", limit=5)
    assert any(topic["label"] == "Agents" for topic in search["topics"])
