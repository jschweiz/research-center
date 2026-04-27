from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
from fastapi.testclient import TestClient

from app.core.external_urls import resolve_external_url
from app.integrations.extractors import ExtractedContent
from app.services.local_control import LocalControlService
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_items import FULL_ARTICLE_SECTION_HEADING, SUMMARY_ASSET_FILENAME
from app.vault.models import RawDocumentFrontmatter
from app.vault.store import VaultStore

SUMMARY_PAYLOAD = {
    "short_summary": "A concise read on production retrieval systems.",
    "why_it_matters": "It turns vague RAG advice into practical architecture choices.",
    "whats_new": "It adds deployment guidance and failure-handling details.",
    "caveats": "The trade-offs still depend on latency and infrastructure limits.",
    "follow_up_questions": ["Which retrieval step dominates latency in production?"],
    "contribution": None,
    "method": None,
    "result": None,
    "limitation": None,
    "possible_extension": None,
}


def _summary_path(store: VaultStore, raw_doc_path: str) -> Path:
    return (store.root / raw_doc_path).parent / SUMMARY_ASSET_FILENAME


def test_import_url_with_summary_writes_summary_asset_for_new_manual_import(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.VaultLightweightEnrichmentService.enrich_stale_documents",
        lambda self, *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.summarize_item",
        lambda self, item, text: dict(SUMMARY_PAYLOAD),
    )

    response = authenticated_client.post(
        "/api/items/import-url-with-summary",
        json={"url": "https://medium.com/@example/production-rag-systems"},
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["source_id"] == "manual-import"
    assert payload["asset_paths"] == ["original.html", SUMMARY_ASSET_FILENAME]

    store = VaultStore()
    document = store.read_raw_document_relative(payload["raw_doc_path"])
    assert document is not None
    assert document.frontmatter.short_summary == SUMMARY_PAYLOAD["short_summary"]
    assert FULL_ARTICLE_SECTION_HEADING not in document.body

    summary_text = _summary_path(store, payload["raw_doc_path"]).read_text(encoding="utf-8")
    assert "# Main points" in summary_text
    assert SUMMARY_PAYLOAD["why_it_matters"] in summary_text


def test_local_control_import_url_with_summary_reuses_matching_medium_document(
    client: TestClient,
    monkeypatch,
) -> None:
    store = VaultStore()
    published_at = datetime(2026, 4, 8, 6, 40, tzinfo=UTC)
    canonical_url = "https://medium.com/@example/designing-a-production-grade-rag-architecture"
    frontmatter = RawDocumentFrontmatter(
        id="medium-derived-rag-article",
        kind="blog-post",
        title="Designing a Production-Grade RAG Architecture",
        source_url=canonical_url,
        source_name="Medium Email",
        authors=["Medium Daily Digest <noreply@medium.com>", "Matt Bentley"],
        published_at=published_at,
        ingested_at=published_at,
        content_hash="",
        tags=["newsletter", "medium"],
        status="active",
        asset_paths=[],
        source_id="medium-email",
        source_pipeline_id="medium-email",
        external_key=f"gmail-message-1::link::{canonical_url}",
        canonical_url=canonical_url,
        doc_role="derived",
        parent_id="medium-parent-newsletter",
        index_visibility="visible",
        fetched_at=published_at,
    )
    store.write_raw_document(
        kind=frontmatter.kind,
        doc_id=frontmatter.id,
        frontmatter=frontmatter,
        body=(
            "# Designing a Production-Grade RAG Architecture\n\n"
            "Source newsletter: Medium Daily Digest\n\n"
            "## Newsletter Context\n\n"
            "> Matt Bentley in Level Up Coding · 15 min read · 464 claps · 11 responses\n\n"
            "Techniques and best practices for grounding LLMs in production."
        ),
    )
    VaultIngestionService().rebuild_items_index(trigger="test_medium_summary_import")

    def _fake_extract(self, url: str, *, allow_insecure_tls: bool = False) -> ExtractedContent:
        if url == canonical_url:
            request = httpx.Request("GET", url)
            response = httpx.Response(403, request=request)
            raise httpx.HTTPStatusError("403 Forbidden", request=request, response=response)
        assert url == resolve_external_url(canonical_url)
        if not allow_insecure_tls:
            raise RuntimeError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed")
        return ExtractedContent(
            title="Designing a Production-Grade RAG Architecture",
            cleaned_text="Full article body from Medium about grounding, retrieval, and system design.",
            outbound_links=[],
            published_at=published_at,
            mime_type="text/html",
            extraction_confidence=0.93,
            raw_payload={"html": "<html><body>full medium article</body></html>"},
        )

    monkeypatch.setattr(
        "app.integrations.extractors.ContentExtractor.extract_from_url",
        _fake_extract,
    )
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.summarize_item",
        lambda self, item, text: dict(SUMMARY_PAYLOAD),
    )

    pairing = LocalControlService().create_pairing_code(label="Lab iPad")
    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    response = client.post(
        "/api/local-control/documents/import-url-with-summary",
        headers={"Authorization": f"Bearer {token}"},
        json={"url": canonical_url},
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["id"] == frontmatter.id
    assert payload["source_id"] == "medium-email"
    assert payload["asset_paths"] == ["main-points.md", "original.html"] or payload["asset_paths"] == ["original.html", "main-points.md"]

    document = store.read_raw_document_relative(payload["raw_doc_path"])
    assert document is not None
    assert document.frontmatter.short_summary == SUMMARY_PAYLOAD["short_summary"]
    assert FULL_ARTICLE_SECTION_HEADING in document.body
    assert "Full article body from Medium about grounding, retrieval, and system design." in document.body

    summary_text = _summary_path(store, payload["raw_doc_path"]).read_text(encoding="utf-8")
    assert SUMMARY_PAYLOAD["short_summary"] in summary_text
    assert SUMMARY_PAYLOAD["whats_new"] in summary_text
