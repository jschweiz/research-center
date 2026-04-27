from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.integrations.extractors import ExtractedContent
from app.services.local_control import LocalControlService
from app.vault.store import VaultStore


def _pair_headers(client: TestClient) -> dict[str, str]:
    pairing = LocalControlService().create_pairing_code(label="Chrome Extension")
    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def _captured_body(prefix: str = "Captured page") -> str:
    return "\n\n".join(
        [
            f"{prefix} title",
            (
                "This captured page contains enough visible text to populate source.md without "
                "falling back to a second network fetch. It includes product details, release "
                "notes, and a few editorial paragraphs that a reader would actually see in the tab."
            ),
            (
                "A second paragraph makes the capture more realistic and keeps the body above the "
                "minimum threshold for a useful import."
            ),
        ]
    )


def _load_capture_json(raw_doc_path: str) -> dict[str, object]:
    store = VaultStore()
    raw = store.read_raw_document_relative(raw_doc_path)
    assert raw is not None
    capture_path = (store.root / raw.path).parent / "capture.json"
    return json.loads(capture_path.read_text(encoding="utf-8"))


def test_local_control_import_page_creates_source_md_and_seeds_metadata(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.VaultLightweightEnrichmentService.enrich_stale_documents",
        lambda self, *args, **kwargs: None,
    )

    response = client.post(
        "/api/local-control/documents/import-page",
        headers=_pair_headers(client),
        json={
            "url": "https://example.com/articles/captured-page",
            "canonical_url": "https://example.com/articles/captured-page?ref=canonical",
            "page_title": "Captured Page Title",
            "site_name": "Example Research",
            "description": "A captured page summary from the browser.",
            "published_at": "2026-04-09T12:00:00Z",
            "author_hints": ["Ada Lovelace"],
            "byline": "By Grace Hopper",
            "language": "en",
            "extraction_mode": "readability",
            "content_text": _captured_body(),
            "article_html": "<article><p>Captured HTML body.</p></article>",
        },
    )

    assert response.status_code == 201
    payload = response.json()
    raw = VaultStore().read_raw_document_relative(payload["raw_doc_path"])
    assert raw is not None
    assert raw.frontmatter.title == "Captured Page Title"
    assert raw.frontmatter.source_name == "Example Research"
    assert raw.frontmatter.authors == ["Ada Lovelace", "Grace Hopper"]
    assert raw.frontmatter.short_summary == "A captured page summary from the browser."
    assert raw.frontmatter.published_at == datetime(2026, 4, 9, 12, 0, tzinfo=UTC)
    assert raw.frontmatter.canonical_url == "https://example.com/articles/captured-page"
    assert raw.frontmatter.asset_paths == ["capture.json", "captured.html"]
    assert raw.body.startswith("# Captured Page Title")
    assert "populate source.md without falling back" in raw.body

    capture_payload = _load_capture_json(payload["raw_doc_path"])
    assert capture_payload["body_source"] == "captured_text"
    assert capture_payload["extraction_mode"] == "readability"


def test_local_control_import_page_is_idempotent_and_updates_existing_document(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.VaultLightweightEnrichmentService.enrich_stale_documents",
        lambda self, *args, **kwargs: None,
    )
    headers = _pair_headers(client)

    first = client.post(
        "/api/local-control/documents/import-page",
        headers=headers,
        json={
            "url": "https://example.com/articles/reimport-me",
            "page_title": "Initial Title",
            "content_text": _captured_body("Initial"),
        },
    )
    assert first.status_code == 201

    second = client.post(
        "/api/local-control/documents/import-page",
        headers=headers,
        json={
            "url": "https://example.com/articles/reimport-me",
            "page_title": "Updated Title",
            "content_text": _captured_body("Updated"),
        },
    )
    assert second.status_code == 201
    assert second.json()["id"] == first.json()["id"]

    store = VaultStore()
    manual_docs = [
        document
        for document in store.list_raw_documents()
        if document.frontmatter.source_id == "manual-import"
    ]
    assert len(manual_docs) == 1
    assert manual_docs[0].frontmatter.id == first.json()["id"]
    assert manual_docs[0].frontmatter.title == "Updated Title"
    assert manual_docs[0].body.startswith("# Updated Title")
    assert "populate source.md without falling back" in manual_docs[0].body


def test_local_control_import_page_ignores_cross_origin_canonical_hint(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.VaultLightweightEnrichmentService.enrich_stale_documents",
        lambda self, *args, **kwargs: None,
    )

    response = client.post(
        "/api/local-control/documents/import-page",
        headers=_pair_headers(client),
        json={
            "url": "https://example.com/articles/canonical-check",
            "canonical_url": "https://elsewhere.com/articles/canonical-check",
            "page_title": "Canonical Check",
            "content_text": _captured_body(),
        },
    )

    assert response.status_code == 201
    raw = VaultStore().read_raw_document_relative(response.json()["raw_doc_path"])
    assert raw is not None
    assert raw.frontmatter.canonical_url == "https://example.com/articles/canonical-check"


def test_local_control_import_page_falls_back_to_server_fetch_when_capture_is_thin(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.VaultLightweightEnrichmentService.enrich_stale_documents",
        lambda self, *args, **kwargs: None,
    )

    def _fake_extract(self, url: str) -> ExtractedContent:
        assert url == "https://example.com/articles/fetch-me"
        return ExtractedContent(
            title="Fetched Server Title",
            cleaned_text=_captured_body("Fetched"),
            outbound_links=[],
            published_at=datetime(2026, 4, 8, 9, 30, tzinfo=UTC),
            mime_type="text/html",
            extraction_confidence=0.88,
            raw_payload={"html": "<html><body>Fetched HTML</body></html>", "fetched_url": url},
        )

    monkeypatch.setattr(
        "app.integrations.extractors.ContentExtractor.extract_from_url",
        _fake_extract,
    )

    response = client.post(
        "/api/local-control/documents/import-page",
        headers=_pair_headers(client),
        json={
            "url": "https://example.com/articles/fetch-me",
            "page_title": "",
            "content_text": "short",
        },
    )

    assert response.status_code == 201
    raw = VaultStore().read_raw_document_relative(response.json()["raw_doc_path"])
    assert raw is not None
    assert raw.frontmatter.title == "Fetched Server Title"
    assert raw.frontmatter.asset_paths == ["capture.json", "original.html"]
    assert raw.body.startswith("# Fetched Server Title")
    assert "Fetched title" in raw.body

    capture_payload = _load_capture_json(response.json()["raw_doc_path"])
    assert capture_payload["body_source"] == "server_fetch"
    assert capture_payload["extraction_mode"] == "server-fetch"


def test_local_control_import_page_creates_placeholder_when_capture_and_fetch_fail(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.VaultLightweightEnrichmentService.enrich_stale_documents",
        lambda self, *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "app.integrations.extractors.ContentExtractor.extract_from_url",
        lambda self, url: (_ for _ in ()).throw(RuntimeError("403 Forbidden")),
    )

    response = client.post(
        "/api/local-control/documents/import-page",
        headers=_pair_headers(client),
        json={
            "url": "https://example.com/articles/protected-page",
            "page_title": "Protected Page",
            "content_text": "tiny",
        },
    )

    assert response.status_code == 201
    raw = VaultStore().read_raw_document_relative(response.json()["raw_doc_path"])
    assert raw is not None
    assert "could not extract the full text" in raw.body
    assert raw.frontmatter.asset_paths == ["capture.json"]

    capture_payload = _load_capture_json(response.json()["raw_doc_path"])
    assert capture_payload["body_source"] == "placeholder"
    assert capture_payload["fetch_error"] == "403 Forbidden"


def test_local_control_import_page_triggers_single_document_lightweight_enrichment(
    client: TestClient,
    monkeypatch,
) -> None:
    calls: list[dict[str, object]] = []

    def _fake_enrich(self, *, trigger: str, source_id=None, doc_id=None, force: bool = False):
        calls.append({"trigger": trigger, "doc_id": doc_id, "source_id": source_id, "force": force})
        store = VaultStore()
        raw = store.find_raw_document(source_id="manual-import", external_key="https://example.com/articles/enrich-me")
        assert raw is not None
        updated = raw.frontmatter.model_copy(
            update={
                "authors": ["Local Gemma"],
                "tags": ["captured", "browser"],
                "short_summary": "Local Gemma enriched this captured page.",
                "lightweight_enrichment_status": "succeeded",
                "lightweight_enriched_at": datetime(2026, 4, 9, 14, 0, tzinfo=UTC),
                "lightweight_enrichment_model": "gemma4:e2b",
            }
        )
        store.write_raw_document(
            kind=updated.kind,
            doc_id=updated.id,
            frontmatter=updated,
            body=raw.body,
        )

    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.VaultLightweightEnrichmentService.enrich_stale_documents",
        _fake_enrich,
    )

    response = client.post(
        "/api/local-control/documents/import-page",
        headers=_pair_headers(client),
        json={
            "url": "https://example.com/articles/enrich-me",
            "page_title": "Enrich Me",
            "content_text": _captured_body(),
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert calls == [
        {
            "trigger": "manual_capture_import",
            "doc_id": payload["id"],
            "source_id": None,
            "force": False,
        }
    ]
    assert payload["authors"] == ["Local Gemma"]
    assert payload["insight"]["short_summary"] == "Local Gemma enriched this captured page"
    assert payload["lightweight_enrichment_status"] == "succeeded"
