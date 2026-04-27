from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

from app.api.deps import get_local_control_device
from app.core.config import get_settings
from app.integrations.zotero import ZoteroExportResult
from app.services.items import ItemService
from app.services.local_control import LocalControlService
from app.services.vault_ingestion import VaultIngestionService
from app.tests.support_publication import seed_publishable_vault
from app.vault.models import RawDocumentFrontmatter
from app.vault.store import VaultStore


def _build_request(*, client_host: str, authorization: str | None = None) -> Request:
    headers: list[tuple[bytes, bytes]] = []
    if authorization is not None:
        headers.append((b"authorization", authorization.encode("utf-8")))
    return Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "http",
            "path": "/api/local-control/status",
            "raw_path": b"/api/local-control/status",
            "query_string": b"",
            "headers": headers,
            "client": (client_host, 12345),
            "server": ("testserver", 80),
        }
    )


def test_local_control_allows_loopback_requests_without_pairing_token(
    client: TestClient,
) -> None:
    request = _build_request(client_host="127.0.0.1")

    device = get_local_control_device(request)

    assert device.label == "Local Mac"
    assert device.metadata_json["trusted_loopback"] is True
    assert device.last_seen_ip == "127.0.0.1"


def test_local_control_still_requires_pairing_token_for_non_loopback_requests(
    client: TestClient,
) -> None:
    request = _build_request(client_host="192.168.1.88")

    with pytest.raises(HTTPException) as exc_info:
        get_local_control_device(request)

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Local control token is required."


def test_local_control_requires_paired_token_and_returns_vault_status(
    client: TestClient,
) -> None:
    seeded = seed_publishable_vault()
    pairing = LocalControlService().create_pairing_code(label="Lab iPad")

    unauthenticated = client.get("/api/local-control/status")
    assert unauthenticated.status_code == 401

    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    payload = redeem.json()
    token = payload["access_token"]

    status_response = client.get(
        "/api/local-control/status",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["device_label"] == "Lab iPad"
    assert status_payload["vault_root_dir"]
    assert status_payload["viewer_bundle_dir"]
    assert status_payload["lightweight_pending_count"] == 1
    assert status_payload["lightweight_metadata_pending_count"] == 1
    assert status_payload["lightweight_scoring_pending_count"] == 0
    assert status_payload["items_index"]["up_to_date"] is True
    assert status_payload["items_index"]["stale_document_count"] == 0
    assert status_payload["items_index"]["indexed_item_count"] == 1
    assert status_payload["topic_count"] >= 1
    assert status_payload["rising_topic_count"] >= 1

    insights_response = client.get(
        "/api/local-control/insights",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert insights_response.status_code == 200
    insights_payload = insights_response.json()
    assert insights_payload["rising_topics"]

    documents_response = client.get(
        "/api/local-control/documents",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert documents_response.status_code == 200
    documents_payload = documents_response.json()
    assert documents_payload["total"] == 1
    assert len(documents_payload["items"]) == 1
    assert documents_payload["items"][0]["title"] == "Signal from the publishing test feed"

    detail_response = client.get(
        f"/api/local-control/documents/{documents_payload['items'][0]['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["title"] == "Signal from the publishing test feed"
    assert detail_payload["cleaned_text"]
    assert "short_summary" in detail_payload["insight"]

    sources_response = client.get(
        "/api/local-control/sources",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert sources_response.status_code == 200
    assert sources_response.json()

    availability_response = client.get(
        "/api/local-control/briefs/availability",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert availability_response.status_code == 200
    assert availability_response.json()["days"]

    today_brief_response = client.get(
        "/api/local-control/briefs/today",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert today_brief_response.status_code == 200
    today_brief_payload = today_brief_response.json()
    assert today_brief_payload["brief_date"] == seeded["brief_date"].isoformat()
    assert today_brief_payload["title"]
    assert today_brief_payload["editorial_shortlist"]

    dated_brief_response = client.get(
        f"/api/local-control/briefs/{seeded['brief_date'].isoformat()}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert dated_brief_response.status_code == 200
    assert dated_brief_response.json()["brief_date"] == seeded["brief_date"].isoformat()

    profile_response = client.get(
        "/api/local-control/profile",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert profile_response.status_code == 200
    assert profile_response.json()["timezone"] == "Europe/Zurich"

    profile_update = client.patch(
        "/api/local-control/profile",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "summary_depth": "deep",
            "audio_brief_settings": {"target_duration_minutes": 7},
        },
    )
    assert profile_update.status_code == 200
    updated_profile = profile_update.json()
    assert updated_profile["summary_depth"] == "deep"
    assert updated_profile["audio_brief_settings"]["target_duration_minutes"] == 7

    admin_response = client.post(
        "/api/ops/ingest-now",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert admin_response.status_code == 401


def test_local_control_documents_include_hidden_primary_newsletters(
    client: TestClient,
) -> None:
    store = VaultStore()
    published_at = datetime(2026, 4, 7, 8, 30, tzinfo=UTC)

    parent = RawDocumentFrontmatter(
        id="tldr-parent-newsletter",
        kind="newsletter",
        title="TLDR AI",
        source_url="https://mail.google.com/mail/u/0/#inbox/gmail-message-1",
        source_name="TLDR Email",
        authors=["TLDR AI <hi@tldrnewsletter.com>"],
        published_at=published_at,
        ingested_at=published_at,
        content_hash="",
        tags=["ai", "newsletter"],
        status="active",
        asset_paths=[],
        source_id="tldr-email",
        source_pipeline_id="tldr-email",
        external_key="gmail-message-1",
        canonical_url="https://mail.google.com/mail/u/0/#inbox/gmail-message-1",
        doc_role="primary",
        parent_id=None,
        index_visibility="hidden",
        fetched_at=published_at,
    )
    child = RawDocumentFrontmatter(
        id="tldr-derived-story",
        kind="news",
        title="A linked TLDR story",
        source_url="https://example.com/tldr-story",
        source_name="TLDR Email",
        authors=["TLDR AI <hi@tldrnewsletter.com>"],
        published_at=published_at,
        ingested_at=published_at,
        content_hash="",
        tags=["ai", "news"],
        status="active",
        asset_paths=[],
        source_id="tldr-email",
        source_pipeline_id="tldr-email",
        external_key="gmail-message-1::link::https://example.com/tldr-story",
        canonical_url="https://example.com/tldr-story",
        doc_role="derived",
        parent_id=parent.id,
        index_visibility="visible",
        fetched_at=published_at,
    )
    store.write_raw_document(
        kind=parent.kind,
        doc_id=parent.id,
        frontmatter=parent,
        body="# TLDR AI\n\n## Email Body\n\nThe full newsletter issue is preserved here.",
    )
    store.write_raw_document(
        kind=child.kind,
        doc_id=child.id,
        frontmatter=child,
        body="# A linked TLDR story\n\nThis is the derived child document.",
    )

    VaultIngestionService().rebuild_items_index(trigger="test_local_control_newsletters")

    public_items = ItemService().list_items(source_id="tldr-email")
    assert [item.id for item in public_items] == [child.id]

    local_items = ItemService().list_items(
        source_id="tldr-email",
        include_hidden_primary_newsletters=True,
    )
    assert {item.id for item in local_items} == {parent.id, child.id}

    pairing = LocalControlService().create_pairing_code(label="Lab iPad")
    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    documents_response = client.get(
        "/api/local-control/documents",
        headers={"Authorization": f"Bearer {token}"},
        params={"source_id": "tldr-email"},
    )
    assert documents_response.status_code == 200
    documents_payload = documents_response.json()
    assert documents_payload["total"] == 2
    assert {entry["id"] for entry in documents_payload["items"]} == {parent.id, child.id}

    newsletters_response = client.get(
        "/api/local-control/documents",
        headers={"Authorization": f"Bearer {token}"},
        params={"source_id": "tldr-email", "content_type": "newsletter"},
    )
    assert newsletters_response.status_code == 200
    newsletters_payload = newsletters_response.json()
    assert newsletters_payload["total"] == 1
    assert [entry["id"] for entry in newsletters_payload["items"]] == [parent.id]
    assert newsletters_payload["items"][0]["content_type"] == "newsletter"

    hidden_subdocuments_response = client.get(
        "/api/local-control/documents",
        headers={"Authorization": f"Bearer {token}"},
        params={"source_id": "tldr-email", "hide_sub_documents": "true"},
    )
    assert hidden_subdocuments_response.status_code == 200
    hidden_subdocuments_payload = hidden_subdocuments_response.json()
    assert hidden_subdocuments_payload["total"] == 1
    assert [entry["id"] for entry in hidden_subdocuments_payload["items"]] == [parent.id]


def test_local_control_documents_filter_by_date_range_and_oldest_sort(
    client: TestClient,
) -> None:
    store = VaultStore()
    dated_documents = [
        ("dated-doc-early", "Early document", datetime(2026, 4, 3, 12, 0, tzinfo=UTC)),
        ("dated-doc-middle", "Middle document", datetime(2026, 4, 5, 12, 0, tzinfo=UTC)),
        ("dated-doc-late", "Late document", datetime(2026, 4, 7, 12, 0, tzinfo=UTC)),
    ]

    for item_id, title, published_at in dated_documents:
        frontmatter = RawDocumentFrontmatter(
            id=item_id,
            kind="article",
            title=title,
            source_url=f"https://example.com/{item_id}",
            source_name="Date Filter Feed",
            authors=["Research Center"],
            published_at=published_at,
            ingested_at=published_at,
            content_hash="",
            tags=["date-filter"],
            status="active",
            asset_paths=[],
            source_id="date-filter-feed",
            source_pipeline_id="date-filter-feed",
            external_key=f"https://example.com/{item_id}",
            canonical_url=f"https://example.com/{item_id}",
            doc_role="primary",
            parent_id=None,
            index_visibility="visible",
            fetched_at=published_at,
        )
        store.write_raw_document(
            kind=frontmatter.kind,
            doc_id=frontmatter.id,
            frontmatter=frontmatter,
            body=f"# {title}\n\nUsed to verify date filtering and sort order.",
        )

    VaultIngestionService().rebuild_items_index(trigger="test_local_control_document_date_range")

    pairing = LocalControlService().create_pairing_code(label="Lab iPad")
    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    response = client.get(
        "/api/local-control/documents",
        headers={"Authorization": f"Bearer {token}"},
        params={
            "from": "2026-04-04",
            "to": "2026-04-07",
            "sort": "oldest",
            "source_id": "date-filter-feed",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert [entry["id"] for entry in payload["items"]] == ["dated-doc-middle", "dated-doc-late"]


def test_local_control_documents_support_pagination(
    client: TestClient,
) -> None:
    store = VaultStore()
    paged_documents = [
        ("pagination-doc-early", "Early page document", datetime(2026, 4, 3, 12, 0, tzinfo=UTC)),
        ("pagination-doc-middle", "Middle page document", datetime(2026, 4, 5, 12, 0, tzinfo=UTC)),
        ("pagination-doc-late", "Late page document", datetime(2026, 4, 7, 12, 0, tzinfo=UTC)),
    ]

    for item_id, title, published_at in paged_documents:
        frontmatter = RawDocumentFrontmatter(
            id=item_id,
            kind="article",
            title=title,
            source_url=f"https://example.com/{item_id}",
            source_name="Pagination Feed",
            authors=["Research Center"],
            published_at=published_at,
            ingested_at=published_at,
            content_hash="",
            tags=["pagination"],
            status="active",
            asset_paths=[],
            source_id="pagination-feed",
            source_pipeline_id="pagination-feed",
            external_key=f"https://example.com/{item_id}",
            canonical_url=f"https://example.com/{item_id}",
            doc_role="primary",
            parent_id=None,
            index_visibility="visible",
            fetched_at=published_at,
        )
        store.write_raw_document(
            kind=frontmatter.kind,
            doc_id=frontmatter.id,
            frontmatter=frontmatter,
            body=f"# {title}\n\nUsed to verify local-control pagination.",
        )

    VaultIngestionService().rebuild_items_index(trigger="test_local_control_document_pagination")

    pairing = LocalControlService().create_pairing_code(label="Lab iPad")
    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    response = client.get(
        "/api/local-control/documents",
        headers={"Authorization": f"Bearer {token}"},
        params={
            "source_id": "pagination-feed",
            "sort": "newest",
            "page": 2,
            "page_size": 2,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert [entry["id"] for entry in payload["items"]] == ["pagination-doc-early"]


def test_local_control_documents_can_be_marked_read_and_starred(
    client: TestClient,
) -> None:
    store = VaultStore()
    published_at = datetime(2026, 4, 6, 9, 0, tzinfo=UTC)
    frontmatter = RawDocumentFrontmatter(
        id="local-control-read-item",
        kind="article",
        title="Read tracking test document",
        source_url="https://example.com/read-tracking",
        source_name="Read Tracking Feed",
        authors=["Research Center"],
        published_at=published_at,
        ingested_at=published_at,
        content_hash="",
        tags=["read-tracking"],
        status="active",
        asset_paths=[],
        source_id="read-tracking-feed",
        source_pipeline_id="read-tracking-feed",
        external_key="https://example.com/read-tracking",
        canonical_url="https://example.com/read-tracking",
        doc_role="primary",
        parent_id=None,
        index_visibility="visible",
        fetched_at=published_at,
    )
    store.write_raw_document(
        kind=frontmatter.kind,
        doc_id=frontmatter.id,
        frontmatter=frontmatter,
        body="# Read tracking test document\n\nUsed to verify local-control read and star actions.",
    )

    VaultIngestionService().rebuild_items_index(trigger="test_local_control_read_and_star")

    pairing = LocalControlService().create_pairing_code(label="Lab iPad")
    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    initial_response = client.get(
        "/api/local-control/documents",
        headers=headers,
        params={"source_id": "read-tracking-feed"},
    )
    assert initial_response.status_code == 200
    initial_payload = initial_response.json()
    assert initial_payload["total"] == 1
    assert [entry["id"] for entry in initial_payload["items"]] == [frontmatter.id]
    assert initial_payload["items"][0]["read"] is False
    assert initial_payload["items"][0]["starred"] is False
    assert initial_payload["items"][0]["score_breakdown"]["relevance_score"] >= 0.0

    read_response = client.post(f"/api/local-control/documents/{frontmatter.id}/read", headers=headers)
    assert read_response.status_code == 200
    assert read_response.json()["detail"] == "Item marked as read."

    detail_response = client.get(f"/api/local-control/documents/{frontmatter.id}", headers=headers)
    assert detail_response.status_code == 200
    assert detail_response.json()["read"] is True
    assert detail_response.json()["starred"] is False

    star_response = client.post(f"/api/local-control/documents/{frontmatter.id}/star", headers=headers)
    assert star_response.status_code == 200
    assert star_response.json()["detail"] == "Marked as important."

    updated_response = client.get(
        "/api/local-control/documents",
        headers=headers,
        params={"source_id": "read-tracking-feed"},
    )
    assert updated_response.status_code == 200
    updated_payload = updated_response.json()
    assert updated_payload["items"][0]["read"] is True
    assert updated_payload["items"][0]["starred"] is True


def test_local_control_documents_can_be_saved_to_zotero(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )
    connection_response = authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "12345"},
            "metadata_json": {
                "library_type": "users",
                "collection_name": "Research Center / Papers",
                "auto_tag_vocabulary": [],
            },
        },
    )
    assert connection_response.status_code == 201

    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    assert created.status_code == 201
    item_id = created.json()["id"]

    captured: dict[str, object] = {}

    def _save(self, **kwargs):
        captured["collection_name"] = kwargs.get("collection_name")
        captured["tags"] = kwargs.get("tags")
        return ZoteroExportResult(
            success=True,
            confidence_score=0.92,
            detail="Saved to Zotero.",
            response_payload={"successful": {"0": "ABCD1234"}},
        )

    monkeypatch.setattr("app.integrations.zotero.ZoteroClient.save_item", _save)

    pairing = LocalControlService().create_pairing_code(label="Lab iPad")
    redeem = authenticated_client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    headers = {"Authorization": f"Bearer {redeem.json()['access_token']}"}

    save_response = authenticated_client.post(
        f"/api/local-control/documents/{item_id}/save-to-zotero",
        headers=headers,
        json={},
    )
    assert save_response.status_code == 200
    assert save_response.json()["triage_status"] == "saved"
    assert save_response.json()["detail"] == "Saved to Zotero."
    assert captured["collection_name"] == "Research Center / Papers"
    assert captured["tags"] == []

    detail_response = authenticated_client.get(f"/api/local-control/documents/{item_id}", headers=headers)
    assert detail_response.status_code == 200
    assert detail_response.json()["read"] is True


def test_local_control_rebuild_index_allows_duplicate_canonical_urls(
    client: TestClient,
) -> None:
    store = VaultStore()
    shared_url = "https://medium.com/@example/repeated-story"
    duplicate_documents = [
        (
            "duplicate-medium-story-1",
            "Repeated story (first mention)",
            datetime(2026, 4, 3, 12, 0, tzinfo=UTC),
        ),
        (
            "duplicate-medium-story-2",
            "Repeated story (second mention)",
            datetime(2026, 4, 8, 12, 0, tzinfo=UTC),
        ),
    ]

    for item_id, title, published_at in duplicate_documents:
        frontmatter = RawDocumentFrontmatter(
            id=item_id,
            kind="blog-post",
            title=title,
            source_url=shared_url,
            source_name="Medium Email",
            authors=["Research Center"],
            published_at=published_at,
            ingested_at=published_at,
            content_hash="",
            tags=["duplicate-url"],
            status="active",
            asset_paths=[],
            source_id="medium-email",
            source_pipeline_id="medium-email",
            external_key=f"{published_at.date().isoformat()}::{shared_url}",
            canonical_url=shared_url,
            doc_role="primary",
            parent_id=None,
            index_visibility="visible",
            fetched_at=published_at,
        )
        store.write_raw_document(
            kind=frontmatter.kind,
            doc_id=frontmatter.id,
            frontmatter=frontmatter,
            body=f"# {title}\n\nStored to verify duplicate canonical URLs remain indexable.",
        )

    pairing = LocalControlService().create_pairing_code(label="Lab iPad")
    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    rebuild_response = client.post(
        "/api/local-control/jobs/rebuild-items-index",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert rebuild_response.status_code == 200
    assert rebuild_response.json()["task_name"] == "rebuild_items_index"

    status_response = client.get(
        "/api/local-control/status",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["items_index"]["up_to_date"] is True
    assert status_payload["items_index"]["indexed_item_count"] == 2

    documents_response = client.get(
        "/api/local-control/documents",
        headers={"Authorization": f"Bearer {token}"},
        params={"source_id": "medium-email"},
    )
    assert documents_response.status_code == 200
    documents_payload = documents_response.json()
    assert documents_payload["total"] == 2
    assert {entry["id"] for entry in documents_payload["items"]} == {
        "duplicate-medium-story-1",
        "duplicate-medium-story-2",
    }
    assert {entry["canonical_url"] for entry in documents_payload["items"]} == {shared_url}


def test_local_control_publish_job_uses_paired_device_and_returns_publication_summary(
    client: TestClient,
) -> None:
    seeded = seed_publishable_vault()
    pairing = LocalControlService().create_pairing_code(label="Control iPad")

    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    response = client.post(
        "/api/local-control/jobs/publish",
        headers={"Authorization": f"Bearer {token}"},
        json={"brief_date": seeded["brief_date"].isoformat()},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["queued"] is False
    assert payload["task_name"] == "publish"
    assert payload["published_edition"]["edition_id"] == f"day:{seeded['brief_date'].isoformat()}"

    operations = client.get(
        "/api/local-control/operations",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert operations.status_code == 200
    assert any(entry["operation_kind"] == "viewer_publish" for entry in operations.json()["runs"])


def test_local_control_status_counts_only_never_enriched_documents(
    client: TestClient,
) -> None:
    seed_publishable_vault()
    store = VaultStore()
    document = store.read_raw_document_relative("raw/article/publish-fixture-item/source.md")
    assert document is not None
    updated_frontmatter = document.frontmatter.model_copy(
        update={
            "lightweight_enrichment_status": "succeeded",
            "lightweight_enriched_at": document.frontmatter.ingested_at,
            "lightweight_enrichment_model": "gemma4:e2b",
            "lightweight_enrichment_input_hash": "test-enrichment-hash",
        }
    )
    store.write_raw_document(
        kind=updated_frontmatter.kind,
        doc_id=updated_frontmatter.id,
        frontmatter=updated_frontmatter,
        body=document.body,
    )
    pairing = LocalControlService().create_pairing_code(label="Lab iPad")

    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    status_response = client.get(
        "/api/local-control/status",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert status_response.status_code == 200
    assert status_response.json()["lightweight_pending_count"] == 0


def test_local_control_sync_vault_job_uses_scoped_local_control_sync(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairing = LocalControlService().create_pairing_code(label="Scoped Sync iPad")

    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    called = {"count": 0}

    def _fake_sync(self) -> None:
        called["count"] += 1

    monkeypatch.setattr(
        "app.services.vault_operations.VaultOperationService.synchronize_local_control",
        _fake_sync,
    )

    response = client.post(
        "/api/local-control/jobs/sync-vault",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    assert called["count"] == 1
    assert "wiki" in response.json()["detail"].lower()


def test_local_control_source_inject_uses_source_pipeline_with_max_items_override(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairing = LocalControlService().create_pairing_code(label="Source Fetch iPad")

    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    called: dict[str, object] = {}

    def _fake_run_source_pipeline(
        self,
        *,
        source_id: str,
        max_items: int | None = None,
        alphaxiv_sort: str | None = None,
    ) -> str:
        called["source_id"] = source_id
        called["max_items"] = max_items
        called["alphaxiv_sort"] = alphaxiv_sort
        return "source-run-123"

    monkeypatch.setattr(
        "app.services.vault_operations.VaultOperationService.run_source_pipeline",
        _fake_run_source_pipeline,
    )

    response = client.post(
        "/api/local-control/jobs/sources/openai-website/inject",
        headers={"Authorization": f"Bearer {token}"},
        json={"max_items": 42},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_name"] == "source_inject"
    assert payload["operation_run_id"] == "source-run-123"
    assert payload["detail"] == (
        "Source fetch completed for OpenAI Website with a cap of 42 documents. "
        "Lightweight enrichment and index refresh remain manual."
    )
    assert called == {"source_id": "openai-website", "max_items": 42, "alphaxiv_sort": None}


def test_local_control_source_inject_accepts_alphaxiv_sort_override(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairing = LocalControlService().create_pairing_code(label="AlphaXiv Fetch iPad")

    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    called: dict[str, object] = {}

    def _fake_run_source_pipeline(
        self,
        *,
        source_id: str,
        max_items: int | None = None,
        alphaxiv_sort: str | None = None,
    ) -> str:
        called["source_id"] = source_id
        called["max_items"] = max_items
        called["alphaxiv_sort"] = alphaxiv_sort
        return "alphaxiv-run-456"

    monkeypatch.setattr(
        "app.services.vault_operations.VaultOperationService.run_source_pipeline",
        _fake_run_source_pipeline,
    )

    response = client.post(
        "/api/local-control/jobs/sources/alphaxiv-paper/inject",
        headers={"Authorization": f"Bearer {token}"},
        json={"max_items": 18, "alphaxiv_sort": "Likes"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_name"] == "source_inject"
    assert payload["operation_run_id"] == "alphaxiv-run-456"
    assert payload["detail"] == (
        "Source fetch completed for alphaXiv Papers with a cap of 18 documents using alphaXiv Likes sort. "
        "Lightweight enrichment and index refresh remain manual."
    )
    assert called == {
        "source_id": "alphaxiv-paper",
        "max_items": 18,
        "alphaxiv_sort": "Likes",
    }


def test_local_control_source_stop_requests_running_fetch(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairing = LocalControlService().create_pairing_code(label="Stop Fetch iPad")

    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    called: dict[str, object] = {}

    class _Run:
        id = "source-run-456"

    def _fake_request_stop_for_source(self, *, source_id: str):
        called["source_id"] = source_id
        return _Run()

    monkeypatch.setattr(
        "app.services.vault_operations.VaultOperationService.request_stop_for_source",
        _fake_request_stop_for_source,
    )

    response = client.post(
        "/api/local-control/jobs/sources/openai-website/stop",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_name"] == "stop_source_fetch"
    assert payload["operation_run_id"] == "source-run-456"
    assert "Stop requested for OpenAI Website" in payload["detail"]
    assert called == {"source_id": "openai-website"}


def test_local_control_lightweight_stop_requests_running_enrichment(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairing = LocalControlService().create_pairing_code(label="Stop Lightweight iPad")

    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    called: dict[str, object] = {}

    class _Run:
        id = "lightweight-run-789"

    def _fake_request_stop_for_lightweight(self):
        called["requested"] = True
        return _Run()

    monkeypatch.setattr(
        "app.services.vault_operations.VaultOperationService.request_stop_for_lightweight",
        _fake_request_stop_for_lightweight,
    )

    response = client.post(
        "/api/local-control/jobs/lightweight-enrich/stop",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_name"] == "stop_lightweight_enrich"
    assert payload["operation_run_id"] == "lightweight-run-789"
    assert "Stop requested for lightweight enrichment" in payload["detail"]
    assert called == {"requested": True}


def test_local_control_lightweight_metadata_job_uses_metadata_phase(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairing = LocalControlService().create_pairing_code(label="Metadata iPad")

    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    called: dict[str, object] = {}

    class _Run:
        id = "lightweight-metadata-run-123"
        summary = "Lightweight metadata refresh updated 3 documents."

    def _fake_lightweight_metadata_enrich(self):
        called["requested"] = True
        return _Run()

    monkeypatch.setattr(
        "app.services.vault_operations.VaultOperationService.lightweight_metadata_enrich",
        _fake_lightweight_metadata_enrich,
    )

    response = client.post(
        "/api/local-control/jobs/lightweight-metadata",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_name"] == "lightweight_metadata"
    assert payload["operation_run_id"] == "lightweight-metadata-run-123"
    assert payload["detail"] == "Lightweight metadata refresh updated 3 documents."
    assert called == {"requested": True}


def test_local_control_lightweight_scoring_job_uses_scoring_phase(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairing = LocalControlService().create_pairing_code(label="Scoring iPad")

    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    called: dict[str, object] = {}

    class _Run:
        id = "lightweight-scoring-run-456"
        summary = "Lightweight scoring refresh updated 2 documents."

    def _fake_lightweight_scoring_enrich(self):
        called["requested"] = True
        return _Run()

    monkeypatch.setattr(
        "app.services.vault_operations.VaultOperationService.lightweight_scoring_enrich",
        _fake_lightweight_scoring_enrich,
    )

    response = client.post(
        "/api/local-control/jobs/lightweight-scoring",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_name"] == "lightweight_scoring"
    assert payload["operation_run_id"] == "lightweight-scoring-run-456"
    assert payload["detail"] == "Lightweight scoring refresh updated 2 documents."
    assert called == {"requested": True}


def test_local_control_rejects_expired_access_tokens_and_revokes_them(
    client: TestClient,
) -> None:
    pairing = LocalControlService().create_pairing_code(label="Expired iPad")

    redeem = client.post(
        "/api/local-control/pair/redeem",
        json={"pairing_token": pairing.pairing_token},
    )
    assert redeem.status_code == 200
    token = redeem.json()["access_token"]

    store = VaultStore()
    devices = store.load_paired_devices()
    assert len(devices.devices) == 1
    devices.devices[0].paired_at = datetime.now(UTC) - timedelta(
        days=get_settings().local_control_token_max_age_days + 1
    )
    store.save_paired_devices(devices)

    status_response = client.get(
        "/api/local-control/status",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert status_response.status_code == 401
    assert "expired" in status_response.json()["detail"].lower()

    refreshed = store.load_paired_devices()
    assert refreshed.devices[0].revoked_at is not None
