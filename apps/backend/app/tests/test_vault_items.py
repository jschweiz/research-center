from __future__ import annotations

from fastapi.testclient import TestClient


def _digest_item_ids(payload: dict[str, object]) -> list[str]:
    item_ids: list[str] = []
    for section_name in (
        "editorial_shortlist",
        "headlines",
        "interesting_side_signals",
        "remaining_reads",
        "papers_table",
    ):
        for entry in payload.get(section_name, []):
            item_ids.append(entry["item"]["id"])
    return item_ids


def _find_digest_entry(payload: dict[str, object], item_id: str) -> dict[str, object] | None:
    for section_name in (
        "editorial_shortlist",
        "headlines",
        "interesting_side_signals",
        "remaining_reads",
        "papers_table",
    ):
        for entry in payload.get(section_name, []):
            if entry["item"]["id"] == item_id:
                return entry
    return None


def test_star_and_archive_actions_update_item_views_and_cached_digests(
    authenticated_client: TestClient,
    fake_extractor: None,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.VaultLightweightEnrichmentService.enrich_stale_documents",
        lambda self, *args, **kwargs: None,
    )

    created = authenticated_client.post("/api/items/import-url", json={"url": "https://example.com/article"})
    assert created.status_code == 201
    item_id = created.json()["id"]

    initial_digest = authenticated_client.get("/api/briefs/today")
    assert initial_digest.status_code == 200
    initial_entry = _find_digest_entry(initial_digest.json(), item_id)
    assert initial_entry is not None
    assert initial_entry["item"]["starred"] is False

    starred = authenticated_client.post(f"/api/items/{item_id}/star")
    assert starred.status_code == 200
    assert starred.json()["detail"] == "Marked as important."

    listing = authenticated_client.get("/api/items")
    assert listing.status_code == 200
    assert listing.json()[0]["starred"] is True

    detail = authenticated_client.get(f"/api/items/{item_id}")
    assert detail.status_code == 200
    assert detail.json()["starred"] is True

    hydrated_digest = authenticated_client.get("/api/briefs/today")
    assert hydrated_digest.status_code == 200
    hydrated_entry = _find_digest_entry(hydrated_digest.json(), item_id)
    assert hydrated_entry is not None
    assert hydrated_entry["item"]["starred"] is True

    unstarred = authenticated_client.post(f"/api/items/{item_id}/star")
    assert unstarred.status_code == 200
    assert unstarred.json()["detail"] == "Removed from important items."
    assert authenticated_client.get(f"/api/items/{item_id}").json()["starred"] is False

    archived = authenticated_client.post(f"/api/items/{item_id}/archive")
    assert archived.status_code == 200
    assert archived.json()["detail"] == "Item archived."

    default_listing = authenticated_client.get("/api/items")
    assert default_listing.status_code == 200
    assert default_listing.json() == []

    archived_listing = authenticated_client.get("/api/items", params={"status": "archived"})
    assert archived_listing.status_code == 200
    assert [item["id"] for item in archived_listing.json()] == [item_id]

    archived_digest = authenticated_client.get("/api/briefs/today")
    assert archived_digest.status_code == 200
    assert item_id not in _digest_item_ids(archived_digest.json())
