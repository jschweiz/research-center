from __future__ import annotations

import json

from app.services.vault_publishing import VaultPublisherService
from app.tests.support_publication import seed_publishable_vault
from app.vault.store import VaultStore


def test_publisher_build_manifest_strips_private_fields_and_is_stable(client) -> None:
    seeded = seed_publishable_vault()
    brief_date = seeded["brief_date"]

    digest = VaultPublisherService().briefs.get_digest_by_date(brief_date)
    assert digest is not None

    manifest = VaultPublisherService().build_manifest(digest)
    item_payload = manifest.items[next(iter(manifest.items))]
    serialized_once = json.dumps(manifest.model_dump(mode="json"), sort_keys=True)
    serialized_twice = json.dumps(manifest.model_dump(mode="json"), sort_keys=True)

    assert "triage_status" not in serialized_once
    assert "starred" not in serialized_once
    assert "raw_payload_retention_until" not in serialized_once
    assert "zotero_matches" not in serialized_once
    assert item_payload.insight.short_summary is not None
    assert serialized_once == serialized_twice


def test_publish_latest_writes_viewer_bundle_inside_vault(client) -> None:
    seeded = seed_publishable_vault()
    summary = VaultPublisherService().publish_latest()
    store = VaultStore()

    edition_slug = summary.record_name

    assert summary.edition_id == f"day:{seeded['brief_date'].isoformat()}"
    assert (store.viewer_dir / "index.html").exists()
    assert (store.viewer_dir / "archive.json").exists()
    assert (store.viewer_dir / "latest" / "index.html").exists()
    assert (store.viewer_dir / "latest" / "manifest.json").exists()
    assert (store.viewer_dir / "latest" / "brief.md").exists()
    assert (store.viewer_dir / "latest" / "brief.json").exists()
    assert (store.viewer_dir / "history" / edition_slug / "index.html").exists()
    assert (store.viewer_dir / "history" / edition_slug / "items").is_dir()

    published_index = store.load_published_index()
    assert published_index.latest is not None
    assert published_index.latest.edition_id == summary.edition_id
