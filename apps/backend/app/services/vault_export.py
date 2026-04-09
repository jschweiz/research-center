from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.models import Item, ItemCluster
from app.db.session import get_session_factory
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_runtime import content_hash, document_identity_hash, utcnow
from app.vault.models import RawDocumentFrontmatter
from app.vault.store import VaultStore


@dataclass(frozen=True)
class VaultExportResult:
    exported_items: int
    vault_root: str


class VaultExporter:
    def __init__(self) -> None:
        self.store = VaultStore()
        self.ingestion = VaultIngestionService()
        self.store.ensure_layout()

    def export_from_sqlite(self) -> VaultExportResult:
        with get_session_factory()() as db:
            items = list(
                db.scalars(
                    select(Item).options(
                        selectinload(Item.content),
                        selectinload(Item.score),
                        selectinload(Item.insight),
                        selectinload(Item.cluster).selectinload(ItemCluster.items),
                    )
                ).all()
            )

        for item in items:
            kind = item.content_type.value
            body = (item.content.cleaned_text if item.content else None) or item.title
            frontmatter = RawDocumentFrontmatter(
                id=item.id,
                kind=kind,
                title=item.title,
                source_url=item.canonical_url,
                source_name=item.source_name,
                authors=item.authors,
                published_at=item.published_at,
                ingested_at=item.updated_at or item.created_at or utcnow(),
                content_hash=item.content_hash or content_hash(item.title, body),
                identity_hash=item.identity_hash
                or document_identity_hash(
                    source_id=item.source_id,
                    canonical_url=item.canonical_url,
                    fallback_key=item.id,
                ),
                tags=[str(tag) for tag in (item.metadata_json.get("tags") or []) if str(tag).strip()]
                if isinstance(item.metadata_json, dict)
                else [],
                status="archived" if item.triage_status.value == "archived" else "active",
                asset_paths=[],
            )
            self.store.write_raw_document(
                kind=kind,
                doc_id=item.id,
                frontmatter=frontmatter,
                body=body,
            )

        self.ingestion.rebuild_items_index(trigger="migration_export")
        return VaultExportResult(
            exported_items=len(items),
            vault_root=str(self.store.root),
        )
