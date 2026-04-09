from __future__ import annotations

from datetime import date

from app.db.models import ContentType
from app.core.outbound import UnsafeOutboundUrlError
from app.schemas.items import ActionRead, ItemDetailRead, ItemListEntry
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_alphaxiv import AlphaXivPaperResolver
from app.services.vault_runtime import (
    brief_priority_key,
    infer_content_type,
    to_item_detail,
    to_item_list_entry,
)
from app.vault.store import VaultStore


class VaultItemService:
    def __init__(self) -> None:
        self.store = VaultStore()
        self.ingestion = VaultIngestionService()
        self.store.ensure_layout()

    def list_items(
        self,
        *,
        query: str | None = None,
        status_filter: str | None = None,
        content_type: str | None = None,
        source_id: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        sort: str = "importance",
        include_hidden_primary_newsletters: bool = False,
    ) -> list[ItemListEntry]:
        items = self.store.query_items(
            query=query,
            status_filter=status_filter,
            content_type=content_type,
            source_id=source_id,
            date_from=date_from,
            date_to=date_to,
            sort=sort,
        )
        items = [
            item
            for item in items
            if item.index_visibility != "hidden"
            or (
                include_hidden_primary_newsletters
                and item.content_type == ContentType.NEWSLETTER
                and item.doc_role == "primary"
            )
        ]
        starred_ids = self._starred_ids()
        if sort == "importance":
            items.sort(key=brief_priority_key, reverse=True)
        return [to_item_list_entry(item, starred=item.id in starred_ids) for item in items]

    def get_item_detail(self, item_id: str) -> ItemDetailRead | None:
        item = self.store.get_item(item_id)
        if not item:
            return None
        items = self.store.load_items_index().items
        raw = self.store.read_raw_document_relative(item.raw_doc_path)
        if raw is not None:
            item = item.model_copy(
                update={
                    "cleaned_text": raw.body,
                    "asset_paths": raw.frontmatter.asset_paths,
                    "short_summary": raw.frontmatter.short_summary,
                    "lightweight_enrichment_status": raw.frontmatter.lightweight_enrichment_status,
                    "lightweight_enriched_at": raw.frontmatter.lightweight_enriched_at,
                    "content_type": infer_content_type(
                        raw.frontmatter.kind,
                        raw.frontmatter.title,
                        raw.frontmatter.canonical_url or raw.frontmatter.source_url,
                        raw.body,
                    ),
                }
            )
        resolver = AlphaXivPaperResolver(store=self.store, items=items)
        alphaxiv = resolver.resolve(item, raw_document=raw)
        return to_item_detail(item, starred=item.id in self._starred_ids(), alphaxiv=alphaxiv)

    def get_item_detail_readonly(self, item_id: str) -> ItemDetailRead | None:
        return self.get_item_detail(item_id)

    def import_url(self, url: str) -> ItemDetailRead:
        try:
            item = self.ingestion.import_url(url)
        except UnsafeOutboundUrlError:
            raise
        return to_item_detail(item, starred=item.id in self._starred_ids())

    def archive_item(self, item_id: str) -> ActionRead | None:
        item = self.store.get_item(item_id)
        if item is None:
            return None
        if item.status == "archived":
            return ActionRead(item_id=item_id, triage_status="archived", detail="Item already archived.")

        document = self.store.read_raw_document_relative(item.raw_doc_path)
        if document is None:
            raise RuntimeError(f"Raw document missing for item '{item_id}'.")

        updated_frontmatter = document.frontmatter.model_copy(update={"status": "archived"})
        self.store.write_raw_document(
            kind=updated_frontmatter.kind,
            doc_id=updated_frontmatter.id,
            frontmatter=updated_frontmatter,
            body=document.body,
        )
        self.ingestion.rebuild_items_index(trigger="manual_archive")
        return ActionRead(item_id=item_id, triage_status="archived", detail="Item archived.")

    def toggle_star(self, item_id: str) -> ActionRead | None:
        item = self.store.get_item(item_id)
        if item is None:
            return None

        state = self.store.load_starred_items()
        starred_ids = [entry_id for entry_id in state.item_ids if entry_id != item_id]
        if item_id in state.item_ids:
            state.item_ids = starred_ids
            detail = "Removed from important items."
        else:
            state.item_ids = [item_id, *starred_ids]
            detail = "Marked as important."
        self.store.save_starred_items(state)
        triage_status = "archived" if item.status == "archived" else "unread"
        return ActionRead(item_id=item_id, triage_status=triage_status, detail=detail)

    def _starred_ids(self) -> set[str]:
        return set(self.store.load_starred_items().item_ids)
