from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from app.core.external_urls import FREEDIUM_MIRROR_PREFIX, resolve_external_url
from app.core.outbound import UnsafeOutboundUrlError
from app.db.models import ConnectionProvider, ContentType
from app.db.session import get_session_factory
from app.integrations.extractors import ExtractedContent
from app.integrations.llm import LLMClient
from app.integrations.zotero import ZoteroClient
from app.schemas.items import ActionRead, CapturedPageImportRequest, ItemDetailRead, ItemListEntry
from app.services.connections import ConnectionService
from app.services.vault_alphaxiv import AlphaXivPaperResolver
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_runtime import (
    brief_priority_key,
    content_hash,
    infer_content_type,
    to_item_detail,
    to_item_list_entry,
    utcnow,
)
from app.services.zotero_auto_tags import merge_zotero_tags, resolve_zotero_auto_tag_vocabulary
from app.vault.models import RawDocument, SUB_DOCUMENT_TAG, VaultItemRecord
from app.vault.store import VaultStore

FULL_ARTICLE_SECTION_HEADING = "## Full article text"
SUMMARY_ASSET_FILENAME = "main-points.md"


class ItemSummaryImportError(RuntimeError):
    pass


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
        include_sub_documents: bool = True,
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
        if not include_sub_documents:
            items = [item for item in items if not self._is_sub_document(item)]
        read_ids = self._read_ids()
        starred_ids = self._starred_ids()
        if sort == "importance":
            items.sort(key=brief_priority_key, reverse=True)
        return [
            to_item_list_entry(
                item,
                read=item.id in read_ids,
                starred=item.id in starred_ids,
            )
            for item in items
        ]

    def list_items_page(
        self,
        *,
        query: str | None = None,
        status_filter: str | None = None,
        content_type: str | None = None,
        source_id: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        sort: str = "importance",
        page: int = 1,
        page_size: int = 50,
        include_hidden_primary_newsletters: bool = False,
        include_sub_documents: bool = True,
    ) -> tuple[list[ItemListEntry], int]:
        offset = max(page - 1, 0) * page_size
        items, total = self.store.query_items_page(
            query=query,
            status_filter=status_filter,
            content_type=content_type,
            source_id=source_id,
            date_from=date_from,
            date_to=date_to,
            sort=sort,
            include_hidden_primary_newsletters=include_hidden_primary_newsletters,
            include_sub_documents=include_sub_documents,
            offset=offset,
            limit=page_size,
        )
        read_ids = self._read_ids()
        starred_ids = self._starred_ids()
        return (
            [
                to_item_list_entry(
                    item,
                    read=item.id in read_ids,
                    starred=item.id in starred_ids,
                )
                for item in items
            ],
            total,
        )

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
        read_ids = self._read_ids()
        starred_ids = self._starred_ids()
        return to_item_detail(
            item,
            read=item.id in read_ids,
            starred=item.id in starred_ids,
            alphaxiv=alphaxiv,
        )

    def get_item_detail_readonly(self, item_id: str) -> ItemDetailRead | None:
        return self.get_item_detail(item_id)

    def import_url(self, url: str) -> ItemDetailRead:
        try:
            item = self.ingestion.import_url(url)
        except UnsafeOutboundUrlError:
            raise
        return to_item_detail(item, read=item.id in self._read_ids(), starred=item.id in self._starred_ids())

    def import_captured_page(self, payload: CapturedPageImportRequest) -> ItemDetailRead:
        item = self.ingestion.import_captured_page(payload)
        return to_item_detail(item, read=item.id in self._read_ids(), starred=item.id in self._starred_ids())

    def import_url_with_summary(self, url: str) -> ItemDetailRead:
        normalized_url = self.ingestion._normalize_manual_url(url)
        existing_item = self._find_existing_item_by_url(normalized_url)

        if existing_item is None:
            try:
                item = self.ingestion.import_url(url)
            except UnsafeOutboundUrlError:
                raise
            raw_document = self.store.read_raw_document_relative(item.raw_doc_path)
            if raw_document is None:
                raise RuntimeError(f"Raw document missing for imported item '{item.id}'.")
            self._write_summary_asset(
                item=item,
                raw_document=raw_document,
                text=raw_document.body,
                html=self._read_existing_html_asset(raw_document),
            )
            refreshed = self.get_item_detail(item.id)
            if refreshed is None:
                raise RuntimeError(f"Imported item '{item.id}' could not be reloaded.")
            return refreshed

        raw_document = self.store.read_raw_document_relative(existing_item.raw_doc_path)
        if raw_document is None:
            raise RuntimeError(f"Raw document missing for item '{existing_item.id}'.")

        extracted = self._extract_article_for_summary(url, raw_document=raw_document)

        extracted_text = (extracted.cleaned_text or extracted.title or "").strip()
        html = extracted.raw_payload.get("html") if isinstance(extracted.raw_payload, dict) else None
        if not extracted_text:
            extracted_text = self._extract_full_article_text_from_body(raw_document.body) or raw_document.body

        self._write_summary_asset(
            item=existing_item,
            raw_document=raw_document,
            text=extracted_text,
            html=html if isinstance(html, str) else None,
            published_at=extracted.published_at,
        )
        refreshed = self.get_item_detail(existing_item.id)
        if refreshed is None:
            raise RuntimeError(f"Updated item '{existing_item.id}' could not be reloaded.")
        return refreshed

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

    def mark_read(self, item_id: str) -> ActionRead | None:
        item = self.store.get_item(item_id)
        if item is None:
            return None
        triage_status = "archived" if item.status == "archived" else "unread"

        state = self.store.load_read_items()
        if item_id in state.item_ids:
            return ActionRead(item_id=item_id, triage_status=triage_status, detail="Item already marked as read.")

        state.item_ids = [item_id, *[entry_id for entry_id in state.item_ids if entry_id != item_id]]
        self.store.save_read_items(state)
        return ActionRead(item_id=item_id, triage_status=triage_status, detail="Item marked as read.")

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
            self._mark_item_read(item_id)
            detail = "Marked as important."
        self.store.save_starred_items(state)
        triage_status = "archived" if item.status == "archived" else "unread"
        return ActionRead(item_id=item_id, triage_status=triage_status, detail=detail)

    def save_to_zotero(
        self,
        item_id: str,
        *,
        tags: list[str] | None = None,
        note_prefix: str | None = None,
    ) -> ActionRead | None:
        item = self.store.get_item(item_id)
        if item is None:
            return None

        detail = self.get_item_detail(item_id)
        if detail is None:
            return None

        with get_session_factory()() as db:
            connection_service = ConnectionService(db)
            connection = connection_service.get_zotero_connection(refresh_if_needed=True)
            payload = connection_service.get_payload(ConnectionProvider.ZOTERO) if connection else None
            metadata = dict(connection.metadata_json) if connection else {}

        if connection is None or not payload:
            return ActionRead(
                item_id=item_id,
                triage_status="needs_review",
                detail="Needs Review: connect Zotero in Settings first.",
            )

        api_key = str(payload.get("api_key") or "").strip()
        library_id = str(payload.get("library_id") or "").strip()
        library_type = str(payload.get("library_type") or metadata.get("library_type") or "users").strip() or "users"
        if not api_key or not library_id:
            return ActionRead(
                item_id=item_id,
                triage_status="needs_review",
                detail="Needs Review: Zotero is missing the API key or library ID.",
            )

        detail_payload = detail.model_dump(mode="json")
        insight_payload = detail_payload.get("insight")
        if not isinstance(insight_payload, dict):
            insight_payload = {}
        text = str(detail.cleaned_text or "").strip() or str(insight_payload.get("short_summary") or "").strip() or detail.title

        allowed_tags = resolve_zotero_auto_tag_vocabulary(metadata)
        try:
            auto_tags, _usage = LLMClient().suggest_zotero_tags_with_usage(
                detail_payload,
                text,
                allowed_tags,
                insight=insight_payload,
            )
        except Exception:
            auto_tags = []
        merged_tags = merge_zotero_tags(tags or [], auto_tags)
        collection_name = metadata.get("collection_name")
        normalized_collection_name = (
            collection_name.strip() if isinstance(collection_name, str) and collection_name.strip() else None
        )
        normalized_note_prefix = note_prefix.strip() if isinstance(note_prefix, str) and note_prefix.strip() else None

        try:
            export_result = ZoteroClient(
                api_key=api_key,
                library_id=library_id,
                library_type=library_type,
            ).save_item(
                item=detail_payload,
                insight=insight_payload,
                tags=merged_tags,
                note_prefix=normalized_note_prefix,
                collection_name=normalized_collection_name,
            )
        except Exception as exc:
            return ActionRead(
                item_id=item_id,
                triage_status="needs_review",
                detail=f"Needs Review: {exc}",
            )

        if export_result.success:
            self._mark_item_read(item_id)
            return ActionRead(
                item_id=item_id,
                triage_status="saved",
                detail=export_result.detail or "Saved to Zotero.",
            )

        detail_message = str(export_result.detail or "").strip() or "Zotero export could not be completed."
        if not detail_message.lower().startswith("needs review"):
            detail_message = f"Needs Review: {detail_message}"
        return ActionRead(
            item_id=item_id,
            triage_status="needs_review",
            detail=detail_message,
        )

    def _find_existing_item_by_url(self, normalized_url: str) -> VaultItemRecord | None:
        candidates = [
            item
            for item in self.store.load_items_index().items
            if self.ingestion._normalize_manual_url(item.canonical_url) == normalized_url
        ]
        if not candidates:
            return None

        def _sort_key(item: VaultItemRecord) -> tuple[int, int, str]:
            preferred_medium_child = 1 if item.source_id == "medium-email" and item.doc_role == "derived" else 0
            recency = item.published_at or item.fetched_at or item.ingested_at
            return (
                preferred_medium_child,
                1 if item.source_id == "manual-import" else 0,
                recency.isoformat() if recency is not None else "",
            )

        return max(candidates, key=_sort_key)

    @staticmethod
    def _is_sub_document(item: VaultItemRecord) -> bool:
        if item.doc_role == "derived":
            return True
        return any(str(tag).strip().casefold() == SUB_DOCUMENT_TAG for tag in item.tags)

    def _write_summary_asset(
        self,
        *,
        item: VaultItemRecord,
        raw_document: RawDocument,
        text: str,
        html: str | None,
        published_at: Any = None,
    ) -> None:
        normalized_text = text.strip() or raw_document.body.strip() or item.title
        summary = LLMClient().summarize_item(
            self._summary_context(item, raw_document),
            normalized_text,
        )

        source_path = self.store.root / raw_document.path
        folder = source_path.parent
        self.store.write_text(
            folder / SUMMARY_ASSET_FILENAME,
            self._render_summary_asset(
                title=raw_document.frontmatter.title,
                canonical_url=raw_document.frontmatter.canonical_url or item.canonical_url,
                summary=summary,
            ),
        )

        asset_paths = list(raw_document.frontmatter.asset_paths)
        self._append_asset_path(asset_paths, SUMMARY_ASSET_FILENAME)
        if html and html.strip():
            self.store.write_text(folder / "original.html", html)
            self._append_asset_path(asset_paths, "original.html")

        updated_body = self._merge_full_article_text(raw_document=raw_document, full_text=normalized_text)
        updated_frontmatter = raw_document.frontmatter.model_copy(
            update={
                "asset_paths": asset_paths,
                "short_summary": self._normalize_summary_value(summary.get("short_summary")),
                "fetched_at": utcnow(),
                "published_at": raw_document.frontmatter.published_at or published_at,
                "content_hash": content_hash(raw_document.frontmatter.title, updated_body),
            }
        )
        self.store.write_raw_document(
            kind=updated_frontmatter.kind,
            doc_id=updated_frontmatter.id,
            frontmatter=updated_frontmatter,
            body=updated_body,
        )
        self.ingestion.rebuild_items_index(trigger="manual_import_summary")

    def _extract_article_for_summary(
        self,
        url: str,
        *,
        raw_document: RawDocument,
    ) -> ExtractedContent:
        attempted_urls: list[str] = []
        last_error: Exception | None = None
        mirror_url = resolve_external_url(url)
        candidate_urls = [url]
        if mirror_url and mirror_url not in candidate_urls:
            candidate_urls.append(mirror_url)

        for candidate_url in candidate_urls:
            attempted_urls.append(candidate_url)
            try:
                return self.ingestion.extractor.extract_from_url(candidate_url)
            except UnsafeOutboundUrlError:
                raise
            except Exception as exc:
                last_error = exc
                if self._should_retry_insecure_freedium(candidate_url, exc):
                    attempted_urls.append(f"{candidate_url} (insecure-tls)")
                    try:
                        return self.ingestion.extractor.extract_from_url(
                            candidate_url,
                            allow_insecure_tls=True,
                        )
                    except UnsafeOutboundUrlError:
                        raise
                    except Exception as insecure_exc:
                        last_error = insecure_exc

        existing_full_text = self._extract_full_article_text_from_body(raw_document.body)
        if existing_full_text:
            existing_html = self._read_existing_html_asset(raw_document) or ""
            return ExtractedContent(
                title=raw_document.frontmatter.title,
                cleaned_text=existing_full_text,
                outbound_links=[],
                published_at=raw_document.frontmatter.published_at,
                mime_type="text/html" if existing_html else None,
                extraction_confidence=0.0,
                raw_payload={
                    "html": existing_html,
                    "attempted_urls": attempted_urls,
                    "fallback": "stored_full_article",
                },
            )

        reason = str(last_error) if last_error is not None else "unknown fetch failure"
        raise ItemSummaryImportError(
            "Could not fetch the full article text for summarization. "
            f"Tried: {', '.join(attempted_urls)}. Last error: {reason}"
        ) from last_error

    @staticmethod
    def _should_retry_insecure_freedium(url: str, exc: Exception) -> bool:
        if not url.startswith(FREEDIUM_MIRROR_PREFIX):
            return False
        message = str(exc).upper()
        return "CERTIFICATE_VERIFY_FAILED" in message or ("SSL" in message and "CERTIFICATE" in message)

    @staticmethod
    def _summary_context(item: VaultItemRecord, raw_document: RawDocument) -> dict[str, Any]:
        published_at = raw_document.frontmatter.published_at or item.published_at
        return {
            "item_id": item.id,
            "title": raw_document.frontmatter.title or item.title,
            "source_name": raw_document.frontmatter.source_name or item.source_name,
            "authors": list(raw_document.frontmatter.authors or item.authors),
            "published_at": published_at.isoformat() if published_at is not None else None,
            "canonical_url": raw_document.frontmatter.canonical_url or item.canonical_url,
            "content_type": item.content_type.value if hasattr(item.content_type, "value") else str(item.content_type),
        }

    @staticmethod
    def _normalize_summary_value(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        cleaned = value.strip()
        return cleaned or None

    @staticmethod
    def _append_asset_path(asset_paths: list[str], asset_path: str) -> None:
        if asset_path not in asset_paths:
            asset_paths.append(asset_path)

    @staticmethod
    def _extract_full_article_text_from_body(body: str) -> str | None:
        if FULL_ARTICLE_SECTION_HEADING not in body:
            return None
        return body.partition(FULL_ARTICLE_SECTION_HEADING)[2].strip() or None

    def _merge_full_article_text(self, *, raw_document: RawDocument, full_text: str) -> str:
        cleaned_text = full_text.strip()
        if raw_document.frontmatter.source_id == "manual-import":
            return f"{cleaned_text}\n" if cleaned_text else raw_document.body

        existing_body = raw_document.body
        if FULL_ARTICLE_SECTION_HEADING in existing_body:
            existing_body = existing_body.partition(FULL_ARTICLE_SECTION_HEADING)[0].rstrip()

        if not cleaned_text:
            return f"{existing_body.rstrip()}\n" if existing_body.strip() else raw_document.body

        prefix = existing_body.rstrip()
        if not prefix:
            return f"{cleaned_text}\n"
        return f"{prefix}\n\n{FULL_ARTICLE_SECTION_HEADING}\n\n{cleaned_text}\n"

    def _read_existing_html_asset(self, raw_document: RawDocument) -> str | None:
        if "original.html" not in raw_document.frontmatter.asset_paths:
            return None
        original_path = Path(self.store.root / raw_document.path).parent / "original.html"
        if not original_path.exists():
            return None
        return original_path.read_text(encoding="utf-8", errors="ignore")

    @staticmethod
    def _render_summary_asset(
        *,
        title: str,
        canonical_url: str,
        summary: dict[str, Any],
    ) -> str:
        lines = ["# Main points", "", f"Article: {title}", f"URL: {canonical_url}", ""]
        bullet_map = [
            ("Summary", summary.get("short_summary")),
            ("Why it matters", summary.get("why_it_matters")),
            ("What's new", summary.get("whats_new")),
            ("Caveats", summary.get("caveats")),
        ]
        for label, value in bullet_map:
            if not isinstance(value, str):
                continue
            cleaned = value.strip()
            if cleaned:
                lines.append(f"- **{label}:** {cleaned}")

        follow_ups = summary.get("follow_up_questions")
        if isinstance(follow_ups, list):
            questions = [
                question.strip()
                for question in follow_ups
                if isinstance(question, str) and question.strip()
            ]
            if questions:
                lines.extend(["", "## Follow-up questions", ""])
                lines.extend(f"- {question}" for question in questions[:5])

        return "\n".join(lines).strip() + "\n"

    def _starred_ids(self) -> set[str]:
        return set(self.store.load_starred_items().item_ids)

    def _read_ids(self) -> set[str]:
        return set(self.store.load_read_items().item_ids)

    def _mark_item_read(self, item_id: str) -> None:
        state = self.store.load_read_items()
        if item_id in state.item_ids:
            return
        state.item_ids = [item_id, *state.item_ids]
        self.store.save_read_items(state)
