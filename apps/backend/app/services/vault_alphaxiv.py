from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import orjson

from app.schemas.common import AlphaXivPaperRead, AlphaXivSimilarPaperRead
from app.services.text import ARXIV_ID_RE, normalize_whitespace
from app.vault.models import RawDocument, VaultItemRecord
from app.vault.store import VaultStore

ALPHAXIV_SOURCE_ID = "alphaxiv-paper"
ALPHAXIV_WEB_ABS_PREFIX = "https://www.alphaxiv.org/abs/"
MAX_SIMILAR_PAPERS = 12
CANONICAL_VERSION_RE = re.compile(r"v\d+$", re.IGNORECASE)
PAPER_ID_IN_URL_RE = re.compile(r"/(?:abs|pdf)/(?P<paper_id>\d{4}\.\d{4,5})(?:v\d+)?", re.IGNORECASE)


class AlphaXivPaperResolver:
    def __init__(
        self,
        *,
        store: VaultStore,
        items: Sequence[VaultItemRecord] | None = None,
    ) -> None:
        self.store = store
        self._items_by_url: dict[str, VaultItemRecord] = {}
        self._items_by_title: dict[str, VaultItemRecord] = {}

        for item in items or []:
            if item.content_type.value != "paper" and item.kind != "paper":
                continue
            normalized_url = self._normalize_url(item.canonical_url)
            if normalized_url and normalized_url not in self._items_by_url:
                self._items_by_url[normalized_url] = item
            paper_id = self._paper_id_from_url(item.canonical_url or "")
            if paper_id:
                alphaxiv_url = f"{ALPHAXIV_WEB_ABS_PREFIX}{paper_id}"
                if alphaxiv_url not in self._items_by_url:
                    self._items_by_url[alphaxiv_url] = item
            title_key = self._normalized_title(item.title)
            if title_key and title_key not in self._items_by_title:
                self._items_by_title[title_key] = item

    def resolve(
        self,
        item: VaultItemRecord,
        *,
        raw_document: RawDocument | None = None,
    ) -> AlphaXivPaperRead | None:
        if item.source_id != ALPHAXIV_SOURCE_ID and "alphaxiv.org/abs/" not in (item.canonical_url or ""):
            return None

        source_dir = self._source_dir(item, raw_document=raw_document)
        if source_dir is None:
            return None

        preview = self._load_json(source_dir / "alphaxiv-preview.json")
        overview = self._load_json(source_dir / "alphaxiv-overview.json")
        metadata = self._load_json(source_dir / "alphaxiv-metadata.json")
        similar_payload = self._load_json(source_dir / "alphaxiv-similar-papers.json")

        short_summary = self._first_non_empty(
            [
                raw_document.frontmatter.short_summary if raw_document is not None else None,
                self._nested_string(preview, "paper_summary", "summary"),
                self._nested_string(overview, "summary", "summary"),
                self._string_value(metadata.get("short_summary") if isinstance(metadata, dict) else None),
                item.short_summary,
            ]
        )
        filed_text = self._render_filed_text(overview=overview, preview=preview, metadata=metadata)
        audio_url = self._first_non_empty(
            [
                self._string_value(metadata.get("podcast_url") if isinstance(metadata, dict) else None),
            ]
        )
        similar_papers = self._resolve_similar_papers(
            item,
            payload=similar_payload,
        )

        if not short_summary and not filed_text and not audio_url and not similar_papers:
            return None

        return AlphaXivPaperRead(
            short_summary=short_summary,
            filed_text=filed_text,
            audio_url=audio_url,
            similar_papers=similar_papers,
        )

    def _resolve_similar_papers(
        self,
        current_item: VaultItemRecord,
        *,
        payload: Any,
    ) -> list[AlphaXivSimilarPaperRead]:
        if not isinstance(payload, list):
            return []

        papers: list[AlphaXivSimilarPaperRead] = []
        seen: set[str] = set()
        current_url = self._normalize_url(current_item.canonical_url)

        for entry in payload:
            if not isinstance(entry, dict):
                continue
            title = self._string_value(entry.get("title"))
            if not title:
                continue
            canonical_url = self._similar_paper_url(entry)
            matched_item = self._match_item(entry, canonical_url=canonical_url, title=title)
            if matched_item is not None and matched_item.id == current_item.id:
                continue
            if canonical_url and self._normalize_url(canonical_url) == current_url:
                continue

            effective_url = canonical_url or (matched_item.canonical_url if matched_item is not None else None)
            if not effective_url:
                continue

            dedupe_key = self._normalize_url(effective_url) or self._normalized_title(title)
            if not dedupe_key or dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            papers.append(
                AlphaXivSimilarPaperRead(
                    title=title,
                    canonical_url=effective_url,
                    app_item_id=matched_item.id if matched_item is not None else None,
                    authors=self._string_list(entry.get("authors")),
                    short_summary=self._first_non_empty(
                        [
                            self._nested_string(entry, "paper_summary", "summary"),
                            self._string_value(entry.get("abstract")),
                        ]
                    ),
                )
            )
            if len(papers) >= MAX_SIMILAR_PAPERS:
                break

        return papers

    def _match_item(
        self,
        payload: dict[str, Any],
        *,
        canonical_url: str | None,
        title: str,
    ) -> VaultItemRecord | None:
        if canonical_url:
            direct = self._items_by_url.get(self._normalize_url(canonical_url))
            if direct is not None:
                return direct

        paper_id = self._paper_id_from_payload(payload)
        if paper_id:
            alternate = self._items_by_url.get(f"{ALPHAXIV_WEB_ABS_PREFIX}{paper_id}")
            if alternate is not None:
                return alternate

        return self._items_by_title.get(self._normalized_title(title))

    def _render_filed_text(
        self,
        *,
        overview: Any,
        preview: Any,
        metadata: Any,
    ) -> str | None:
        titles = overview.get("summarySectionTitles") if isinstance(overview, dict) else {}
        summary_payload = (
            overview.get("summary")
            if isinstance(overview, dict) and isinstance(overview.get("summary"), dict)
            else preview.get("paper_summary")
            if isinstance(preview, dict) and isinstance(preview.get("paper_summary"), dict)
            else {}
        )
        abstract = self._first_non_empty(
            [
                self._string_value(overview.get("abstract") if isinstance(overview, dict) else None),
                self._string_value(preview.get("abstract") if isinstance(preview, dict) else None),
                self._string_value(metadata.get("abstract") if isinstance(metadata, dict) else None),
            ]
        )

        sections = [
            (self._section_title(titles, "abstract", "Abstract"), self._markdown_block(abstract)),
            (
                self._section_title(titles, "problem", "Problem"),
                self._markdown_block(summary_payload.get("originalProblem") if isinstance(summary_payload, dict) else None),
            ),
            (
                self._section_title(titles, "method", "Method"),
                self._markdown_block(summary_payload.get("solution") if isinstance(summary_payload, dict) else None),
            ),
            (
                self._section_title(titles, "results", "Results"),
                self._markdown_block(summary_payload.get("results") if isinstance(summary_payload, dict) else None),
            ),
            (
                self._section_title(titles, "summary", "Summary"),
                self._markdown_block(summary_payload.get("summary") if isinstance(summary_payload, dict) else None),
            ),
            (
                self._section_title(titles, "takeaways", "Takeaways"),
                self._markdown_block(summary_payload.get("keyInsights") if isinstance(summary_payload, dict) else None),
            ),
        ]

        lines: list[str] = []
        for title, body in sections:
            if not body:
                continue
            if lines:
                lines.append("")
            lines.extend([f"## {title}", "", body])

        rendered = "\n".join(lines).strip()
        return rendered or None

    def _markdown_block(self, value: Any) -> str | None:
        if isinstance(value, list):
            items = self._string_list(value)
            if not items:
                return None
            if len(items) == 1:
                return items[0]
            return "\n".join(f"- {entry}" for entry in items)

        text = self._string_value(value)
        return text or None

    def _source_dir(self, item: VaultItemRecord, *, raw_document: RawDocument | None) -> Path | None:
        if raw_document is not None:
            return (self.store.root / raw_document.path).parent
        if not item.raw_doc_path:
            return None
        return (self.store.root / item.raw_doc_path).parent

    def _load_json(self, path: Path) -> Any | None:
        if not path.exists():
            return None
        try:
            return orjson.loads(path.read_bytes())
        except orjson.JSONDecodeError:
            return None

    def _paper_id_from_payload(self, payload: dict[str, Any]) -> str | None:
        candidates = [
            payload.get("universal_paper_id"),
            payload.get("canonical_id"),
        ]
        for candidate in candidates:
            paper_id = self._normalize_paper_id(candidate)
            if paper_id:
                return paper_id
        return None

    def _similar_paper_url(self, payload: dict[str, Any]) -> str | None:
        explicit = self._string_value(payload.get("alphaxiv_url") or payload.get("alphaxivLink"))
        if explicit:
            return explicit
        paper_id = self._paper_id_from_payload(payload)
        if not paper_id:
            return None
        return f"{ALPHAXIV_WEB_ABS_PREFIX}{paper_id}"

    @staticmethod
    def _paper_id_from_url(url: str) -> str | None:
        match = PAPER_ID_IN_URL_RE.search(url)
        if not match:
            return None
        return match.group("paper_id")

    def _normalize_paper_id(self, value: Any) -> str | None:
        text = self._string_value(value)
        if not text:
            return None
        normalized = CANONICAL_VERSION_RE.sub("", text)
        if not ARXIV_ID_RE.match(normalized):
            return None
        return normalized.lower().removeprefix("arxiv:")

    @staticmethod
    def _normalize_url(url: str | None) -> str | None:
        text = normalize_whitespace(url)
        if not text:
            return None
        return text.rstrip("/")

    @staticmethod
    def _normalized_title(title: str | None) -> str:
        return normalize_whitespace(title).casefold()

    @staticmethod
    def _string_value(value: Any) -> str | None:
        text = normalize_whitespace(value)
        return text or None

    def _nested_string(self, payload: Any, *path: str) -> str | None:
        current = payload
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return self._string_value(current)

    def _string_list(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        normalized: list[str] = []
        for entry in value:
            text = self._string_value(entry)
            if text:
                normalized.append(text)
        return normalized

    def _section_title(self, titles: Any, key: str, fallback: str) -> str:
        if not isinstance(titles, dict):
            return fallback
        return self._string_value(titles.get(key)) or fallback

    @staticmethod
    def _first_non_empty(values: Sequence[str | None]) -> str | None:
        for value in values:
            cleaned = normalize_whitespace(value)
            if cleaned:
                return cleaned
        return None
