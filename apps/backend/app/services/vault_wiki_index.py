from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from app.services.vault_runtime import slugify, utcnow
from app.vault.frontmatter import parse_frontmatter_document
from app.vault.models import GraphEdge, GraphIndex, GraphNode, PageIndexEntry, PagesIndex
from app.vault.store import VaultStore

WIKILINK_RE = re.compile(r"\[\[([^\]|#]+)(?:#[^\]|]+)?(?:\|[^\]]+)?\]\]")
MARKDOWN_LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+\.md(?:#[^)]+)?)\)")


@dataclass(frozen=True)
class ScannedWikiPage:
    entry: PageIndexEntry
    path: Path
    body: str


class VaultWikiIndexService:
    def __init__(self, *, store: VaultStore | None = None, ensure_layout: bool = True) -> None:
        self.store = store or VaultStore()
        if ensure_layout:
            self.store.ensure_layout()

    def rebuild(self) -> tuple[PagesIndex, GraphIndex]:
        pages, graph = self.scan()
        self.store.save_pages_index(pages)
        self.store.save_graph_index(graph)
        return pages, graph

    def scan(self) -> tuple[PagesIndex, GraphIndex]:
        scanned = self._scan_pages()
        backlinks: dict[str, set[str]] = defaultdict(set)
        edge_set: set[tuple[str, str, str]] = set()

        title_lookup: dict[str, str] = {}
        slug_lookup: dict[str, str] = {}
        relative_lookup: dict[str, str] = {}
        for page in scanned:
            entry = page.entry
            title_lookup.setdefault(entry.title.casefold(), entry.id)
            slug_lookup.setdefault(entry.slug.casefold(), entry.id)
            slug_lookup.setdefault(f"{entry.namespace}/{entry.slug}".casefold(), entry.id)
            relative_lookup.setdefault(entry.path.casefold(), entry.id)
            for alias in entry.aliases:
                title_lookup.setdefault(alias.casefold(), entry.id)

        for page in scanned:
            resolved_targets = self._resolve_links(
                page,
                title_lookup=title_lookup,
                slug_lookup=slug_lookup,
                relative_lookup=relative_lookup,
            )
            for target_id in resolved_targets:
                if target_id == page.entry.id:
                    continue
                edge = (page.entry.id, target_id, "wiki_link")
                if edge in edge_set:
                    continue
                edge_set.add(edge)
                backlinks[target_id].add(page.entry.id)

        pages: list[PageIndexEntry] = []
        for page in scanned:
            entry = page.entry.model_copy(update={"backlinks": sorted(backlinks.get(page.entry.id, set()))})
            pages.append(entry)

        graph = GraphIndex(
            generated_at=utcnow(),
            nodes=[
                GraphNode(
                    id=entry.id,
                    label=entry.title,
                    node_type=entry.page_type,
                    path=entry.path,
                )
                for entry in pages
            ],
            edges=[GraphEdge(source=source, target=target, edge_type=edge_type) for source, target, edge_type in sorted(edge_set)],
        )
        return PagesIndex(generated_at=utcnow(), pages=pages), graph

    def list_pages(self) -> list[PageIndexEntry]:
        pages, _graph = self.scan()
        return pages.pages

    def _scan_pages(self) -> list[ScannedWikiPage]:
        scanned: list[ScannedWikiPage] = []
        for path in sorted(self.store.wiki_dir.rglob("*.md")):
            relative_path = path.relative_to(self.store.root)
            namespace = path.relative_to(self.store.wiki_dir).parent.as_posix() or "misc"
            raw_frontmatter, body = parse_frontmatter_document(path.read_text(encoding="utf-8"))
            updated_at = self._coerce_datetime(raw_frontmatter.get("updated_at"))
            if updated_at is None:
                updated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            title = self._coerce_string(raw_frontmatter.get("title")) or path.stem.replace("-", " ").title()
            slug = slugify(self._coerce_string(raw_frontmatter.get("slug")) or path.stem, fallback=path.stem)
            entry = PageIndexEntry(
                id=self._coerce_string(raw_frontmatter.get("id")) or f"wiki:{namespace}:{slug}",
                page_type=self._coerce_string(raw_frontmatter.get("page_type")) or namespace or "note",
                title=title,
                namespace=namespace,
                slug=slug,
                path=relative_path.as_posix(),
                aliases=self._coerce_str_list(raw_frontmatter.get("aliases")),
                source_refs=self._coerce_str_list(raw_frontmatter.get("source_refs")),
                backlinks=[],
                updated_at=updated_at,
                managed=self._coerce_bool(raw_frontmatter.get("managed"), default=True),
            )
            scanned.append(ScannedWikiPage(entry=entry, path=path, body=body))
        return scanned

    def _resolve_links(
        self,
        page: ScannedWikiPage,
        *,
        title_lookup: dict[str, str],
        slug_lookup: dict[str, str],
        relative_lookup: dict[str, str],
    ) -> set[str]:
        resolved: set[str] = set()
        for target in WIKILINK_RE.findall(page.body):
            normalized = target.strip().casefold()
            if not normalized:
                continue
            page_id = title_lookup.get(normalized) or slug_lookup.get(normalized)
            if page_id:
                resolved.add(page_id)
        for target in MARKDOWN_LINK_RE.findall(page.body):
            path_part = target.split("#", 1)[0].strip()
            if not path_part:
                continue
            resolved_path = (page.path.parent / path_part).resolve()
            try:
                relative = resolved_path.relative_to(self.store.root).as_posix().casefold()
            except ValueError:
                continue
            page_id = relative_lookup.get(relative)
            if page_id:
                resolved.add(page_id)
        return resolved

    @staticmethod
    def _coerce_string(value: object) -> str | None:
        return str(value).strip() if isinstance(value, str) and value.strip() else None

    @classmethod
    def _coerce_str_list(cls, value: object) -> list[str]:
        if not isinstance(value, list):
            return []
        normalized: list[str] = []
        for item in value:
            coerced = cls._coerce_string(item)
            if coerced:
                normalized.append(coerced)
        return normalized

    @staticmethod
    def _coerce_bool(value: object, *, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "yes", "1"}:
                return True
            if normalized in {"false", "no", "0"}:
                return False
        return default

    @staticmethod
    def _coerce_datetime(value: object) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
