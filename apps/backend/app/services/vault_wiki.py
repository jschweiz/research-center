from __future__ import annotations

import posixpath
from collections import defaultdict
from pathlib import Path

from app.core.external_urls import resolve_external_url
from app.db.models import IngestionRunType, RunStatus
from app.schemas.ops import OperationBasicInfoRead
from app.services.vault_insights import VaultInsightsService
from app.services.vault_runtime import RunRecorder, short_hash, slugify, utcnow
from app.services.vault_wiki_index import VaultWikiIndexService
from app.vault.models import (
    PagesIndex,
    TopicIndexEntry,
    VaultItemRecord,
    WikiPage,
    WikiPageFrontmatter,
)
from app.vault.store import VaultStore

MANAGED_WIKI_NAMESPACES = {"maps", "sources", "topics", "trends"}
MAP_SECTION_TOPICS = {
    "Frontier Systems": {
        "Agents",
        "Code Generation",
        "Knowledge Graphs",
        "Memory",
        "Reasoning",
        "Search",
        "Test-Time Compute",
        "Tool Use",
    },
    "Learning & Model Design": {
        "Data Curation",
        "Distillation",
        "Efficiency",
        "Fine-Tuning",
        "Inference",
        "Long Context",
        "Open-Weight Models",
        "Pretraining",
        "Reinforcement Learning",
        "Small Models",
        "Synthetic Data",
    },
    "Modalities & Embodiment": {
        "Audio",
        "Computer Vision",
        "Image Generation",
        "Multimodal",
        "Robotics",
        "Video Generation",
        "World Models",
    },
    "Reliability & Governance": {
        "Alignment",
        "Evaluations",
        "Interpretability",
        "Safety",
    },
}


class VaultWikiService:
    def __init__(self) -> None:
        self.store = VaultStore()
        self.runs = RunRecorder(self.store)
        self.insights = VaultInsightsService(store=self.store)
        self.wiki_index = VaultWikiIndexService(store=self.store)
        self.store.ensure_layout()

    def compile(self, *, trigger: str = "manual_compile") -> PagesIndex:
        run = self.runs.start(
            run_type=IngestionRunType.DIGEST,
            operation_kind="wiki_compile",
            trigger=trigger,
            title="Wiki compile",
            summary="Rebuilding managed source, topic, trend, and map pages.",
        )
        lease = None
        try:
            lease = self.store.acquire_lease(name="compile-wiki", owner="mac", ttl_seconds=600)
            indexed_items, insights = self.insights.ensure_index(persist=True)
            items = [item for item in indexed_items if item.index_visibility != "hidden"]
            item_lookup = {item.id: item for item in items}
            source_page_paths = {item.id: self._source_page_relative_path(item) for item in items}
            topic_page_paths = {
                topic.id: topic.page_path or f"wiki/topics/{topic.slug}.md"
                for topic in insights.topics
            }
            active_paths: set[str] = set()

            for item in items:
                active_paths.add(
                    self._write_source_page(
                        item=item,
                        items=items,
                        source_page_paths=source_page_paths,
                        topic_page_paths=topic_page_paths,
                        trends_page_path=insights.trends_page_path,
                        map_page_path=insights.map_page_path,
                    )
                )

            for topic in insights.topics:
                active_paths.add(
                    self._write_topic_page(
                        topic=topic,
                        item_lookup=item_lookup,
                        source_page_paths=source_page_paths,
                        topic_page_paths=topic_page_paths,
                        trends_page_path=insights.trends_page_path,
                        map_page_path=insights.map_page_path,
                    )
                )

            active_paths.add(self._write_trends_page(insights=insights, topic_page_paths=topic_page_paths))
            active_paths.add(
                self._write_map_page(
                    insights=insights,
                    topic_page_paths=topic_page_paths,
                )
            )

            self._prune_managed_pages(active_paths)

            pages, _graph = self.wiki_index.scan()
            self._persist_backlinks(pages, active_paths=active_paths)
            pages, graph = self.wiki_index.rebuild()

            run.basic_info.extend(
                [
                    OperationBasicInfoRead(label="Items", value=str(len(items))),
                    OperationBasicInfoRead(label="Topics", value=str(len(insights.topics))),
                    OperationBasicInfoRead(label="Rising topics", value=str(len(insights.rising_topic_ids))),
                    OperationBasicInfoRead(label="Pages", value=str(len(pages.pages))),
                    OperationBasicInfoRead(label="Edges", value=str(len(graph.edges))),
                    OperationBasicInfoRead(label="Wiki dir", value=str(self.store.wiki_dir)),
                ]
            )
            self.runs.finish(
                run,
                status=RunStatus.SUCCEEDED,
                summary=f"Compiled {len(pages.pages)} managed wiki pages into a research graph.",
            )
            return pages
        except Exception as exc:
            run.errors.append(str(exc))
            self.runs.finish(run, status=RunStatus.FAILED, summary="Wiki compile failed.")
            raise
        finally:
            if lease is not None:
                self.store.release_lease(lease)

    def _write_source_page(
        self,
        *,
        item: VaultItemRecord,
        items: list[VaultItemRecord],
        source_page_paths: dict[str, str],
        topic_page_paths: dict[str, str],
        trends_page_path: str | None,
        map_page_path: str | None,
    ) -> str:
        slug = self._source_page_slug(item)
        current_path = self._source_page_relative_path(item)
        body = self._render_source_page_body(
            item=item,
            items=items,
            source_page_paths=source_page_paths,
            topic_page_paths=topic_page_paths,
            current_path=current_path,
            trends_page_path=trends_page_path,
            map_page_path=map_page_path,
        )
        return self._write_managed_page(
            namespace="sources",
            slug=slug,
            frontmatter=WikiPageFrontmatter(
                id=f"page:{item.id}",
                page_type="source-note",
                title=item.title,
                aliases=[item.title],
                source_refs=[item.id],
                backlinks=[],
                updated_at=utcnow(),
                managed=True,
            ),
            body=body,
        )

    def _write_topic_page(
        self,
        *,
        topic: TopicIndexEntry,
        item_lookup: dict[str, VaultItemRecord],
        source_page_paths: dict[str, str],
        topic_page_paths: dict[str, str],
        trends_page_path: str | None,
        map_page_path: str | None,
    ) -> str:
        aliases = self._unique_strings([topic.label, *topic.aliases])
        current_path = topic.page_path or f"wiki/topics/{topic.slug}.md"
        body = self._render_topic_page_body(
            topic=topic,
            item_lookup=item_lookup,
            source_page_paths=source_page_paths,
            topic_page_paths=topic_page_paths,
            current_path=current_path,
            trends_page_path=trends_page_path,
            map_page_path=map_page_path,
        )
        return self._write_managed_page(
            namespace="topics",
            slug=topic.slug,
            frontmatter=WikiPageFrontmatter(
                id=f"topic:{topic.id}",
                page_type="topic",
                title=topic.label,
                aliases=aliases,
                source_refs=topic.item_ids[:100],
                backlinks=[],
                updated_at=utcnow(),
                managed=True,
            ),
            body=body,
        )

    def _write_trends_page(self, *, insights, topic_page_paths: dict[str, str]) -> str:
        current_path = insights.trends_page_path or "wiki/trends/rising-topics.md"
        body = self._render_trends_page_body(
            insights=insights,
            topic_page_paths=topic_page_paths,
            current_path=current_path,
            map_page_path=insights.map_page_path,
        )
        rising_refs: list[str] = []
        for topic_id in insights.rising_topic_ids[:12]:
            topic = next((entry for entry in insights.topics if entry.id == topic_id), None)
            if topic is not None:
                rising_refs.extend(topic.representative_item_ids[:3])
        return self._write_managed_page(
            namespace="trends",
            slug="rising-topics",
            frontmatter=WikiPageFrontmatter(
                id="map:trends:rising-topics",
                page_type="trend-report",
                title="Rising Topics",
                aliases=["Trend Radar", "Topic Radar"],
                source_refs=self._unique_strings(rising_refs)[:120],
                backlinks=[],
                updated_at=utcnow(),
                managed=True,
            ),
            body=body,
        )

    def _write_map_page(self, *, insights, topic_page_paths: dict[str, str]) -> str:
        current_path = insights.map_page_path or "wiki/maps/global-ai-research.md"
        body = self._render_map_page_body(
            insights=insights,
            topic_page_paths=topic_page_paths,
            current_path=current_path,
            trends_page_path=insights.trends_page_path,
        )
        map_refs: list[str] = []
        for topic in insights.topics[:18]:
            map_refs.extend(topic.representative_item_ids[:2])
        return self._write_managed_page(
            namespace="maps",
            slug="global-ai-research",
            frontmatter=WikiPageFrontmatter(
                id="map:global-ai-research",
                page_type="map",
                title="Global AI Research",
                aliases=["AI Research Landscape", "Research Mind Map"],
                source_refs=self._unique_strings(map_refs)[:120],
                backlinks=[],
                updated_at=utcnow(),
                managed=True,
            ),
            body=body,
        )

    def _write_managed_page(
        self,
        *,
        namespace: str,
        slug: str,
        frontmatter: WikiPageFrontmatter,
        body: str,
    ) -> str:
        frontmatter = frontmatter.model_copy(
            update={
                "aliases": self._unique_strings(frontmatter.aliases),
                "source_refs": self._unique_strings(frontmatter.source_refs),
            }
        )
        existing = self._read_existing_page(namespace=namespace, slug=slug)
        if existing is not None and self._page_matches(existing, frontmatter=frontmatter, body=body):
            return existing.path

        if existing is not None:
            frontmatter = frontmatter.model_copy(
                update={
                    "updated_at": utcnow(),
                    "backlinks": existing.frontmatter.backlinks,
                }
            )

        path = self.store.write_wiki_page(
            namespace=namespace,
            slug=slug,
            frontmatter=frontmatter,
            body=body,
        )
        return str(path.relative_to(self.store.root))

    def _persist_backlinks(self, pages: PagesIndex, *, active_paths: set[str]) -> None:
        page_lookup = {page.path: page for page in pages.pages}
        for relative_path in active_paths:
            entry = page_lookup.get(relative_path)
            if entry is None:
                continue
            path = self.store.root / relative_path
            if not path.exists():
                continue
            existing = self.store.read_wiki_page(path)
            backlinks = sorted(entry.backlinks)
            if existing.frontmatter.backlinks == backlinks:
                continue
            existing.frontmatter.backlinks = backlinks
            existing.frontmatter.updated_at = utcnow()
            self.store.write_wiki_page(
                namespace=entry.namespace,
                slug=entry.slug,
                frontmatter=existing.frontmatter,
                body=existing.body,
            )

    def _prune_managed_pages(self, active_paths: set[str]) -> None:
        for namespace in MANAGED_WIKI_NAMESPACES:
            base = self.store.wiki_dir / namespace
            if not base.exists():
                continue
            for page_path in base.rglob("*.md"):
                relative = str(page_path.relative_to(self.store.root))
                if relative in active_paths:
                    continue
                if not self._is_managed_page(page_path):
                    continue
                page_path.unlink(missing_ok=True)

    def _render_source_page_body(
        self,
        *,
        item: VaultItemRecord,
        items: list[VaultItemRecord],
        source_page_paths: dict[str, str],
        topic_page_paths: dict[str, str],
        current_path: str,
        trends_page_path: str | None,
        map_page_path: str | None,
    ) -> str:
        topic_links = [
            self._page_markdown_link(
                label=ref.label,
                current_path=current_path,
                target_path=topic_page_paths.get(ref.topic_id) or f"wiki/topics/{slugify(ref.label, fallback=ref.topic_id)}.md",
            )
            for ref in item.topic_refs
        ]
        related_items = self._related_items(item=item, items=items, limit=6)
        lines = [
            f"# {item.title}",
            "",
            "System-generated source note. Build higher-order synthesis pages around it instead of editing the raw document.",
            "",
            "## Metadata",
            "",
            f"- Source: {item.source_name}",
            f"- Canonical URL: {resolve_external_url(item.canonical_url)}",
            f"- Document kind: {item.kind}",
        ]
        if item.published_at is not None:
            lines.append(f"- Published at: {item.published_at.isoformat()}")
        if item.authors:
            lines.append(f"- Authors: {', '.join(item.authors)}")
        if item.tags:
            lines.append(f"- Tags: {', '.join(item.tags[:12])}")
        if topic_links:
            lines.append(f"- Topics: {', '.join(topic_links)}")
            lines.append(f"- Trend score: {item.trend_score:.2f}")
            lines.append(f"- Novelty score: {item.novelty_score:.2f}")

        if item.short_summary:
            lines.extend(["", "## Summary", "", item.short_summary])

        if topic_links:
            lines.extend(["", "## Topic Map", ""])
            lines.extend(f"- {link}" for link in topic_links)

        if related_items:
            lines.extend(["", "## Related Research", ""])
            for related, shared_topics in related_items:
                related_path = self._relative_wiki_link(
                    current_path=source_page_paths[item.id],
                    target_path=source_page_paths[related.id],
                )
                descriptor = f"shared topics: {', '.join(shared_topics)}" if shared_topics else "connected signal"
                lines.append(f"- [{related.title}]({related_path}) ({descriptor})")

        lines.extend(
            [
                "",
                "## Radar",
                "",
                f"- {self._page_markdown_link(label='Rising Topics', current_path=current_path, target_path=trends_page_path or 'wiki/trends/rising-topics.md')}",
                f"- {self._page_markdown_link(label='Global AI Research', current_path=current_path, target_path=map_page_path or 'wiki/maps/global-ai-research.md')}",
            ]
        )

        if item.cleaned_text:
            lines.extend(["", "## Source Excerpt", "", item.cleaned_text[:4000]])

        return "\n".join(lines).strip() + "\n"

    def _render_topic_page_body(
        self,
        *,
        topic: TopicIndexEntry,
        item_lookup: dict[str, VaultItemRecord],
        source_page_paths: dict[str, str],
        topic_page_paths: dict[str, str],
        current_path: str,
        trends_page_path: str | None,
        map_page_path: str | None,
    ) -> str:
        lines = [
            f"# {topic.label}",
            "",
            "System-generated topic page that anchors Codex and the wiki around a stable research concept.",
            "",
            "## Signal Summary",
            "",
            f"- Trend score: {topic.trend_score:.2f}",
            f"- Novelty score: {topic.novelty_score:.2f}",
            f"- Items in last 7 days: {topic.recent_item_count_7d}",
            f"- Items in last 30 days: {topic.recent_item_count_30d}",
            f"- Total supporting items: {topic.total_item_count}",
            f"- Source diversity: {topic.source_diversity}",
        ]
        if topic.first_seen_at is not None:
            lines.append(f"- First seen: {topic.first_seen_at.isoformat()}")
        if topic.last_seen_at is not None:
            lines.append(f"- Last seen: {topic.last_seen_at.isoformat()}")

        if topic.aliases:
            lines.extend(["", "## Aliases", ""])
            lines.extend(f"- {alias}" for alias in topic.aliases[:10] if alias.casefold() != topic.label.casefold())

        if topic.related_topic_ids:
            lines.extend(["", "## Related Topics", ""])
            lines.extend(
                "- "
                + self._page_markdown_link(
                    label=self._topic_title(topic_id),
                    current_path=current_path,
                    target_path=topic_page_paths.get(topic_id),
                )
                for topic_id in topic.related_topic_ids[:10]
            )

        if topic.source_names:
            lines.extend(["", "## Leading Sources", ""])
            lines.extend(f"- {source_name}" for source_name in topic.source_names[:10])

        representative_items = [
            item_lookup[item_id]
            for item_id in topic.representative_item_ids
            if item_id in item_lookup and item_id in source_page_paths
        ]
        if representative_items:
            lines.extend(["", "## Representative Signals", ""])
            for item in representative_items:
                relative_link = self._relative_wiki_link(
                    current_path=current_path,
                    target_path=source_page_paths[item.id],
                )
                lines.append(f"- [{item.title}]({relative_link})")

        lines.extend(
            [
                "",
                "## Map Links",
                "",
                f"- {self._page_markdown_link(label='Rising Topics', current_path=current_path, target_path=trends_page_path or 'wiki/trends/rising-topics.md')}",
                f"- {self._page_markdown_link(label='Global AI Research', current_path=current_path, target_path=map_page_path or 'wiki/maps/global-ai-research.md')}",
            ]
        )
        return "\n".join(lines).strip() + "\n"

    def _render_trends_page_body(
        self,
        *,
        insights,
        topic_page_paths: dict[str, str],
        current_path: str,
        map_page_path: str | None,
    ) -> str:
        topic_lookup = {topic.id: topic for topic in insights.topics}
        lines = [
            "# Rising Topics",
            "",
            "System-generated radar for the fastest-moving topics in the vault. Use it to decide what deserves synthesis, monitoring, and deeper Codex work.",
        ]
        if insights.rising_topic_ids:
            lines.extend(["", "## Fast Movers", ""])
            for topic_id in insights.rising_topic_ids[:12]:
                topic = topic_lookup.get(topic_id)
                if topic is None:
                    continue
                lines.append(
                    "- "
                    + self._page_markdown_link(
                        label=topic.label,
                        current_path=current_path,
                        target_path=topic_page_paths.get(topic.id),
                    )
                    + " "
                    + f"(trend {topic.trend_score:.2f}; 7d {topic.recent_item_count_7d}; "
                    + f"30d {topic.recent_item_count_30d}; sources {topic.source_diversity})"
                )

            emerging = [
                topic_lookup[topic_id]
                for topic_id in insights.rising_topic_ids
                if topic_id in topic_lookup and topic_lookup[topic_id].novelty_score > 0
            ]
            if emerging:
                lines.extend(["", "## Emerging Topics", ""])
                for topic in emerging[:8]:
                    lines.append(
                        "- "
                        + self._page_markdown_link(
                            label=topic.label,
                            current_path=current_path,
                            target_path=topic_page_paths.get(topic.id),
                        )
                        + f" (novelty {topic.novelty_score:.2f}; first seen {topic.first_seen_at.isoformat() if topic.first_seen_at else 'unknown'})"
                    )

        if insights.connections:
            lines.extend(["", "## Expanding Connections", ""])
            for connection in insights.connections[:12]:
                lines.append(
                    "- "
                    + self._page_markdown_link(
                        label=self._topic_title(connection.source_topic_id),
                        current_path=current_path,
                        target_path=topic_page_paths.get(connection.source_topic_id),
                    )
                    + " <-> "
                    + self._page_markdown_link(
                        label=self._topic_title(connection.target_topic_id),
                        current_path=current_path,
                        target_path=topic_page_paths.get(connection.target_topic_id),
                    )
                    + " "
                    + f"(co-occurrence {connection.weight})"
                )

        lines.extend(
            [
                "",
                "## Landscape Entry Point",
                "",
                f"- {self._page_markdown_link(label='Global AI Research', current_path=current_path, target_path=map_page_path or 'wiki/maps/global-ai-research.md')}",
            ]
        )
        return "\n".join(lines).strip() + "\n"

    def _render_map_page_body(
        self,
        *,
        insights,
        topic_page_paths: dict[str, str],
        current_path: str,
        trends_page_path: str | None,
    ) -> str:
        topics_by_label = {topic.label: topic for topic in insights.topics}
        section_topics: dict[str, list[str]] = defaultdict(list)
        assigned: set[str] = set()
        for section, labels in MAP_SECTION_TOPICS.items():
            for label in labels:
                if label in topics_by_label:
                    section_topics[section].append(label)
                    assigned.add(label)

        emerging = [
            topic.label
            for topic in insights.topics[:18]
            if topic.label not in assigned
        ]

        lines = [
            "# Global AI Research",
            "",
            "System-generated landscape map for the vault. Treat this as the canonical starting point for exploring the research graph, rising topics, and source-note coverage.",
        ]
        for section, labels in section_topics.items():
            if not labels:
                continue
            lines.extend(["", f"## {section}", ""])
            for label in labels:
                topic = topics_by_label.get(label)
                lines.append(
                    "- "
                    + self._page_markdown_link(
                        label=label,
                        current_path=current_path,
                        target_path=topic_page_paths.get(topic.id) if topic is not None else None,
                    )
                )

        if emerging:
            lines.extend(["", "## Emerging Topics", ""])
            for label in emerging[:12]:
                topic = topics_by_label.get(label)
                lines.append(
                    "- "
                    + self._page_markdown_link(
                        label=label,
                        current_path=current_path,
                        target_path=topic_page_paths.get(topic.id) if topic is not None else None,
                    )
                )

        if insights.rising_topic_ids:
            lines.extend(["", "## Trend Radar", ""])
            lines.append(
                "- "
                + self._page_markdown_link(
                    label="Rising Topics",
                    current_path=current_path,
                    target_path=trends_page_path or "wiki/trends/rising-topics.md",
                )
            )
            for topic_id in insights.rising_topic_ids[:8]:
                lines.append(
                    "- "
                    + self._page_markdown_link(
                        label=self._topic_title(topic_id),
                        current_path=current_path,
                        target_path=topic_page_paths.get(topic_id),
                    )
                )

        return "\n".join(lines).strip() + "\n"

    def _related_items(
        self,
        *,
        item: VaultItemRecord,
        items: list[VaultItemRecord],
        limit: int,
    ) -> list[tuple[VaultItemRecord, list[str]]]:
        item_topic_lookup = {ref.topic_id: ref.label for ref in item.topic_refs}
        ranked: list[tuple[int, int, VaultItemRecord, list[str]]] = []
        for candidate in items:
            if candidate.id == item.id:
                continue
            candidate_topic_lookup = {ref.topic_id: ref.label for ref in candidate.topic_refs}
            shared_topic_ids = sorted(set(item_topic_lookup).intersection(candidate_topic_lookup))
            if not shared_topic_ids and item.parent_id not in {candidate.id, candidate.parent_id}:
                continue
            shared_topics = [item_topic_lookup[topic_id] for topic_id in shared_topic_ids]
            ranked.append(
                (
                    len(shared_topic_ids),
                    int(candidate.parent_id == item.id or item.parent_id == candidate.id),
                    candidate,
                    shared_topics,
                )
            )
        ranked.sort(
            key=lambda entry: (
                entry[0],
                entry[1],
                entry[2].published_at or entry[2].fetched_at or entry[2].ingested_at,
                entry[2].title.casefold(),
            ),
            reverse=True,
        )
        return [(candidate, shared_topics) for _count, _parent_hit, candidate, shared_topics in ranked[:limit]]

    def _read_existing_page(self, *, namespace: str, slug: str) -> WikiPage | None:
        path = self.store.wiki_dir / namespace / f"{slug}.md"
        if not path.exists():
            return None
        return self.store.read_wiki_page(path)

    @staticmethod
    def _page_matches(existing: WikiPage, *, frontmatter: WikiPageFrontmatter, body: str) -> bool:
        return (
            existing.body == body
            and existing.frontmatter.id == frontmatter.id
            and existing.frontmatter.page_type == frontmatter.page_type
            and existing.frontmatter.title == frontmatter.title
            and existing.frontmatter.aliases == frontmatter.aliases
            and existing.frontmatter.source_refs == frontmatter.source_refs
            and existing.frontmatter.managed == frontmatter.managed
        )

    def _is_managed_page(self, path: Path) -> bool:
        try:
            return self.store.read_wiki_page(path).frontmatter.managed
        except Exception:
            return False

    def _source_page_relative_path(self, item: VaultItemRecord) -> str:
        return f"wiki/sources/{self._source_page_slug(item)}.md"

    @staticmethod
    def _source_page_slug(item: VaultItemRecord) -> str:
        title_slug = slugify(item.title, fallback=item.id)[:64]
        return f"{title_slug}-{short_hash(item.id, length=6)}"

    @staticmethod
    def _relative_wiki_link(*, current_path: str, target_path: str) -> str:
        current_dir = Path(current_path).parent.as_posix()
        return posixpath.relpath(target_path, start=current_dir)

    def _topic_title(self, topic_id: str) -> str:
        topic = next((entry for entry in self.store.load_insights_index().topics if entry.id == topic_id), None)
        if topic is not None:
            return topic.label
        return topic_id.replace("-", " ").title()

    def _page_markdown_link(self, *, label: str, current_path: str, target_path: str | None) -> str:
        if not target_path:
            return f"[[{label}]]"
        return f"[{label}]({self._relative_wiki_link(current_path=current_path, target_path=target_path)})"

    @staticmethod
    def _unique_strings(values: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for raw in values:
            value = str(raw or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result
