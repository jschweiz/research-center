from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from datetime import timedelta
from itertools import combinations

from app.services.text import normalize_whitespace
from app.services.vault_runtime import slugify, utcnow
from app.vault.models import (
    InsightsIndex,
    ItemTopicReference,
    TopicConnection,
    TopicIndexEntry,
    VaultItemRecord,
)
from app.vault.store import VaultStore

GENERIC_TOPIC_TAGS = {
    "ai",
    "alpha",
    "alphaXiv".casefold(),
    "analysis",
    "article",
    "audio",
    "blog",
    "blog-post",
    "github",
    "news",
    "note",
    "paper",
    "post",
    "research",
    "source",
    "summary",
    "thread",
    "transcript",
    "update",
    "video",
}
TOPIC_STOPWORDS = {
    "a",
    "an",
    "and",
    "for",
    "from",
    "in",
    "into",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}
ARXIV_TOPIC_LABELS = {
    "cs.ai": "Artificial Intelligence",
    "cs.cl": "Language Modeling",
    "cs.cv": "Computer Vision",
    "cs.lg": "Machine Learning",
    "cs.ro": "Robotics",
    "cs.ir": "Information Retrieval",
    "cs.mm": "Multimodal Systems",
    "cs.ma": "Multi-Agent Systems",
    "cs.se": "Software Engineering",
    "stat.ml": "Machine Learning",
}
CANONICAL_TOPIC_ALIASES = {
    "Agents": (
        "agent",
        "agents",
        "agentic",
        "multi-agent",
        "multi agent",
        "autonomous agent",
        "agent workflow",
    ),
    "Evaluations": (
        "eval",
        "evals",
        "evaluation",
        "evaluations",
        "benchmark",
        "benchmarks",
        "leaderboard",
    ),
    "Reasoning": (
        "reasoning",
        "chain-of-thought",
        "chain of thought",
        "deliberation",
        "thinking",
    ),
    "Test-Time Compute": (
        "test-time compute",
        "test time compute",
        "inference-time compute",
        "inference time compute",
        "reasoning-time compute",
    ),
    "Tool Use": (
        "tool use",
        "tool-use",
        "tool calling",
        "function calling",
        "tools",
    ),
    "Code Generation": (
        "code generation",
        "coding agent",
        "code agent",
        "software engineering agent",
        "program synthesis",
    ),
    "Retrieval-Augmented Generation": (
        "rag",
        "retrieval augmented generation",
        "retrieval-augmented generation",
        "retrieval generation",
    ),
    "Memory": (
        "memory",
        "episodic memory",
        "long-term memory",
        "long term memory",
    ),
    "Knowledge Graphs": (
        "knowledge graph",
        "knowledge graphs",
        "graph rag",
        "entity graph",
        "graph retrieval",
    ),
    "Search": (
        "search",
        "web search",
        "retrieval",
        "information retrieval",
    ),
    "Reinforcement Learning": (
        "reinforcement learning",
        "policy optimization",
        "rlhf",
        "dpo",
        "grpo",
    ),
    "Distillation": (
        "distillation",
        "knowledge distillation",
        "distilled",
        "teacher student",
    ),
    "Alignment": (
        "alignment",
        "preference optimization",
        "reward modeling",
        "constitutional",
        "alignment tax",
    ),
    "Safety": (
        "safety",
        "guardrail",
        "guardrails",
        "red teaming",
        "jailbreak",
        "safeguard",
    ),
    "Interpretability": (
        "interpretability",
        "mechanistic interpretability",
        "circuits",
        "feature attribution",
    ),
    "Synthetic Data": (
        "synthetic data",
        "data synthesis",
        "self-play data",
        "self play data",
    ),
    "Data Curation": (
        "dataset",
        "datasets",
        "data curation",
        "data mixture",
        "data filtering",
    ),
    "Fine-Tuning": (
        "fine tuning",
        "fine-tuning",
        "instruction tuning",
        "lora",
        "supervised fine-tuning",
    ),
    "Pretraining": (
        "pretraining",
        "pre-training",
        "scaling law",
        "scaling laws",
        "pretrain",
    ),
    "Efficiency": (
        "efficiency",
        "quantization",
        "pruning",
        "compression",
        "mixture-of-experts",
        "mixture of experts",
        "moe",
    ),
    "Inference": (
        "inference",
        "serving",
        "latency",
        "throughput",
        "deployment",
    ),
    "Open-Weight Models": (
        "open weight",
        "open-weight",
        "open model",
        "open source model",
        "open-source model",
    ),
    "Small Models": (
        "small model",
        "small models",
        "small language model",
        "small language models",
        "slm",
    ),
    "Long Context": (
        "long context",
        "context window",
        "long-context",
        "context compression",
    ),
    "Multimodal": (
        "multimodal",
        "vision-language",
        "vision language",
        "vlm",
        "multimodal systems",
    ),
    "Computer Vision": (
        "vision",
        "computer vision",
        "image understanding",
        "visual reasoning",
    ),
    "Image Generation": (
        "diffusion",
        "image generation",
        "text-to-image",
        "text to image",
    ),
    "Video Generation": (
        "video generation",
        "video model",
        "text-to-video",
        "text to video",
    ),
    "Audio": (
        "audio",
        "speech",
        "tts",
        "asr",
        "voice",
    ),
    "Robotics": (
        "robotics",
        "robot",
        "embodied",
        "embodied agent",
    ),
    "World Models": (
        "world model",
        "world models",
        "planning",
        "planner",
    ),
    "Infrastructure": (
        "gpu",
        "tpu",
        "infrastructure",
        "cluster",
        "distributed training",
        "systems",
    ),
}
TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9+./-]*")


class VaultInsightsService:
    def __init__(self, *, store: VaultStore | None = None, ensure_layout: bool = True) -> None:
        self.store = store or VaultStore()
        if ensure_layout:
            self.store.ensure_layout()
        self._canonical_alias_lookup = self._build_canonical_alias_lookup()

    def enrich_items(
        self,
        items: list[VaultItemRecord],
    ) -> tuple[list[VaultItemRecord], InsightsIndex]:
        annotated_items = [
            item.model_copy(
                update={
                    "topic_refs": self._extract_item_topics(item),
                    "trend_score": 0.0,
                    "novelty_score": 0.0,
                }
            )
            for item in items
        ]
        insights = self._build_index(annotated_items)
        topic_lookup = {topic.id: topic for topic in insights.topics}
        finalized_items = [
            item.model_copy(
                update={
                    "trend_score": round(
                        max((topic_lookup[ref.topic_id].trend_score for ref in item.topic_refs if ref.topic_id in topic_lookup), default=0.0),
                        4,
                    ),
                    "novelty_score": round(
                        max((topic_lookup[ref.topic_id].novelty_score for ref in item.topic_refs if ref.topic_id in topic_lookup), default=0.0),
                        4,
                    ),
                }
            )
            for item in annotated_items
        ]
        return finalized_items, insights

    def rebuild(self, *, items: list[VaultItemRecord] | None = None) -> InsightsIndex:
        source_items = list(items) if items is not None else list(self.store.load_items_index().items)
        enriched_items, insights = self.enrich_items(source_items)
        if items is None:
            self.store.save_items_index(self.store.load_items_index().model_copy(update={"generated_at": utcnow(), "items": enriched_items}))
        self.store.save_insights_index(insights)
        return insights

    def ensure_index(
        self,
        *,
        items: list[VaultItemRecord] | None = None,
        persist: bool = False,
    ) -> tuple[list[VaultItemRecord], InsightsIndex]:
        items_index = self.store.load_items_index() if items is None else None
        source_items = list(items) if items is not None else list(items_index.items)
        insights = self.store.load_insights_index()
        if not self._needs_refresh(
            items=source_items,
            insights=insights,
            items_generated_at=items_index.generated_at if items_index is not None else None,
        ):
            return source_items, insights

        enriched_items, refreshed_insights = self.enrich_items(source_items)
        if persist:
            self.store.save_insights_index(refreshed_insights)
            if items_index is not None:
                self.store.save_items_index(
                    items_index.model_copy(
                        update={
                            "generated_at": utcnow(),
                            "items": enriched_items,
                        }
                    )
                )
        return enriched_items, refreshed_insights

    def candidate_topics_for_items(
        self,
        items: list[VaultItemRecord],
        *,
        limit: int = 12,
    ) -> list[TopicIndexEntry]:
        _items, insights = self.ensure_index(persist=False)
        topic_ids: list[str] = []
        for item in items:
            topic_ids.extend(ref.topic_id for ref in item.topic_refs)
        if not topic_ids:
            return insights.topics[:limit]
        topic_lookup = {topic.id: topic for topic in insights.topics}
        selected = [topic_lookup[topic_id] for topic_id in dict.fromkeys(topic_ids) if topic_id in topic_lookup]
        selected.sort(
            key=lambda topic: (
                topic.trend_score,
                topic.recent_item_count_7d,
                topic.total_item_count,
                topic.label.casefold(),
            ),
            reverse=True,
        )
        return selected[:limit]

    def rising_topics(self, *, limit: int = 10) -> list[TopicIndexEntry]:
        _items, index = self.ensure_index(persist=False)
        lookup = {topic.id: topic for topic in index.topics}
        return [lookup[topic_id] for topic_id in index.rising_topic_ids[:limit] if topic_id in lookup]

    def load_index(self) -> InsightsIndex:
        return self.store.load_insights_index()

    def _needs_refresh(
        self,
        *,
        items: list[VaultItemRecord],
        insights: InsightsIndex,
        items_generated_at,
    ) -> bool:
        if not items:
            return bool(insights.topics or insights.connections or insights.rising_topic_ids)
        if items_generated_at is not None and insights.generated_at < items_generated_at:
            return True
        if not insights.topics:
            return True
        if any(not item.topic_refs for item in items):
            return True
        indexed_topic_ids = {topic.id for topic in insights.topics}
        item_topic_ids = {ref.topic_id for item in items for ref in item.topic_refs}
        return not item_topic_ids.issubset(indexed_topic_ids)

    def _build_index(self, items: list[VaultItemRecord]) -> InsightsIndex:
        now = utcnow()
        horizon_7d = now - timedelta(days=7)
        horizon_30d = now - timedelta(days=30)
        topic_items: dict[str, list[VaultItemRecord]] = defaultdict(list)
        topic_labels: dict[str, str] = {}
        topic_aliases: dict[str, set[str]] = defaultdict(set)
        topic_connections: Counter[tuple[str, str]] = Counter()

        for item in items:
            topic_ids: list[str] = []
            for ref in item.topic_refs:
                topic_labels.setdefault(ref.topic_id, ref.label)
                topic_aliases[ref.topic_id].update(ref.aliases)
                topic_items[ref.topic_id].append(item)
                topic_ids.append(ref.topic_id)
            for left, right in combinations(sorted(set(topic_ids)), 2):
                topic_connections[(left, right)] += 1

        related_lookup: dict[str, list[tuple[str, int]]] = defaultdict(list)
        connections = [
            TopicConnection(source_topic_id=source_id, target_topic_id=target_id, weight=weight)
            for (source_id, target_id), weight in sorted(
                topic_connections.items(),
                key=lambda entry: (-entry[1], entry[0][0], entry[0][1]),
            )
        ]
        for connection in connections:
            related_lookup[connection.source_topic_id].append((connection.target_topic_id, connection.weight))
            related_lookup[connection.target_topic_id].append((connection.source_topic_id, connection.weight))

        topics: list[TopicIndexEntry] = []
        for topic_id, grouped_items in topic_items.items():
            sorted_items = sorted(
                grouped_items,
                key=lambda item: (
                    item.published_at or item.fetched_at or item.ingested_at,
                    item.title.casefold(),
                ),
                reverse=True,
            )
            timestamps = [
                item.published_at or item.fetched_at or item.ingested_at
                for item in sorted_items
            ]
            first_seen_at = min(timestamps, default=None)
            last_seen_at = max(timestamps, default=None)
            recent_7d_count = sum(1 for timestamp in timestamps if timestamp >= horizon_7d)
            recent_30d_count = sum(1 for timestamp in timestamps if timestamp >= horizon_30d)
            prior_count = max(len(sorted_items) - recent_30d_count, 0)
            source_names = sorted({item.source_name for item in sorted_items if item.source_name})
            source_diversity = len(source_names)
            surge_ratio = recent_7d_count / max(1, recent_30d_count - recent_7d_count)
            maturity_penalty = math.log1p(prior_count)
            trend_score = (
                recent_7d_count * 3.1
                + recent_30d_count * 1.4
                + source_diversity * 1.15
                + min(surge_ratio, 6.0) * 2.0
                - maturity_penalty * 0.55
            )
            novelty_score = (
                min(surge_ratio, 4.0) * 1.2
                + (2.0 if first_seen_at is not None and first_seen_at >= horizon_30d else 0.0)
                + (1.0 if len(sorted_items) <= 3 and recent_7d_count > 0 else 0.0)
            )
            label = topic_labels.get(topic_id, topic_id)
            slug = slugify(label, fallback=topic_id)
            topics.append(
                TopicIndexEntry(
                    id=topic_id,
                    label=label,
                    slug=slug,
                    page_path=f"wiki/topics/{slug}.md",
                    aliases=sorted(topic_aliases.get(topic_id, set())),
                    item_ids=[item.id for item in sorted_items],
                    representative_item_ids=[item.id for item in sorted_items[:6]],
                    source_names=source_names,
                    source_diversity=source_diversity,
                    total_item_count=len(sorted_items),
                    recent_item_count_7d=recent_7d_count,
                    recent_item_count_30d=recent_30d_count,
                    first_seen_at=first_seen_at,
                    last_seen_at=last_seen_at,
                    trend_score=round(trend_score, 4),
                    novelty_score=round(novelty_score, 4),
                    related_topic_ids=[
                        related_topic_id
                        for related_topic_id, _weight in sorted(
                            related_lookup.get(topic_id, []),
                            key=lambda entry: (-entry[1], topic_labels.get(entry[0], entry[0]).casefold()),
                        )[:8]
                    ],
                )
            )

        topics.sort(
            key=lambda topic: (
                topic.trend_score,
                topic.recent_item_count_7d,
                topic.total_item_count,
                topic.label.casefold(),
            ),
            reverse=True,
        )
        rising_topic_ids = [topic.id for topic in topics[:24]]
        return InsightsIndex(
            generated_at=now,
            topics=topics,
            connections=connections,
            rising_topic_ids=rising_topic_ids,
            map_page_path="wiki/maps/global-ai-research.md",
            trends_page_path="wiki/trends/rising-topics.md",
        )

    def _extract_item_topics(self, item: VaultItemRecord) -> list[ItemTopicReference]:
        tags = [normalize_whitespace(tag) for tag in item.tags if normalize_whitespace(tag)]
        normalized_tags = {tag.casefold() for tag in tags}
        title = normalize_whitespace(item.title)
        summary = normalize_whitespace(item.short_summary or "")
        body = normalize_whitespace((item.cleaned_text or "")[:4000])
        title_lower = title.casefold()
        summary_lower = summary.casefold()
        body_lower = body.casefold()
        topic_scores: dict[str, float] = {}
        topic_aliases: dict[str, set[str]] = defaultdict(set)

        def push(label: str, score: float, alias: str | None = None) -> None:
            topic_id = slugify(label, fallback="topic")
            topic_scores[topic_id] = topic_scores.get(topic_id, 0.0) + score
            if alias:
                topic_aliases[topic_id].add(alias)

        for label, aliases in CANONICAL_TOPIC_ALIASES.items():
            matched = False
            for alias in aliases:
                alias_lower = alias.casefold()
                if alias_lower in normalized_tags:
                    push(label, 3.0, alias)
                    matched = True
                    continue
                if self._contains_phrase(title_lower, alias_lower):
                    push(label, 2.5, alias)
                    matched = True
                    continue
                if self._contains_phrase(summary_lower, alias_lower):
                    push(label, 1.5, alias)
                    matched = True
                    continue
                if self._contains_phrase(body_lower, alias_lower):
                    push(label, 0.75, alias)
                    matched = True
            if matched and label.casefold() in normalized_tags:
                push(label, 1.0, label)

        for raw_topic in tags:
            normalized_topic = self._normalize_external_topic(raw_topic)
            if normalized_topic is None:
                continue
            push(normalized_topic, 2.6, raw_topic)

        for raw_topic in self._extract_title_phrases(title):
            normalized_topic = self._normalize_external_topic(raw_topic)
            if normalized_topic is None:
                continue
            push(normalized_topic, 1.2, raw_topic)

        ranked_topics = sorted(
            topic_scores.items(),
            key=lambda entry: (-entry[1], entry[0]),
        )
        references: list[ItemTopicReference] = []
        for topic_id, score in ranked_topics[:6]:
            label = self._label_for_topic_id(topic_id)
            references.append(
                ItemTopicReference(
                    topic_id=topic_id,
                    label=label,
                    score=round(score, 4),
                    aliases=sorted(topic_aliases.get(topic_id, set())),
                )
            )

        if references:
            return references

        fallback = self._normalize_external_topic(item.kind) or "Research"
        return [
            ItemTopicReference(
                topic_id=slugify(fallback, fallback="research"),
                label=fallback,
                score=1.0,
                aliases=[item.kind],
            )
        ]

    def _build_canonical_alias_lookup(self) -> dict[str, str]:
        lookup: dict[str, str] = {}
        for label, aliases in CANONICAL_TOPIC_ALIASES.items():
            lookup[label.casefold()] = label
            for alias in aliases:
                lookup[alias.casefold()] = label
        for category, label in ARXIV_TOPIC_LABELS.items():
            lookup[category.casefold()] = label
        return lookup

    def _normalize_external_topic(self, value: str) -> str | None:
        normalized = normalize_whitespace(value).strip("[](){}.,:;!?")
        if not normalized:
            return None
        lowered = normalized.casefold()
        canonical = self._canonical_alias_lookup.get(lowered)
        if canonical:
            return canonical
        if lowered in GENERIC_TOPIC_TAGS:
            return None
        if lowered in ARXIV_TOPIC_LABELS:
            return ARXIV_TOPIC_LABELS[lowered]
        if not any(char.isalpha() for char in normalized):
            return None
        cleaned_tokens = [
            token
            for token in re.split(r"[\s_/:-]+", normalized)
            if token and token.casefold() not in TOPIC_STOPWORDS
        ]
        if not cleaned_tokens:
            return None
        if len(cleaned_tokens) == 1 and len(cleaned_tokens[0]) < 4 and cleaned_tokens[0].upper() == cleaned_tokens[0]:
            return None
        return " ".join(self._normalize_topic_token(token) for token in cleaned_tokens)[:72]

    def _extract_title_phrases(self, title: str) -> list[str]:
        tokens = TOKEN_RE.findall(title)
        if len(tokens) < 2:
            return []
        phrases: list[str] = []
        for size in (3, 2):
            for index in range(len(tokens) - size + 1):
                chunk = tokens[index : index + size]
                lowered_chunk = [token.casefold() for token in chunk]
                if all(token in TOPIC_STOPWORDS for token in lowered_chunk):
                    continue
                if not any(
                    token in self._canonical_alias_lookup
                    or token in {"ai", "agent", "agents", "rag", "llm", "rl", "vision", "audio", "safety", "alignment"}
                    for token in lowered_chunk
                ):
                    continue
                phrase = " ".join(chunk)
                if phrase.casefold() not in {entry.casefold() for entry in phrases}:
                    phrases.append(phrase)
        return phrases[:4]

    def _label_for_topic_id(self, topic_id: str) -> str:
        for label in CANONICAL_TOPIC_ALIASES:
            if slugify(label, fallback="topic") == topic_id:
                return label
        return topic_id.replace("-", " ").title()

    @staticmethod
    def _contains_phrase(text: str, phrase: str) -> bool:
        if not text or not phrase:
            return False
        pattern = rf"(?<![A-Za-z0-9]){re.escape(phrase)}(?![A-Za-z0-9])"
        return re.search(pattern, text) is not None

    @staticmethod
    def _normalize_topic_token(token: str) -> str:
        uppercase_tokens = {
            "ai",
            "api",
            "asr",
            "dpo",
            "gpu",
            "llm",
            "lora",
            "ml",
            "moe",
            "nlp",
            "rag",
            "rl",
            "rlhf",
            "slm",
            "tts",
            "vlm",
        }
        lowered = token.casefold()
        if lowered in uppercase_tokens:
            return lowered.upper()
        if re.fullmatch(r"[A-Z0-9.-]{2,}", token):
            return token
        return token.capitalize()
