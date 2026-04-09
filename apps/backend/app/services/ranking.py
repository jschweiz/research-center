from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.orm import Session

from app.db.models import Item, ItemScore, ScoreBucket
from app.services.profile import DEFAULT_RANKING_THRESHOLDS, DEFAULT_RANKING_WEIGHTS, ProfileService


def _contains_any(text: str, values: list[str]) -> int:
    lowered = text.lower()
    return sum(1 for value in values if value.lower() in lowered)


def _normalize_llm_relevance(value: object) -> float | None:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    return round(min(max(score, 0.0), 1.0), 4)


class RankingService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.profile = ProfileService(db).get_profile()

    def score_item(self, item: Item) -> ItemScore:
        source_priority = item.source.priority if item.source else 50
        content_text = ((item.content.cleaned_text if item.content else "") or "").lower()
        title_text = item.title.lower()

        topic_match_count = _contains_any(f"{title_text}\n{content_text}", self.profile.favorite_topics)
        author_match_count = sum(
            1 for author in item.authors if author.lower() in {fav.lower() for fav in self.profile.favorite_authors}
        )
        source_match = 1.0 if item.source_name.lower() in {s.lower() for s in self.profile.favorite_sources} else 0.0
        ignored_penalty = _contains_any(f"{title_text}\n{content_text}", self.profile.ignored_topics)

        recency_hours = 48.0
        if item.published_at:
            published = item.published_at
            if published.tzinfo is None:
                published = published.replace(tzinfo=UTC)
            recency_hours = max((datetime.now(UTC) - published).total_seconds() / 3600, 0.0)
        recency_score = max(0.0, 1.0 - min(recency_hours / 72.0, 1.0))
        novelty_score = max(0.05, 0.8 - (len(item.cluster.items) - 1) * 0.08) if item.cluster else 0.7
        source_quality_score = min(max(source_priority / 100.0, 0.1), 1.0)
        author_match_score = min(author_match_count * 0.5, 1.0)
        topic_match_score = min(topic_match_count * 0.3 + source_match * 0.2, 1.0)
        zotero_affinity_score = min(
            max((max((match.similarity_score for match in item.zotero_matches), default=0.0)), 0.0),
            1.0,
        )
        heuristic_relevance_score = max(
            0.0,
            min(recency_score + topic_match_score * 0.4 + author_match_score * 0.3, 1.0),
        )
        metadata = item.metadata_json if isinstance(item.metadata_json, dict) else {}
        llm_enrichment = (
            metadata.get("llm_enrichment")
            if isinstance(metadata.get("llm_enrichment"), dict)
            else {}
        )
        llm_relevance_score = _normalize_llm_relevance(llm_enrichment.get("relevance_score"))
        llm_tags = llm_enrichment.get("tags") if isinstance(llm_enrichment.get("tags"), list) else []
        relevance_score = (
            llm_relevance_score if llm_relevance_score is not None else heuristic_relevance_score
        )

        weights = DEFAULT_RANKING_WEIGHTS | (self.profile.ranking_weights or {})
        thresholds = DEFAULT_RANKING_THRESHOLDS | (self.profile.ranking_thresholds or {})
        total = (
            relevance_score * weights["relevance"]
            + novelty_score * weights["novelty"]
            + source_quality_score * weights["source_quality"]
            + author_match_score * weights["author_match"]
            + topic_match_score * weights["topic_match"]
            + zotero_affinity_score * weights["zotero_affinity"]
            + item.manual_priority_boost
            - min(ignored_penalty * 0.15, 0.45)
        )
        total = round(max(0.0, min(total, 1.0)), 4)

        bucket = ScoreBucket.ARCHIVE
        if total >= thresholds["must_read_min"]:
            bucket = ScoreBucket.MUST_READ
        elif total >= thresholds["worth_a_skim_min"]:
            bucket = ScoreBucket.WORTH_A_SKIM

        score = item.score or ItemScore(item=item)
        score.relevance_score = round(relevance_score, 4)
        score.novelty_score = round(novelty_score, 4)
        score.source_quality_score = round(source_quality_score, 4)
        score.author_match_score = round(author_match_score, 4)
        score.topic_match_score = round(topic_match_score, 4)
        score.zotero_affinity_score = round(zotero_affinity_score, 4)
        score.total_score = total
        score.bucket = bucket
        reason_trace = {
            "recency_hours": round(recency_hours, 1),
            "topic_matches": topic_match_count,
            "author_matches": author_match_count,
            "source_priority": source_priority,
            "cluster_size": len(item.cluster.items) if item.cluster else 1,
            "favorite_source_match": bool(source_match),
            "ignored_penalty": ignored_penalty,
            "scoring_mode": "llm" if llm_relevance_score is not None else "heuristic",
            "heuristic_relevance_score": round(heuristic_relevance_score, 4),
        }
        if llm_relevance_score is not None:
            reason_trace["llm_reason"] = str(llm_enrichment.get("reason") or "")
            reason_trace["llm_tag_count"] = len(llm_tags)
            reason_trace["llm_author_fill"] = bool(llm_enrichment.get("author_applied"))
        score.reason_trace = reason_trace
        self.db.add(score)
        return score
