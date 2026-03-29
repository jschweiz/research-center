from __future__ import annotations

import logging
import math
import re
from difflib import SequenceMatcher
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import Item, ItemCluster, ItemMention

TITLE_CLEAN_RE = re.compile(r"[^a-z0-9\s]+")
logger = logging.getLogger(__name__)


def normalize_title(title: str) -> str:
    compact = TITLE_CLEAN_RE.sub(" ", title.lower())
    return " ".join(compact.split())


def cosine_similarity(a: list[float] | None, b: list[float] | None) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    numerator = sum(x * y for x, y in zip(a, b, strict=False))
    denom_a = math.sqrt(sum(x * x for x in a))
    denom_b = math.sqrt(sum(y * y for y in b))
    if not denom_a or not denom_b:
        return 0.0
    return numerator / (denom_a * denom_b)


class EmbeddingBackend:
    _model: Any = None
    _load_failed = False

    @classmethod
    def encode(cls, text: str) -> list[float] | None:
        if not get_settings().enable_embeddings:
            return None
        if cls._load_failed:
            return None
        if cls._model is None:
            try:
                from sentence_transformers import SentenceTransformer

                cls._model = SentenceTransformer(get_settings().sentence_transformer_model)
            except Exception:
                cls._load_failed = True
                logger.warning(
                    "clustering.embedding_backend_unavailable",
                    extra={
                        "embedding_model": get_settings().sentence_transformer_model,
                    },
                )
                return None
        vector = cls._model.encode(text, normalize_embeddings=True)
        return vector.tolist()


class ClusterService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def assign_item(self, item: Item) -> ItemCluster:
        normalized = normalize_title(item.title)
        embedding = EmbeddingBackend.encode(normalized)
        clusters = list(self.db.scalars(select(ItemCluster)).all())

        best_cluster: ItemCluster | None = None
        best_score = 0.0
        for cluster in clusters:
            lexical = SequenceMatcher(None, normalized, normalize_title(cluster.title)).ratio()
            semantic = cosine_similarity(embedding, cluster.embedding)
            combined = max(lexical, semantic)
            if combined > best_score:
                best_score = combined
                best_cluster = cluster

        if not best_cluster or best_score < 0.82:
            best_cluster = ItemCluster(title=item.title, summary_hint=item.title, embedding=embedding)
            self.db.add(best_cluster)
            self.db.flush()

        item.cluster = best_cluster
        mention_exists = self.db.scalar(
            select(ItemMention).where(
                ItemMention.cluster_id == best_cluster.id,
                ItemMention.item_id == item.id,
            )
        )
        if not mention_exists:
            best_cluster.mentions.append(ItemMention(item=item, note="Also mentioned in"))
        if not best_cluster.representative_item_id or best_cluster.representative_item and len(item.title) > len(best_cluster.representative_item.title):
            best_cluster.representative_item = item
        self.db.add(best_cluster)
        return best_cluster
