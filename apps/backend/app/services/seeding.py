from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import ContentType, Item, Source
from app.services.briefs import BriefService
from app.services.default_sources import upsert_default_sources
from app.services.ingestion import IngestionService
from app.services.profile import ProfileService


class SeedService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.ingestion = IngestionService(db)

    def ensure_demo_state(self) -> None:
        has_items = self.db.scalar(select(Item.id).limit(1)) is not None
        upsert_default_sources(self.db)
        if has_items:
            self.db.commit()
            return

        profile = ProfileService(self.db).get_profile()
        profile.favorite_topics = [
            "reasoning",
            "evaluation",
            "agents",
            "benchmarks",
            "open models",
            "retrieval",
            "alignment",
        ]
        self.db.add(profile)
        self.db.commit()

        sources = {
            source.name: source
            for source in self.db.scalars(
                select(Source).where(
                    Source.name.in_(
                        [
                            "Frontier AI Papers",
                            "OpenAI News",
                            "Hugging Face Blog",
                            "Import AI",
                            "TLDR AI",
                        ]
                    )
                )
            ).all()
        }

        now = datetime.now(UTC)
        demo_items = [
            {
                "source": sources["Frontier AI Papers"],
                "title": (
                    "Sparse verification routing improves reasoning benchmarks "
                    "without larger models"
                ),
                "url": "https://arxiv.org/abs/2603.12345",
                "authors": ["A. Rivera", "J. Cho"],
                "published_at": now - timedelta(days=1, hours=5),
                "text": (
                    "The paper proposes sparse verification routing for multi-step reasoning. "
                    "It routes hard examples through a verifier and keeps easy "
                    "examples on a cheaper path. "
                    "Results show stronger benchmark accuracy with lower average inference cost."
                ),
                "content_type": ContentType.PAPER,
            },
            {
                "source": sources["Frontier AI Papers"],
                "title": (
                    "Open evaluation suite reveals hidden regressions in frontier agent scaffolds"
                ),
                "url": "https://arxiv.org/abs/2603.54321",
                "authors": ["M. Patel", "S. Nguyen", "R. Zhao"],
                "published_at": now - timedelta(days=1, hours=9),
                "text": (
                    "This paper introduces a public evaluation suite for agent scaffolds. "
                    "It shows several frontier systems regress on long-horizon "
                    "reliability despite better demo performance. "
                    "The benchmark focuses on realistic tool misuse, retries, "
                    "and recovery behavior."
                ),
                "content_type": ContentType.PAPER,
            },
            {
                "source": sources["OpenAI News"],
                "title": "Why retrieval quality, not model size, is deciding enterprise agent wins",
                "url": "https://openai.com/news/",
                "authors": ["Dana Mercer"],
                "published_at": now - timedelta(days=1, hours=3),
                "text": (
                    "The article argues that retrieval coverage and freshness "
                    "now dominate enterprise agent quality. "
                    "Teams that can ground answers on live internal data beat "
                    "larger generic models on trust and throughput."
                ),
                "content_type": ContentType.ARTICLE,
            },
            {
                "source": sources["TLDR AI"],
                "title": "TLDR AI: two labs quietly converged on the same verifier pattern",
                "url": "https://mail.google.com/mail/u/0/#inbox/demo-field-notes-01",
                "authors": ["TLDR AI"],
                "published_at": now - timedelta(days=1, hours=2),
                "text": (
                    "Two independent labs appear to be converging on "
                    "lightweight verifier modules layered on top of planner "
                    "models. "
                    "The interesting signal is not the announcement itself but "
                    "how similar the system decomposition looks."
                ),
                "content_type": ContentType.NEWSLETTER,
            },
            {
                "source": sources["Hugging Face Blog"],
                "title": "Also mentioned: verifier routing shows up in product launch coverage",
                "url": "https://huggingface.co/blog",
                "authors": ["Dana Mercer"],
                "published_at": now - timedelta(days=1, hours=4),
                "text": (
                    "Several launch posts repeated the verifier routing claim "
                    "from the morning paper. "
                    "Most coverage adds little beyond framing and vendor positioning."
                ),
                "content_type": ContentType.ARTICLE,
            },
            {
                "source": sources["Import AI"],
                "title": "A small release note with unusually strong benchmark hygiene",
                "url": "https://www.deeplearning.ai/the-batch/",
                "authors": ["Marta Ilic"],
                "published_at": now - timedelta(days=1, hours=7),
                "text": (
                    "The post is minor on the surface but unusually explicit "
                    "about dataset contamination controls, confidence "
                    "intervals, and failure categories. "
                    "It is a good skim candidate because the methodology is "
                    "stronger than the headline."
                ),
                "content_type": ContentType.ARTICLE,
            },
        ]

        for entry in demo_items:
            self.ingestion.ingest_payload(
                source=entry["source"],
                title=entry["title"],
                canonical_url=entry["url"],
                authors=entry["authors"],
                published_at=entry["published_at"],
                cleaned_text=entry["text"],
                raw_payload={"seeded": True},
                outbound_links=[],
                extraction_confidence=0.9,
                metadata_json={"seeded": True},
                content_type=entry["content_type"],
            )

        brief_service = BriefService(self.db)
        brief_service.generate_digest(brief_service.current_edition_date(), force=True)
