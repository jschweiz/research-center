from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.models import Item, Source, SourceRule, SourceType


@dataclass(frozen=True)
class RuleSpec:
    rule_type: str
    value: str


@dataclass(frozen=True)
class SourceSpec:
    aliases: tuple[str, ...]
    source_type: SourceType
    name: str
    url: str | None
    query: str | None
    description: str
    priority: int
    tags: tuple[str, ...]
    config_json: dict[str, Any] = field(default_factory=dict)
    rules: tuple[RuleSpec, ...] = ()


DEFAULT_SOURCE_SPECS: tuple[SourceSpec, ...] = (
    SourceSpec(
        aliases=("Frontier AI Papers", "Research Papers"),
        source_type=SourceType.ARXIV,
        name="Frontier AI Papers",
        url=None,
        query=(
            "(cat:cs.AI OR cat:cs.CL OR cat:cs.LG) "
            'AND (all:"language model" OR all:agent OR all:reasoning)'
        ),
        description="Recent frontier-model papers spanning LLMs, agents, and reasoning.",
        priority=98,
        tags=("papers", "ai", "arxiv", "frontier"),
    ),
    SourceSpec(
        aliases=("Evaluation & Alignment Papers",),
        source_type=SourceType.ARXIV,
        name="Evaluation & Alignment Papers",
        url=None,
        query=(
            "(cat:cs.AI OR cat:cs.CL OR cat:cs.LG) "
            "AND (all:evaluation OR all:benchmark OR all:alignment "
            "OR all:interpretability OR all:safety)"
        ),
        description=(
            "Fresh papers focused on evaluation, safety, interpretability, and alignment work."
        ),
        priority=96,
        tags=("papers", "ai", "arxiv", "evaluation", "alignment"),
    ),
    SourceSpec(
        aliases=("Multimodal & Speech Papers",),
        source_type=SourceType.ARXIV,
        name="Multimodal & Speech Papers",
        url=None,
        query=(
            "(cat:cs.CV OR cat:cs.CL OR cat:cs.SD OR cat:eess.AS) "
            'AND (all:multimodal OR all:"vision language" OR all:audio OR all:speech)'
        ),
        description=(
            "Recent multimodal, vision-language, audio, and speech papers worth scanning."
        ),
        priority=94,
        tags=("papers", "ai", "arxiv", "multimodal", "speech"),
    ),
    SourceSpec(
        aliases=("OpenAI News",),
        source_type=SourceType.RSS,
        name="OpenAI News",
        url="https://openai.com/news/rss.xml",
        query=None,
        description="OpenAI product, research, and policy updates from the official news feed.",
        priority=92,
        tags=("rss", "ai", "frontier", "lab", "official"),
        config_json={"website_url": "https://openai.com/news"},
    ),
    SourceSpec(
        aliases=("Anthropic News", "Anthropic"),
        source_type=SourceType.RSS,
        name="Anthropic News",
        url="https://www.anthropic.com/news",
        query=None,
        description=(
            "Anthropic announcements, policy notes, and product updates from the official newsroom."
        ),
        priority=91,
        tags=("rss", "ai", "anthropic", "lab", "official"),
        config_json={
            "website_url": "https://www.anthropic.com/news",
            "discovery_mode": "website_index",
            "article_path_prefixes": ["/news/"],
        },
    ),
    SourceSpec(
        aliases=("Hugging Face Blog", "Latent Dispatch"),
        source_type=SourceType.RSS,
        name="Hugging Face Blog",
        url="https://huggingface.co/blog/feed.xml",
        query=None,
        description="Hugging Face engineering, open model, and research ecosystem updates.",
        priority=90,
        tags=("rss", "ai", "open-source", "models", "official"),
        config_json={"website_url": "https://huggingface.co/blog"},
    ),
    SourceSpec(
        aliases=("Google AI Blog",),
        source_type=SourceType.RSS,
        name="Google AI Blog",
        url="https://blog.google/innovation-and-ai/technology/ai/rss/",
        query=None,
        description=(
            "Google AI, Google DeepMind, and research-adjacent announcements "
            "from the official AI feed."
        ),
        priority=88,
        tags=("rss", "ai", "google", "deepmind", "official"),
        config_json={"website_url": "https://blog.google/innovation-and-ai/technology/ai/"},
    ),
    SourceSpec(
        aliases=("Mistral AI News", "Mistral News", "Mistral"),
        source_type=SourceType.RSS,
        name="Mistral AI News",
        url="https://mistral.ai/news",
        query=None,
        description=(
            "Mistral AI product, model, and research announcements from the official news page."
        ),
        priority=87,
        tags=("rss", "ai", "mistral", "lab", "official"),
        config_json={
            "website_url": "https://mistral.ai/news",
            "discovery_mode": "website_index",
            "article_path_prefixes": ["/news/"],
        },
    ),
    SourceSpec(
        aliases=("Microsoft Research Blog",),
        source_type=SourceType.RSS,
        name="Microsoft Research Blog",
        url="https://www.microsoft.com/en-us/research/feed/",
        query=None,
        description=(
            "Microsoft Research posts covering AI systems, methods, and broader computing research."
        ),
        priority=86,
        tags=("rss", "ai", "research", "microsoft", "official"),
        config_json={"website_url": "https://www.microsoft.com/en-us/research/blog/"},
    ),
    SourceSpec(
        aliases=("Together AI Blog",),
        source_type=SourceType.RSS,
        name="Together AI Blog",
        url="https://www.together.ai/blog/rss.xml",
        query=None,
        description=(
            "Official Together AI updates on open models, inference, agents, and infrastructure."
        ),
        priority=84,
        tags=("rss", "ai", "open-source", "inference", "official"),
        config_json={"website_url": "https://www.together.ai/blog"},
    ),
    SourceSpec(
        aliases=("Meta AI Engineering",),
        source_type=SourceType.RSS,
        name="Meta AI Engineering",
        url="https://engineering.fb.com/tag/ai/feed/",
        query=None,
        description=(
            "Meta engineering posts tagged for AI, with a bias toward production systems and infra."
        ),
        priority=82,
        tags=("rss", "ai", "meta", "engineering", "official"),
        config_json={"website_url": "https://engineering.fb.com/tag/ai/"},
    ),
    SourceSpec(
        aliases=("BAIR Blog",),
        source_type=SourceType.RSS,
        name="BAIR Blog",
        url="https://bair.berkeley.edu/blog/feed.xml",
        query=None,
        description=(
            "Berkeley AI Research writeups with stronger method detail than typical launch posts."
        ),
        priority=80,
        tags=("rss", "ai", "academic", "research"),
        config_json={"website_url": "https://bair.berkeley.edu/blog/"},
    ),
    SourceSpec(
        aliases=("Interconnects AI",),
        source_type=SourceType.RSS,
        name="Interconnects AI",
        url="https://www.interconnects.ai/feed",
        query=None,
        description=(
            "Research-heavy synthesis on frontier labs, scaling, and model development tradeoffs."
        ),
        priority=78,
        tags=("rss", "ai", "analysis", "research"),
        config_json={"website_url": "https://www.interconnects.ai"},
    ),
    SourceSpec(
        aliases=("SemiAnalysis AI",),
        source_type=SourceType.RSS,
        name="SemiAnalysis AI",
        url="https://www.semianalysis.com/feed",
        query=None,
        description=(
            "Deep dives on AI infrastructure, chips, training economics, and frontier-lab strategy."
        ),
        priority=76,
        tags=("rss", "ai", "analysis", "infrastructure"),
        config_json={"website_url": "https://www.semianalysis.com"},
    ),
    SourceSpec(
        aliases=("Latent Space",),
        source_type=SourceType.RSS,
        name="Latent Space",
        url="https://www.latent.space/feed",
        query=None,
        description=(
            "Practical coverage of agents, evaluation patterns, and developer "
            "tooling around LLM systems."
        ),
        priority=74,
        tags=("rss", "ai", "agents", "tooling"),
        config_json={"website_url": "https://www.latent.space"},
    ),
    SourceSpec(
        aliases=("Simon Willison",),
        source_type=SourceType.RSS,
        name="Simon Willison",
        url="https://simonwillison.net/atom/everything/",
        query=None,
        description=(
            "High-signal posts on LLM tooling, product changes, and open-source AI workflows."
        ),
        priority=72,
        tags=("rss", "ai", "tooling", "open-source"),
        config_json={"website_url": "https://simonwillison.net/"},
    ),
    SourceSpec(
        aliases=("Import AI",),
        source_type=SourceType.RSS,
        name="Import AI",
        url="https://importai.substack.com/feed",
        query=None,
        description=(
            "Jack Clark's long-running AI digest covering research, policy, "
            "and frontier-model developments."
        ),
        priority=70,
        tags=("rss", "ai", "analysis", "policy"),
        config_json={"website_url": "https://importai.substack.com"},
    ),
    SourceSpec(
        aliases=("TLDR AI", "Field Notes"),
        source_type=SourceType.GMAIL,
        name="TLDR AI",
        url=None,
        query="from:dan@tldrnewsletter.com",
        description="TLDR AI newsletter.",
        priority=66,
        tags=("newsletter", "ai", "briefing"),
        rules=(RuleSpec(rule_type="label", value="tldr-ai"),),
    ),
    SourceSpec(
        aliases=("Medium digest",),
        source_type=SourceType.GMAIL,
        name="Medium digest",
        url=None,
        query="from:noreply@medium.com",
        description="Medium newsletter with recent popular medium articles.",
        priority=66,
        tags=("newsletter", "ai", "medium", "briefing"),
        rules=(RuleSpec(rule_type="label", value="tldr-ai"),),
    ),
    SourceSpec(
        aliases=("Testing Catalog",),
        source_type=SourceType.GMAIL,
        name="Testing Catalog",
        url=None,
        query="from:testingcatalog@ghost.io",
        description="News on popular AI technologies and products.",
        priority=66,
        tags=("newsletter", "ai", "briefing"),
        rules=(RuleSpec(rule_type="label", value="tldr-ai"),),
    ),
)


def find_source(db: Session, aliases: tuple[str, ...]) -> Source | None:
    for name in aliases:
        source = db.scalar(select(Source).where(Source.name == name))
        if source:
            return source
    return None


def apply_source_spec(db: Session, spec: SourceSpec) -> str:
    source = find_source(db, spec.aliases)
    if source is None:
        source = Source(
            type=spec.source_type,
            name=spec.name,
            url=spec.url,
            query=spec.query,
            description=spec.description,
            active=True,
            priority=spec.priority,
            tags=list(spec.tags),
            config_json=dict(spec.config_json),
        )
        db.add(source)
        db.flush()
        action = "created"
    else:
        action = "updated"
        source.type = spec.source_type
        source.name = spec.name
        source.url = spec.url
        source.query = spec.query
        source.description = spec.description
        source.active = True
        source.priority = spec.priority
        source.tags = list(spec.tags)
        source.config_json = {**(source.config_json or {}), **dict(spec.config_json)}

    source.rules.clear()
    for rule in spec.rules:
        source.rules.append(SourceRule(rule_type=rule.rule_type, value=rule.value, active=True))

    db.add(source)
    db.flush()
    db.execute(update(Item).where(Item.source_id == source.id).values(source_name=source.name))
    return action


def upsert_default_sources(db: Session) -> list[tuple[str, str]]:
    return [(spec.name, apply_source_spec(db, spec)) for spec in DEFAULT_SOURCE_SPECS]
