from __future__ import annotations

from datetime import UTC, date, datetime, time
from enum import StrEnum
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    Time,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.types import EmbeddingVector


def uuid_str() -> str:
    return str(uuid4())


def utcnow() -> datetime:
    return datetime.now(UTC)


class SourceType(StrEnum):
    RSS = "rss"
    GMAIL = "gmail_newsletter"
    ARXIV = "arxiv"
    MANUAL = "manual_url"


class ContentType(StrEnum):
    ARTICLE = "article"
    PAPER = "paper"
    NEWSLETTER = "newsletter"
    POST = "post"
    THREAD = "thread"
    SIGNAL = "signal"


class IngestionRunType(StrEnum):
    INGEST = "ingest"
    DIGEST = "digest"
    ZOTERO_SYNC = "zotero_sync"
    CLEANUP = "cleanup"
    DEEPER_SUMMARY = "deeper_summary"


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ConnectionProvider(StrEnum):
    GMAIL = "gmail"
    ZOTERO = "zotero"


class ConnectionStatus(StrEnum):
    DISCONNECTED = "disconnected"
    CONNECTED = "connected"
    ERROR = "error"


class TriageStatus(StrEnum):
    UNREAD = "unread"
    REVIEW = "needs_review"
    SAVED = "saved"
    ARCHIVED = "archived"


class ScoreBucket(StrEnum):
    MUST_READ = "must_read"
    WORTH_A_SKIM = "worth_a_skim"
    ARCHIVE = "archive"


class ActionType(StrEnum):
    OPENED = "opened"
    ARCHIVED = "archived"
    STARRED = "starred"
    IGNORED_SIMILAR = "ignored_similar"
    SAVED_TO_ZOTERO = "saved_to_zotero"
    LISTENED = "listened"
    ASKED_DEEPER = "asked_deeper"


class DigestSection(StrEnum):
    HEADLINES = "headlines"
    EDITORIAL_SHORTLIST = "editorial_shortlist"
    INTERESTING_SIDE_SIGNALS = "interesting_side_signals"
    REMAINING_READS = "remaining_reads"


class DataMode(StrEnum):
    SEED = "seed"
    LIVE = "live"


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    type: Mapped[SourceType] = mapped_column(Enum(SourceType), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str | None] = mapped_column(String(2000))
    query: Mapped[str | None] = mapped_column(String(2000))
    description: Mapped[str | None] = mapped_column(Text)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=50, nullable=False)
    tags: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    config_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    rules: Mapped[list[SourceRule]] = relationship(back_populates="source", cascade="all, delete-orphan")
    items: Mapped[list[Item]] = relationship(back_populates="source")
    runs: Mapped[list[IngestionRun]] = relationship(back_populates="source")


class SourceRule(Base):
    __tablename__ = "source_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    source_id: Mapped[str | None] = mapped_column(ForeignKey("sources.id"))
    rule_type: Mapped[str] = mapped_column(String(100), nullable=False)
    value: Mapped[str] = mapped_column(String(2000), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    source: Mapped[Source | None] = relationship(back_populates="rules")


class ConnectionSecret(Base):
    __tablename__ = "connection_secrets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    provider: Mapped[ConnectionProvider] = mapped_column(Enum(ConnectionProvider), nullable=False)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    encrypted_payload: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[ConnectionStatus] = mapped_column(
        Enum(ConnectionStatus), default=ConnectionStatus.DISCONNECTED, nullable=False
    )
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    source_id: Mapped[str | None] = mapped_column(ForeignKey("sources.id"))
    run_type: Mapped[IngestionRunType] = mapped_column(Enum(IngestionRunType), nullable=False)
    status: Mapped[RunStatus] = mapped_column(Enum(RunStatus), default=RunStatus.PENDING, nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    error: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    source: Mapped[Source | None] = relationship(back_populates="runs")


class AIBudgetDay(Base):
    __tablename__ = "ai_budget_days"

    budget_date: Mapped[date] = mapped_column(Date, primary_key=True)
    spent_usd: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    reserved_usd: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    limit_usd: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class AIBudgetReservation(Base):
    __tablename__ = "ai_budget_reservations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    budget_date: Mapped[date] = mapped_column(Date, nullable=False)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    operation: Mapped[str] = mapped_column(String(100), nullable=False)
    state: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    estimated_cost_usd: Mapped[float] = mapped_column(Float, nullable=False)
    actual_cost_usd: Mapped[float | None] = mapped_column(Float)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ItemCluster(Base):
    __tablename__ = "item_clusters"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    summary_hint: Mapped[str | None] = mapped_column(Text)
    embedding: Mapped[list[float] | None] = mapped_column(EmbeddingVector(384))
    representative_item_id: Mapped[str | None] = mapped_column(ForeignKey("items.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    representative_item: Mapped[Item | None] = relationship(
        foreign_keys=[representative_item_id], post_update=True
    )
    items: Mapped[list[Item]] = relationship(
        back_populates="cluster", foreign_keys="Item.cluster_id", overlaps="representative_item"
    )
    mentions: Mapped[list[ItemMention]] = relationship(back_populates="cluster", cascade="all, delete-orphan")


class Item(Base):
    __tablename__ = "items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    source_id: Mapped[str | None] = mapped_column(ForeignKey("sources.id"))
    cluster_id: Mapped[str | None] = mapped_column(ForeignKey("item_clusters.id"))
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    source_name: Mapped[str] = mapped_column(String(255), nullable=False)
    authors: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    canonical_url: Mapped[str] = mapped_column(String(2000), nullable=False, unique=True)
    content_type: Mapped[ContentType] = mapped_column(Enum(ContentType), nullable=False)
    language: Mapped[str] = mapped_column(String(16), default="en", nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    ingest_status: Mapped[RunStatus] = mapped_column(Enum(RunStatus), default=RunStatus.SUCCEEDED, nullable=False)
    extraction_confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    triage_status: Mapped[TriageStatus] = mapped_column(
        Enum(TriageStatus), default=TriageStatus.UNREAD, nullable=False
    )
    starred: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    manual_priority_boost: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    source: Mapped[Source | None] = relationship(back_populates="items")
    cluster: Mapped[ItemCluster | None] = relationship(
        back_populates="items", foreign_keys=[cluster_id], overlaps="representative_item"
    )
    content: Mapped[ItemContent | None] = relationship(
        back_populates="item", uselist=False, cascade="all, delete-orphan"
    )
    score: Mapped[ItemScore | None] = relationship(
        back_populates="item", uselist=False, cascade="all, delete-orphan"
    )
    insight: Mapped[ItemInsight | None] = relationship(
        back_populates="item", uselist=False, cascade="all, delete-orphan"
    )
    mentions: Mapped[list[ItemMention]] = relationship(back_populates="item", cascade="all, delete-orphan")
    zotero_matches: Mapped[list[ZoteroMatch]] = relationship(
        back_populates="item", cascade="all, delete-orphan"
    )
    zotero_exports: Mapped[list[ZoteroExport]] = relationship(
        back_populates="item", cascade="all, delete-orphan"
    )
    actions: Mapped[list[UserAction]] = relationship(back_populates="item", cascade="all, delete-orphan")
    digest_entries: Mapped[list[DigestEntry]] = relationship(back_populates="item")


class ItemContent(Base):
    __tablename__ = "item_contents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    item_id: Mapped[str] = mapped_column(ForeignKey("items.id"), unique=True)
    raw_payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    cleaned_text: Mapped[str | None] = mapped_column(Text)
    extracted_text: Mapped[str | None] = mapped_column(Text)
    outbound_links: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    mime_type: Mapped[str | None] = mapped_column(String(255))
    word_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    raw_payload_retention_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    raw_payload_purged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    item: Mapped[Item] = relationship(back_populates="content")


class ItemMention(Base):
    __tablename__ = "item_mentions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    cluster_id: Mapped[str] = mapped_column(ForeignKey("item_clusters.id"))
    item_id: Mapped[str] = mapped_column(ForeignKey("items.id"))
    note: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    cluster: Mapped[ItemCluster] = relationship(back_populates="mentions")
    item: Mapped[Item] = relationship(back_populates="mentions")


class ItemScore(Base):
    __tablename__ = "item_scores"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    item_id: Mapped[str] = mapped_column(ForeignKey("items.id"), unique=True)
    relevance_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    novelty_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    source_quality_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    author_match_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    topic_match_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    zotero_affinity_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    total_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    bucket: Mapped[ScoreBucket] = mapped_column(Enum(ScoreBucket), default=ScoreBucket.ARCHIVE)
    reason_trace: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    item: Mapped[Item] = relationship(back_populates="score")


class ItemInsight(Base):
    __tablename__ = "item_insights"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    item_id: Mapped[str] = mapped_column(ForeignKey("items.id"), unique=True)
    short_summary: Mapped[str | None] = mapped_column(Text)
    why_it_matters: Mapped[str | None] = mapped_column(Text)
    whats_new: Mapped[str | None] = mapped_column(Text)
    caveats: Mapped[str | None] = mapped_column(Text)
    follow_up_questions: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    contribution: Mapped[str | None] = mapped_column(Text)
    method: Mapped[str | None] = mapped_column(Text)
    result: Mapped[str | None] = mapped_column(Text)
    limitation: Mapped[str | None] = mapped_column(Text)
    possible_extension: Mapped[str | None] = mapped_column(Text)
    deeper_summary: Mapped[str | None] = mapped_column(Text)
    experiment_ideas: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    item: Mapped[Item] = relationship(back_populates="insight")


class ZoteroMatch(Base):
    __tablename__ = "zotero_matches"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    item_id: Mapped[str] = mapped_column(ForeignKey("items.id"))
    library_item_key: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    similarity_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    item: Mapped[Item] = relationship(back_populates="zotero_matches")


class ZoteroExport(Base):
    __tablename__ = "zotero_exports"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    item_id: Mapped[str] = mapped_column(ForeignKey("items.id"))
    status: Mapped[RunStatus] = mapped_column(Enum(RunStatus), default=RunStatus.PENDING, nullable=False)
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    response_payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    error: Mapped[str | None] = mapped_column(Text)
    exported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    item: Mapped[Item] = relationship(back_populates="zotero_exports")


class UserAction(Base):
    __tablename__ = "user_actions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    item_id: Mapped[str] = mapped_column(ForeignKey("items.id"))
    action_type: Mapped[ActionType] = mapped_column(Enum(ActionType), nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    item: Mapped[Item] = relationship(back_populates="actions")


class Digest(Base):
    __tablename__ = "digests"
    __table_args__ = (UniqueConstraint("brief_date", "data_mode", name="uq_digests_brief_date_data_mode"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    brief_date: Mapped[date] = mapped_column(Date, nullable=False)
    data_mode: Mapped[DataMode] = mapped_column(Enum(DataMode), default=DataMode.SEED, nullable=False)
    status: Mapped[RunStatus] = mapped_column(Enum(RunStatus), default=RunStatus.SUCCEEDED, nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    editorial_note: Mapped[str | None] = mapped_column(Text)
    suggested_follow_ups: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    audio_brief_status: Mapped[str | None] = mapped_column(String(50))
    audio_brief_script: Mapped[str | None] = mapped_column(Text)
    audio_brief_chapters: Mapped[list[dict]] = mapped_column(JSON, default=list, nullable=False)
    audio_brief_error: Mapped[str | None] = mapped_column(Text)
    audio_brief_generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    audio_artifact_url: Mapped[str | None] = mapped_column(String(2000))
    audio_artifact_provider: Mapped[str | None] = mapped_column(String(100))
    audio_artifact_voice: Mapped[str | None] = mapped_column(String(100))
    audio_duration_seconds: Mapped[int | None] = mapped_column(Integer)
    audio_metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    entries: Mapped[list[DigestEntry]] = relationship(back_populates="digest", cascade="all, delete-orphan")


class DigestEntry(Base):
    __tablename__ = "digest_entries"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    digest_id: Mapped[str] = mapped_column(ForeignKey("digests.id"))
    item_id: Mapped[str] = mapped_column(ForeignKey("items.id"))
    section: Mapped[DigestSection] = mapped_column(Enum(DigestSection), nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    note: Mapped[str | None] = mapped_column(Text)

    digest: Mapped[Digest] = relationship(back_populates="entries")
    item: Mapped[Item] = relationship(back_populates="digest_entries")


class ProfileSetting(Base):
    __tablename__ = "profile_settings"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    favorite_topics: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    favorite_authors: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    favorite_sources: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    ignored_topics: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    digest_time: Mapped[time] = mapped_column(Time, default=lambda: time(hour=7, minute=0), nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), default="Europe/Zurich", nullable=False)
    data_mode: Mapped[DataMode] = mapped_column(Enum(DataMode), default=DataMode.SEED, nullable=False)
    summary_depth: Mapped[str] = mapped_column(String(50), default="balanced", nullable=False)
    ranking_weights: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
