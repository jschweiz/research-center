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
    NEWS = "news"
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
    INTERRUPTED = "interrupted"


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
    identity_hash: Mapped[str | None] = mapped_column(String(64), index=True)
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
    ranking_thresholds: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    brief_sections: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    audio_brief_settings: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    prompt_guidance: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    alphaxiv_search_settings: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class PublishedEdition(Base):
    __tablename__ = "published_editions"
    __table_args__ = (UniqueConstraint("edition_id", name="uq_published_editions_edition_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    edition_id: Mapped[str] = mapped_column(String(120), nullable=False)
    period_type: Mapped[str] = mapped_column(String(20), nullable=False)
    brief_date: Mapped[date | None] = mapped_column(Date)
    week_start: Mapped[date | None] = mapped_column(Date)
    week_end: Mapped[date | None] = mapped_column(Date)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    digest_generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    has_audio: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    manifest_path: Mapped[str] = mapped_column(String(2000), nullable=False)
    manifest_size_bytes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    audio_path: Mapped[str | None] = mapped_column(String(2000))
    audio_size_bytes: Mapped[int | None] = mapped_column(Integer)
    cloudkit_record_name: Mapped[str | None] = mapped_column(String(255))
    cloudkit_environment: Mapped[str | None] = mapped_column(String(32))
    cloudkit_sync_status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class LocalPairingCode(Base):
    __tablename__ = "local_pairing_codes"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    local_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    redeemed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class PairedDevice(Base):
    __tablename__ = "paired_devices"
    __table_args__ = (UniqueConstraint("token_hash", name="uq_paired_devices_token_hash"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_ip: Mapped[str | None] = mapped_column(String(255))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    paired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class VaultSource(Base):
    __tablename__ = "vault_sources"

    id: Mapped[str] = mapped_column(String(120), primary_key=True)
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    raw_kind: Mapped[str] = mapped_column(String(80), nullable=False)
    custom_pipeline_id: Mapped[str | None] = mapped_column(String(120))
    classification_mode: Mapped[str] = mapped_column(String(80), default="fixed", nullable=False)
    decomposition_mode: Mapped[str] = mapped_column(String(80), default="none", nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    tags_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    url: Mapped[str | None] = mapped_column(String(2000))
    max_items: Mapped[int] = mapped_column(Integer, default=20, nullable=False)
    config_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class VaultRun(Base):
    __tablename__ = "vault_runs"

    id: Mapped[str] = mapped_column(String(120), primary_key=True)
    run_type: Mapped[IngestionRunType] = mapped_column(Enum(IngestionRunType), nullable=False)
    status: Mapped[RunStatus] = mapped_column(Enum(RunStatus), nullable=False)
    operation_kind: Mapped[str] = mapped_column(String(120), nullable=False)
    trigger: Mapped[str | None] = mapped_column(String(255))
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    prompt_path: Mapped[str | None] = mapped_column(String(2000))
    manifest_path: Mapped[str | None] = mapped_column(String(2000))
    changed_file_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    steps: Mapped[list[VaultRunStep]] = relationship(
        back_populates="run", cascade="all, delete-orphan", order_by="VaultRunStep.step_index.asc()"
    )
    events: Mapped[list[VaultRunEvent]] = relationship(
        back_populates="run", cascade="all, delete-orphan", order_by="VaultRunEvent.event_index.asc()"
    )


class VaultRunStep(Base):
    __tablename__ = "vault_run_steps"
    __table_args__ = (UniqueConstraint("run_id", "step_index", name="uq_vault_run_steps_run_step"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    run_id: Mapped[str] = mapped_column(ForeignKey("vault_runs.id"), nullable=False)
    step_index: Mapped[int] = mapped_column(Integer, nullable=False)
    step_kind: Mapped[str] = mapped_column(String(120), nullable=False)
    status: Mapped[RunStatus] = mapped_column(Enum(RunStatus), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_id: Mapped[str | None] = mapped_column(String(120))
    doc_id: Mapped[str | None] = mapped_column(String(255))
    created_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    skipped_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    counts_by_kind_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)

    run: Mapped[VaultRun] = relationship(back_populates="steps")


class VaultRunEvent(Base):
    __tablename__ = "vault_run_events"
    __table_args__ = (UniqueConstraint("run_id", "event_index", name="uq_vault_run_events_run_event"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid_str)
    run_id: Mapped[str] = mapped_column(ForeignKey("vault_runs.id"), nullable=False)
    event_index: Mapped[int] = mapped_column(Integer, nullable=False)
    logged_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    level: Mapped[str] = mapped_column(String(32), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    step_index: Mapped[int | None] = mapped_column(Integer)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)

    run: Mapped[VaultRun] = relationship(back_populates="events")


class VaultLease(Base):
    __tablename__ = "vault_leases"

    name: Mapped[str] = mapped_column(String(120), primary_key=True)
    owner: Mapped[str] = mapped_column(String(120), nullable=False)
    token: Mapped[str] = mapped_column(String(64), nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class VaultStopRequest(Base):
    __tablename__ = "vault_stop_requests"

    run_id: Mapped[str] = mapped_column(String(120), primary_key=True)
    source_id: Mapped[str | None] = mapped_column(String(120))
    requested_by: Mapped[str] = mapped_column(String(120), nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class VaultPairingCode(Base):
    __tablename__ = "vault_pairing_codes"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    local_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    redeemed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class VaultPairedDevice(Base):
    __tablename__ = "vault_paired_devices"
    __table_args__ = (UniqueConstraint("token_hash", name="uq_vault_paired_devices_token_hash"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_ip: Mapped[str | None] = mapped_column(String(255))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    paired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class VaultStarredItem(Base):
    __tablename__ = "vault_starred_items"

    item_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    starred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class VaultAIBudgetDay(Base):
    __tablename__ = "vault_ai_budget_days"

    budget_date: Mapped[date] = mapped_column(Date, primary_key=True)
    spent_usd: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    reserved_usd: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    limit_usd: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class VaultAIBudgetReservation(Base):
    __tablename__ = "vault_ai_budget_reservations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    budget_date: Mapped[date] = mapped_column(Date, nullable=False)
    provider: Mapped[str] = mapped_column(String(120), nullable=False)
    operation: Mapped[str] = mapped_column(String(120), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    estimated_cost_usd: Mapped[float] = mapped_column(Float, nullable=False)
    actual_cost_usd: Mapped[float | None] = mapped_column(Float)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class VaultAITrace(Base):
    __tablename__ = "vault_ai_traces"

    trace_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str | None] = mapped_column(String(120))
    provider: Mapped[str] = mapped_column(String(120), nullable=False)
    model: Mapped[str] = mapped_column(String(255), nullable=False)
    operation: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    prompt_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    prompt_path: Mapped[str | None] = mapped_column(String(2000))
    trace_path: Mapped[str | None] = mapped_column(String(2000))
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cost_usd: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    context_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    error: Mapped[str | None] = mapped_column(Text)


class VaultRawDocument(Base):
    __tablename__ = "vault_raw_documents"

    raw_doc_path: Mapped[str] = mapped_column(String(2000), primary_key=True)
    doc_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    kind: Mapped[str] = mapped_column(String(80), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    source_id: Mapped[str | None] = mapped_column(String(120))
    source_name: Mapped[str | None] = mapped_column(String(255))
    canonical_url: Mapped[str | None] = mapped_column(String(2000))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    identity_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    tags_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    asset_paths_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active", nullable=False)
    doc_role: Mapped[str] = mapped_column(String(32), default="primary", nullable=False)
    parent_id: Mapped[str | None] = mapped_column(String(255))
    index_visibility: Mapped[str] = mapped_column(String(32), default="visible", nullable=False)
    short_summary: Mapped[str | None] = mapped_column(Text)
    lightweight_enrichment_status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    lightweight_enriched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lightweight_enrichment_model: Mapped[str | None] = mapped_column(String(255))
    lightweight_scoring_model: Mapped[str | None] = mapped_column(String(255))
    body_text: Mapped[str | None] = mapped_column(Text)
    frontmatter_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class VaultItemProjection(Base):
    __tablename__ = "vault_items"

    item_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    raw_doc_path: Mapped[str] = mapped_column(String(2000), nullable=False, unique=True)
    kind: Mapped[str] = mapped_column(String(80), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    source_id: Mapped[str | None] = mapped_column(String(120), index=True)
    source_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    organization_name: Mapped[str | None] = mapped_column(String(255))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    canonical_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    content_type: Mapped[ContentType] = mapped_column(Enum(ContentType), nullable=False, index=True)
    extraction_confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    cleaned_text: Mapped[str | None] = mapped_column(Text)
    short_summary: Mapped[str | None] = mapped_column(Text)
    tags_text: Mapped[str] = mapped_column(Text, default="", nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active", nullable=False, index=True)
    doc_role: Mapped[str] = mapped_column(String(32), default="primary", nullable=False)
    parent_id: Mapped[str | None] = mapped_column(String(255))
    index_visibility: Mapped[str] = mapped_column(String(32), default="visible", nullable=False, index=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    identity_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    bucket: Mapped[ScoreBucket] = mapped_column(Enum(ScoreBucket), default=ScoreBucket.ARCHIVE, nullable=False, index=True)
    total_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False, index=True)
    trend_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    novelty_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    lightweight_enrichment_status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    lightweight_enriched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)


class VaultTopic(Base):
    __tablename__ = "vault_topics"

    topic_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    page_path: Mapped[str | None] = mapped_column(String(2000))
    source_diversity: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    total_item_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    recent_item_count_7d: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    recent_item_count_30d: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    trend_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False, index=True)
    novelty_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)


class VaultTopicEdge(Base):
    __tablename__ = "vault_topic_edges"

    source_topic_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    target_topic_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    weight: Mapped[int] = mapped_column(Integer, default=0, nullable=False)


class VaultWikiPage(Base):
    __tablename__ = "vault_wiki_pages"

    page_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    path: Mapped[str] = mapped_column(String(2000), nullable=False, unique=True)
    page_type: Mapped[str] = mapped_column(String(120), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    namespace: Mapped[str] = mapped_column(String(120), nullable=False)
    slug: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    managed: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)


class VaultPublishedEdition(Base):
    __tablename__ = "vault_published_editions"

    edition_id: Mapped[str] = mapped_column(String(120), primary_key=True)
    record_name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    period_type: Mapped[str] = mapped_column(String(32), nullable=False)
    brief_date: Mapped[date | None] = mapped_column(Date)
    week_start: Mapped[date | None] = mapped_column(Date)
    week_end: Mapped[date | None] = mapped_column(Date)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    has_audio: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)


class VaultProjectionState(Base):
    __tablename__ = "vault_projection_states"

    name: Mapped[str] = mapped_column(String(80), primary_key=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
