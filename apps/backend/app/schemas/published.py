from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field

from app.db.models import ContentType, DataMode, ScoreBucket
from app.schemas.common import AlphaXivPaperRead


class PublishedItemListEntryRead(BaseModel):
    id: str
    kind: str | None = None
    source_id: str | None = None
    title: str
    source_name: str
    organization_name: str | None = None
    authors: list[str] = Field(default_factory=list)
    published_at: datetime | None = None
    canonical_url: str
    content_type: ContentType
    extraction_confidence: float = 0.0
    short_summary: str | None = None
    bucket: ScoreBucket = ScoreBucket.ARCHIVE
    total_score: float = 0.0
    reason_trace: dict = Field(default_factory=dict)
    also_mentioned_in_count: int = 0


class PublishedRelatedMentionRead(BaseModel):
    item_id: str
    title: str
    source_name: str
    canonical_url: str


class PublishedItemScoreRead(BaseModel):
    relevance_score: float = 0.0
    novelty_score: float = 0.0
    source_quality_score: float = 0.0
    author_match_score: float = 0.0
    topic_match_score: float = 0.0
    zotero_affinity_score: float = 0.0
    total_score: float = 0.0
    bucket: ScoreBucket = ScoreBucket.ARCHIVE
    reason_trace: dict = Field(default_factory=dict)


class PublishedItemInsightRead(BaseModel):
    short_summary: str | None = None
    why_it_matters: str | None = None
    whats_new: str | None = None
    caveats: str | None = None
    follow_up_questions: list[str] = Field(default_factory=list)
    contribution: str | None = None
    method: str | None = None
    result: str | None = None
    limitation: str | None = None
    possible_extension: str | None = None
    deeper_summary: str | None = None
    experiment_ideas: list[str] = Field(default_factory=list)


class PublishedItemDetailRead(BaseModel):
    id: str
    kind: str | None = None
    source_id: str | None = None
    title: str
    source_name: str
    organization_name: str | None = None
    authors: list[str] = Field(default_factory=list)
    published_at: datetime | None = None
    canonical_url: str
    content_type: ContentType
    extraction_confidence: float = 0.0
    cleaned_text: str | None = None
    outbound_links: list[str] = Field(default_factory=list)
    score: PublishedItemScoreRead = Field(default_factory=PublishedItemScoreRead)
    insight: PublishedItemInsightRead = Field(default_factory=PublishedItemInsightRead)
    also_mentioned_in: list[PublishedRelatedMentionRead] = Field(default_factory=list)
    doc_role: str = "primary"
    parent_id: str | None = None
    asset_paths: list[str] = Field(default_factory=list)
    raw_doc_path: str | None = None
    lightweight_enrichment_status: str = "pending"
    lightweight_enriched_at: datetime | None = None
    alphaxiv: AlphaXivPaperRead | None = None


class PublishedDigestEntryRead(BaseModel):
    item: PublishedItemListEntryRead
    note: str | None = None
    rank: int


class PublishedPaperTableEntryRead(BaseModel):
    item: PublishedItemListEntryRead
    rank: int
    zotero_tags: list[str] = Field(default_factory=list)
    credibility_score: int | None = None


class PublishedAudioBriefChapterRead(BaseModel):
    item_id: str
    item_title: str
    section: str
    rank: int
    headline: str
    narration: str
    offset_seconds: int


class PublishedAudioBriefRead(BaseModel):
    status: str
    script: str | None = None
    chapters: list[PublishedAudioBriefChapterRead] = Field(default_factory=list)
    estimated_duration_seconds: int | None = None
    audio_url: str | None = None
    audio_duration_seconds: int | None = None
    provider: str | None = None
    voice: str | None = None
    error: str | None = None
    generated_at: datetime | None = None
    metadata: dict = Field(default_factory=dict)


class PublishedDigestRead(BaseModel):
    id: str
    period_type: str = "day"
    brief_date: date | None = None
    week_start: date | None = None
    week_end: date | None = None
    coverage_start: date
    coverage_end: date
    data_mode: DataMode
    title: str
    editorial_note: str | None = None
    suggested_follow_ups: list[str] = Field(default_factory=list)
    audio_brief: PublishedAudioBriefRead | None = None
    generated_at: datetime
    headlines: list[PublishedDigestEntryRead] = Field(default_factory=list)
    editorial_shortlist: list[PublishedDigestEntryRead] = Field(default_factory=list)
    interesting_side_signals: list[PublishedDigestEntryRead] = Field(default_factory=list)
    remaining_reads: list[PublishedDigestEntryRead] = Field(default_factory=list)
    papers_table: list[PublishedPaperTableEntryRead] = Field(default_factory=list)


class PublishedAvailabilityDayRead(BaseModel):
    brief_date: date
    coverage_start: date
    coverage_end: date


class PublishedAvailabilityWeekRead(BaseModel):
    week_start: date
    week_end: date
    coverage_start: date
    coverage_end: date


class PublishedAvailabilityRead(BaseModel):
    default_day: date | None = None
    days: list[PublishedAvailabilityDayRead] = Field(default_factory=list)
    weeks: list[PublishedAvailabilityWeekRead] = Field(default_factory=list)


class PublishedEditionSummaryRead(BaseModel):
    edition_id: str
    record_name: str
    period_type: str
    brief_date: date | None = None
    week_start: date | None = None
    week_end: date | None = None
    title: str
    generated_at: datetime | None = None
    published_at: datetime
    has_audio: bool = False
    schema_version: int = 1


class PublishedEditionManifestRead(BaseModel):
    schema_version: int = 1
    edition: PublishedEditionSummaryRead
    availability: PublishedAvailabilityRead
    available_editions: list[PublishedEditionSummaryRead] = Field(default_factory=list)
    digest: PublishedDigestRead
    items: dict[str, PublishedItemDetailRead] = Field(default_factory=dict)
