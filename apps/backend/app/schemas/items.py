from datetime import UTC, datetime

from pydantic import BaseModel, Field, HttpUrl, field_serializer

from app.db.models import ContentType, RunStatus, ScoreBucket, TriageStatus
from app.schemas.common import AlphaXivPaperRead


def _serialize_utc_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.astimezone(UTC).isoformat(timespec="seconds")


class ManualImportRequest(BaseModel):
    url: HttpUrl


class ItemScoreRead(BaseModel):
    relevance_score: float = 0.0
    novelty_score: float = 0.0
    source_quality_score: float = 0.0
    author_match_score: float = 0.0
    topic_match_score: float = 0.0
    zotero_affinity_score: float = 0.0
    total_score: float = 0.0
    bucket: ScoreBucket = ScoreBucket.ARCHIVE
    reason_trace: dict = Field(default_factory=dict)


class ItemInsightRead(BaseModel):
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


class RelatedMention(BaseModel):
    item_id: str
    title: str
    source_name: str
    canonical_url: str


class ItemListEntry(BaseModel):
    id: str
    kind: str | None = None
    source_id: str | None = None
    title: str
    source_name: str
    organization_name: str | None = None
    authors: list[str]
    published_at: datetime | None
    canonical_url: str
    content_type: ContentType
    triage_status: TriageStatus
    starred: bool
    extraction_confidence: float
    short_summary: str | None = None
    bucket: ScoreBucket = ScoreBucket.ARCHIVE
    total_score: float = 0.0
    reason_trace: dict = Field(default_factory=dict)
    also_mentioned_in_count: int = 0

    @field_serializer("published_at", when_used="json")
    def serialize_published_at(self, value: datetime | None) -> str | None:
        return _serialize_utc_datetime(value)


class ItemDetailRead(BaseModel):
    id: str
    kind: str | None = None
    source_id: str | None = None
    title: str
    source_name: str
    organization_name: str | None = None
    authors: list[str]
    published_at: datetime | None
    canonical_url: str
    content_type: ContentType
    triage_status: TriageStatus
    starred: bool
    ingest_status: RunStatus
    extraction_confidence: float
    cleaned_text: str | None = None
    outbound_links: list[str] = Field(default_factory=list)
    raw_payload_retention_until: datetime | None = None
    score: ItemScoreRead = Field(default_factory=ItemScoreRead)
    insight: ItemInsightRead = Field(default_factory=ItemInsightRead)
    also_mentioned_in: list[RelatedMention] = Field(default_factory=list)
    zotero_matches: list[dict] = Field(default_factory=list)
    doc_role: str = "primary"
    parent_id: str | None = None
    asset_paths: list[str] = Field(default_factory=list)
    raw_doc_path: str | None = None
    lightweight_enrichment_status: str = "pending"
    lightweight_enriched_at: datetime | None = None
    alphaxiv: AlphaXivPaperRead | None = None

    @field_serializer("published_at", "raw_payload_retention_until", "lightweight_enriched_at", when_used="json")
    def serialize_datetimes(self, value: datetime | None) -> str | None:
        return _serialize_utc_datetime(value)


class ZoteroSaveRequest(BaseModel):
    tags: list[str] = Field(default_factory=list)
    note_prefix: str | None = None


class ActionRead(BaseModel):
    item_id: str
    triage_status: TriageStatus
    detail: str
