from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

from app.db.models import ContentType, ScoreBucket
from app.schemas.ops import IngestionRunHistoryRead
from app.schemas.published import PublishedEditionSummaryRead


class LightweightJudgeScore(BaseModel):
    relevance_score: float = 0.0
    source_fit_score: float = 0.0
    topic_fit_score: float = 0.0
    author_fit_score: float = 0.0
    evidence_fit_score: float = 0.0
    confidence_score: float = 0.0
    bucket_hint: ScoreBucket = ScoreBucket.ARCHIVE
    reason: str | None = None
    evidence_quotes: list[str] = Field(default_factory=list)


class VaultItemScore(BaseModel):
    relevance_score: float = 0.0
    novelty_score: float = 0.0
    source_quality_score: float = 0.0
    author_match_score: float = 0.0
    topic_match_score: float = 0.0
    zotero_affinity_score: float = 0.0
    total_score: float = 0.0
    bucket: ScoreBucket = ScoreBucket.ARCHIVE
    reason_trace: dict[str, Any] = Field(default_factory=dict)


class RawDocumentFrontmatter(BaseModel):
    id: str
    kind: str
    title: str
    source_url: str | None = None
    source_name: str | None = None
    authors: list[str] = Field(default_factory=list)
    published_at: datetime | None = None
    ingested_at: datetime
    content_hash: str
    identity_hash: str | None = None
    tags: list[str] = Field(default_factory=list)
    status: str = "active"
    asset_paths: list[str] = Field(default_factory=list)
    source_id: str | None = None
    source_pipeline_id: str | None = None
    external_key: str | None = None
    canonical_url: str | None = None
    doc_role: str = "primary"
    parent_id: str | None = None
    index_visibility: str = "visible"
    fetched_at: datetime | None = None
    short_summary: str | None = None
    lightweight_enrichment_status: str = "pending"
    lightweight_enriched_at: datetime | None = None
    lightweight_enrichment_model: str | None = None
    lightweight_enrichment_input_hash: str | None = None
    lightweight_enrichment_error: str | None = None
    lightweight_scoring_model: str | None = None
    lightweight_scoring_input_hash: str | None = None
    lightweight_score: LightweightJudgeScore | None = None

    @field_validator("authors", "tags", "asset_paths", mode="before")
    @classmethod
    def normalize_string_lists(cls, value: object) -> list[str]:
        if not isinstance(value, list):
            return []
        normalized: list[str] = []
        for entry in value:
            cleaned = entry.strip() if isinstance(entry, str) else ""
            if cleaned:
                normalized.append(cleaned)
        return normalized


class RawDocument(BaseModel):
    frontmatter: RawDocumentFrontmatter
    body: str
    path: str


class VaultSourceDefinition(BaseModel):
    id: str
    type: str
    name: str
    enabled: bool = True
    raw_kind: str
    custom_pipeline_id: str | None = None
    classification_mode: str = "fixed"
    decomposition_mode: str = "none"
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    url: str | None = None
    max_items: int = Field(default=20, ge=1, le=100)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    config_json: dict[str, Any] = Field(default_factory=dict)


class VaultSourcesConfig(BaseModel):
    sources: list[VaultSourceDefinition] = Field(default_factory=list)


class WikiPageFrontmatter(BaseModel):
    id: str
    page_type: str
    title: str
    aliases: list[str] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    backlinks: list[str] = Field(default_factory=list)
    updated_at: datetime
    managed: bool = True


class WikiPage(BaseModel):
    frontmatter: WikiPageFrontmatter
    body: str
    path: str


class VaultItemRecord(BaseModel):
    id: str
    kind: str
    title: str
    source_id: str | None = None
    source_name: str
    organization_name: str | None = None
    authors: list[str] = Field(default_factory=list)
    published_at: datetime | None = None
    ingested_at: datetime
    fetched_at: datetime | None = None
    canonical_url: str
    content_type: ContentType
    extraction_confidence: float = 0.0
    cleaned_text: str | None = None
    outbound_links: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    status: str = "active"
    asset_paths: list[str] = Field(default_factory=list)
    content_hash: str
    identity_hash: str | None = None
    raw_doc_path: str
    doc_role: str = "primary"
    parent_id: str | None = None
    index_visibility: str = "visible"
    short_summary: str | None = None
    lightweight_enrichment_status: str = "pending"
    lightweight_enriched_at: datetime | None = None
    lightweight_enrichment_model: str | None = None
    topic_refs: list[ItemTopicReference] = Field(default_factory=list)
    trend_score: float = 0.0
    novelty_score: float = 0.0
    lightweight_scoring_model: str | None = None
    lightweight_score: LightweightJudgeScore | None = None
    score: VaultItemScore = Field(default_factory=VaultItemScore)
    updated_at: datetime


class ItemsIndex(BaseModel):
    generated_at: datetime
    items: list[VaultItemRecord] = Field(default_factory=list)


class ItemTopicReference(BaseModel):
    topic_id: str
    label: str
    score: float = 0.0
    aliases: list[str] = Field(default_factory=list)


class TopicConnection(BaseModel):
    source_topic_id: str
    target_topic_id: str
    weight: int = 0


class TopicIndexEntry(BaseModel):
    id: str
    label: str
    slug: str
    page_path: str | None = None
    aliases: list[str] = Field(default_factory=list)
    item_ids: list[str] = Field(default_factory=list)
    representative_item_ids: list[str] = Field(default_factory=list)
    source_names: list[str] = Field(default_factory=list)
    source_diversity: int = 0
    total_item_count: int = 0
    recent_item_count_7d: int = 0
    recent_item_count_30d: int = 0
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    trend_score: float = 0.0
    novelty_score: float = 0.0
    related_topic_ids: list[str] = Field(default_factory=list)


class InsightsIndex(BaseModel):
    generated_at: datetime
    topics: list[TopicIndexEntry] = Field(default_factory=list)
    connections: list[TopicConnection] = Field(default_factory=list)
    rising_topic_ids: list[str] = Field(default_factory=list)
    map_page_path: str | None = None
    trends_page_path: str | None = None


class PageIndexEntry(BaseModel):
    id: str
    page_type: str
    title: str
    namespace: str
    slug: str
    path: str
    aliases: list[str] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    backlinks: list[str] = Field(default_factory=list)
    updated_at: datetime
    managed: bool = True


class PagesIndex(BaseModel):
    generated_at: datetime
    pages: list[PageIndexEntry] = Field(default_factory=list)


class GraphNode(BaseModel):
    id: str
    label: str
    node_type: str
    path: str | None = None


class GraphEdge(BaseModel):
    source: str
    target: str
    edge_type: str


class GraphIndex(BaseModel):
    generated_at: datetime
    nodes: list[GraphNode] = Field(default_factory=list)
    edges: list[GraphEdge] = Field(default_factory=list)


class PublishedIndex(BaseModel):
    generated_at: datetime
    latest: PublishedEditionSummaryRead | None = None
    editions: list[PublishedEditionSummaryRead] = Field(default_factory=list)


class PairingCodeState(BaseModel):
    id: str
    label: str
    local_url: str
    expires_at: datetime
    redeemed_at: datetime | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class PairingCodesState(BaseModel):
    codes: list[PairingCodeState] = Field(default_factory=list)


class PairedDeviceState(BaseModel):
    id: str
    label: str
    token_hash: str
    last_used_at: datetime | None = None
    last_seen_ip: str | None = None
    revoked_at: datetime | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    paired_at: datetime
    updated_at: datetime


class PairedDevicesState(BaseModel):
    devices: list[PairedDeviceState] = Field(default_factory=list)


class StarredItemsState(BaseModel):
    item_ids: list[str] = Field(default_factory=list)


class LocalBudgetDayState(BaseModel):
    budget_date: date
    spent_usd: float = 0.0
    reserved_usd: float = 0.0
    limit_usd: float = 0.0
    updated_at: datetime


class LocalBudgetReservationState(BaseModel):
    id: str
    budget_date: date
    provider: str
    operation: str
    state: str
    estimated_cost_usd: float
    actual_cost_usd: float | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    finalized_at: datetime | None = None


class LocalBudgetState(BaseModel):
    days: list[LocalBudgetDayState] = Field(default_factory=list)
    reservations: list[LocalBudgetReservationState] = Field(default_factory=list)


class AITraceUsage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class AITraceReference(BaseModel):
    trace_id: str
    provider: str
    model: str
    operation: str
    status: str
    recorded_at: datetime
    duration_ms: int
    prompt_sha256: str
    prompt_path: str | None = None
    trace_path: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0
    context: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None


class AITraceArtifact(BaseModel):
    id: str
    recorded_at: datetime
    completed_at: datetime
    provider: str
    model: str
    operation: str
    status: str
    duration_ms: int
    request_id: str | None = None
    task_id: str | None = None
    task_name: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    prompt_sha256: str
    prompt_chars: int = 0
    system_instruction_chars: int = 0
    schema_name: str | None = None
    response_schema: dict[str, Any] | None = None
    max_output_tokens: int | None = None
    usage: AITraceUsage | None = None
    estimated_cost_usd: float | None = None
    actual_cost_usd: float | None = None
    prompt_path: str | None = None
    response_text: str | None = None
    parsed_output: Any = None
    provider_payload: Any = None
    error: str | None = None


class AIRunManifest(BaseModel):
    run_id: str
    generated_at: datetime
    trace_count: int = 0
    providers: list[str] = Field(default_factory=list)
    models: list[str] = Field(default_factory=list)
    operations: list[str] = Field(default_factory=list)
    ai_prompt_tokens: int = 0
    ai_completion_tokens: int = 0
    ai_total_tokens: int = 0
    ai_cost_usd: float = 0.0
    traces: list[AITraceReference] = Field(default_factory=list)


class LeaseState(BaseModel):
    name: str
    owner: str
    token: str
    acquired_at: datetime
    expires_at: datetime


class OperationStopRequestState(BaseModel):
    run_id: str
    source_id: str | None = None
    requested_by: str
    requested_at: datetime


class RunLogState(BaseModel):
    runs: list[IngestionRunHistoryRead] = Field(default_factory=list)
