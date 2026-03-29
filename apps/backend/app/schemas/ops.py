from datetime import date, datetime

from pydantic import BaseModel, Field

from app.db.models import IngestionRunType, RunStatus


class JobResponse(BaseModel):
    queued: bool
    task_name: str
    detail: str
    operation_run_id: str | None = None


class RegenerateBriefRequest(BaseModel):
    brief_date: date | None = None


class IngestionRunItemRead(BaseModel):
    title: str
    outcome: str
    content_type: str
    extraction_confidence: float


class IngestionRunSourceStatsRead(BaseModel):
    source_id: str | None = None
    source_name: str
    status: RunStatus
    ingested_count: int = 0
    created_count: int = 0
    updated_count: int = 0
    duplicate_mention_count: int = 0
    extractor_fallback_count: int = 0
    ai_prompt_tokens: int = 0
    ai_completion_tokens: int = 0
    ai_total_tokens: int = 0
    ai_cost_usd: float = 0.0
    average_extraction_confidence: float | None = None
    items: list[IngestionRunItemRead] = Field(default_factory=list)
    error: str | None = None


class OperationBasicInfoRead(BaseModel):
    label: str
    value: str


class OperationLogRead(BaseModel):
    logged_at: datetime
    level: str
    message: str


class IngestionRunHistoryRead(BaseModel):
    id: str
    run_type: IngestionRunType
    status: RunStatus
    operation_kind: str
    trigger: str | None = None
    title: str
    summary: str
    started_at: datetime
    finished_at: datetime | None
    affected_edition_days: list[date] = Field(default_factory=list)
    total_titles: int = 0
    source_count: int = 0
    failed_source_count: int = 0
    created_count: int = 0
    updated_count: int = 0
    duplicate_mention_count: int = 0
    extractor_fallback_count: int = 0
    ai_prompt_tokens: int = 0
    ai_completion_tokens: int = 0
    ai_total_tokens: int = 0
    ai_cost_usd: float = 0.0
    tts_cost_usd: float = 0.0
    total_cost_usd: float = 0.0
    average_extraction_confidence: float | None = None
    basic_info: list[OperationBasicInfoRead] = Field(default_factory=list)
    logs: list[OperationLogRead] = Field(default_factory=list)
    source_stats: list[IngestionRunSourceStatsRead] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
