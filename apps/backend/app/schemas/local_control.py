from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field

from app.schemas.ops import IngestionRunHistoryRead, ItemsIndexStatusRead, JobResponse
from app.schemas.published import PublishedEditionSummaryRead


class PairRedeemRequest(BaseModel):
    pairing_token: str
    device_label: str | None = None


class PairRedeemResponse(BaseModel):
    device_label: str
    paired_local_url: str
    access_token: str
    hosted_return_url: str | None = None


class VaultGitStatusRead(BaseModel):
    enabled: bool
    repo_ready: bool
    branch: str | None = None
    remote_name: str | None = None
    remote_url: str | None = None
    current_commit: str | None = None
    current_summary: str | None = None
    has_uncommitted_changes: bool = False
    changed_files: int = 0
    ahead_count: int = 0
    behind_count: int = 0
    git_lfs_available: bool = False


class OllamaStatusRead(BaseModel):
    available: bool
    model: str | None = None
    detail: str | None = None


class CodexStatusRead(BaseModel):
    available: bool
    authenticated: bool = False
    binary: str | None = None
    model: str | None = None
    profile: str | None = None
    search_enabled: bool = False
    timeout_minutes: int | None = None
    compile_batch_size: int | None = None
    detail: str | None = None


class LocalControlInsightTopicRead(BaseModel):
    id: str
    label: str
    page_path: str | None = None
    recent_item_count_7d: int = 0
    recent_item_count_30d: int = 0
    total_item_count: int = 0
    source_diversity: int = 0
    trend_score: float = 0.0
    novelty_score: float = 0.0
    related_topic_ids: list[str] = Field(default_factory=list)


class LocalControlInsightsRead(BaseModel):
    map_page: str | None = None
    trends_page: str | None = None
    topics: list[LocalControlInsightTopicRead] = Field(default_factory=list)
    rising_topics: list[LocalControlInsightTopicRead] = Field(default_factory=list)


class LocalControlStatusRead(BaseModel):
    device_label: str
    paired_local_url: str
    vault_root_dir: str
    viewer_bundle_dir: str
    current_brief_date: date
    latest_publication: PublishedEditionSummaryRead | None = None
    latest_brief_dir: str | None = None
    raw_document_count: int = 0
    lightweight_pending_count: int = 0
    lightweight_metadata_pending_count: int = 0
    lightweight_scoring_pending_count: int = 0
    items_index: ItemsIndexStatusRead = Field(default_factory=ItemsIndexStatusRead)
    wiki_page_count: int = 0
    topic_count: int = 0
    rising_topic_count: int = 0
    vault_sync: VaultGitStatusRead | None = None
    ollama: OllamaStatusRead | None = None
    codex: CodexStatusRead | None = None


class LocalControlOperationsRead(BaseModel):
    runs: list[IngestionRunHistoryRead] = Field(default_factory=list)


class LocalControlJobResponse(JobResponse):
    published_edition: PublishedEditionSummaryRead | None = None
    completed_at: datetime | None = None
