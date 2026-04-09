from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

AdvancedJobType = Literal["compile", "health_check", "answer", "file_output"]
AdvancedOutputKind = Literal["answer", "slides", "chart"]
HealthCheckScope = Literal["vault", "wiki", "raw"]


class AdvancedCompileRequest(BaseModel):
    source_id: str | None = None
    doc_id: str | None = None
    limit: int | None = Field(default=None, ge=1, le=50)


class HealthCheckRequest(BaseModel):
    scope: HealthCheckScope = "vault"
    topic: str | None = None

    @field_validator("topic")
    @classmethod
    def normalize_topic(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class AnswerQueryRequest(BaseModel):
    question: str
    output_kind: AdvancedOutputKind = "answer"

    @field_validator("question")
    @classmethod
    def validate_question(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Question is required.")
        return normalized


class FileOutputRequest(BaseModel):
    path: str

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        normalized = value.strip().lstrip("/")
        if not normalized:
            raise ValueError("Path is required.")
        return normalized


class CodexManifestRawDoc(BaseModel):
    id: str
    kind: str
    title: str
    source_id: str | None = None
    source_name: str | None = None
    content_hash: str
    identity_hash: str | None = None
    canonical_url: str | None = None
    raw_doc_path: str
    short_summary: str | None = None
    published_at: datetime | None = None
    fetched_at: datetime | None = None
    doc_role: str = "primary"
    parent_id: str | None = None
    index_visibility: str = "visible"
    topic_refs: list[CodexManifestTopicRef] = Field(default_factory=list)
    trend_score: float = 0.0
    novelty_score: float = 0.0


class CodexManifestTopicRef(BaseModel):
    topic_id: str
    label: str
    score: float = 0.0


class CodexManifestTopic(BaseModel):
    id: str
    label: str
    slug: str
    page_path: str | None = None
    aliases: list[str] = Field(default_factory=list)
    representative_item_ids: list[str] = Field(default_factory=list)
    recent_item_count_7d: int = 0
    recent_item_count_30d: int = 0
    total_item_count: int = 0
    source_diversity: int = 0
    trend_score: float = 0.0
    novelty_score: float = 0.0
    related_topic_ids: list[str] = Field(default_factory=list)


class CodexManifestWikiPage(BaseModel):
    id: str
    page_type: str
    title: str
    namespace: str
    slug: str
    path: str
    aliases: list[str] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    backlinks: list[str] = Field(default_factory=list)
    updated_at: datetime | None = None
    managed: bool = True


class CodexEnrichmentManifest(BaseModel):
    run_id: str
    job_type: AdvancedJobType
    vault_root: str
    target_paths: list[str] = Field(default_factory=list)
    candidate_raw_docs: list[CodexManifestRawDoc] = Field(default_factory=list)
    candidate_wiki_pages: list[CodexManifestWikiPage] = Field(default_factory=list)
    candidate_topics: list[CodexManifestTopic] = Field(default_factory=list)
    rising_topics: list[CodexManifestTopic] = Field(default_factory=list)
    allowed_write_globs: list[str] = Field(default_factory=list)
    question: str | None = None
    output_kind: AdvancedOutputKind | None = None
    web_allowed: bool = False
    profile_context: dict[str, object] = Field(default_factory=dict)
    success_criteria: list[str] = Field(default_factory=list)


class CodexFollowUpJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_type: AdvancedJobType
    reason: str
    target_path: str | None = None


class CodexEnrichmentSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_type: AdvancedJobType
    summary: str
    touched_files: list[str] = Field(default_factory=list)
    created_wiki_pages: list[str] = Field(default_factory=list)
    updated_wiki_pages: list[str] = Field(default_factory=list)
    output_paths: list[str] = Field(default_factory=list)
    unresolved_questions: list[str] = Field(default_factory=list)
    suggested_follow_up_jobs: list[CodexFollowUpJob] = Field(default_factory=list)


class CompileStateEntry(BaseModel):
    doc_id: str
    last_compiled_content_hash: str
    last_compile_run_id: str
    affected_wiki_pages: list[str] = Field(default_factory=list)
    compiled_at: datetime


class CompileState(BaseModel):
    documents: dict[str, CompileStateEntry] = Field(default_factory=dict)
