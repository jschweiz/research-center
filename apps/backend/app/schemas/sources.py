from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from app.db.models import RunStatus
from app.schemas.ops import IngestionRunHistoryRead
from app.schemas.profile import AlphaXivSort

SourceType = Literal["website", "gmail_newsletter"]
SourceClassificationMode = Literal["fixed", "written_content_auto"]
SourceDecompositionMode = Literal["none", "newsletter_entries"]


class SourceLatestExtractionRunRead(BaseModel):
    id: str
    status: RunStatus
    operation_kind: str
    summary: str
    started_at: datetime
    finished_at: datetime | None = None
    emitted_kinds: list[str] = Field(default_factory=list)


class SourceCreate(BaseModel):
    type: SourceType
    name: str
    raw_kind: str
    classification_mode: SourceClassificationMode = "fixed"
    decomposition_mode: SourceDecompositionMode = "none"
    url: str | None = None
    query: str | None = None
    description: str | None = None
    active: bool = True
    max_items: int = Field(default=20, ge=1, le=100)
    tags: list[str] = Field(default_factory=list)
    config_json: dict[str, Any] = Field(default_factory=dict)

    @field_validator("name", "raw_kind")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("This field is required.")
        return trimmed

    @model_validator(mode="after")
    def validate_locator(self) -> SourceCreate:
        if self.type == "website" and not (self.url and self.url.strip()):
            raise ValueError("Provide the website feed or index URL for this source.")
        if self.type == "gmail_newsletter" and not (
            (self.query and self.query.strip())
            or (self.config_json.get("senders") if isinstance(self.config_json, dict) else None)
            or (self.config_json.get("raw_query") if isinstance(self.config_json, dict) else None)
        ):
            raise ValueError("Provide the sender email list or Gmail query for this source.")
        if self.type == "gmail_newsletter" and self.raw_kind != "newsletter":
            raise ValueError("Gmail newsletter sources must write the newsletter raw kind.")
        return self


class SourceUpdate(BaseModel):
    name: str | None = None
    raw_kind: str | None = None
    classification_mode: SourceClassificationMode | None = None
    decomposition_mode: SourceDecompositionMode | None = None
    url: str | None = None
    query: str | None = None
    description: str | None = None
    active: bool | None = None
    max_items: int | None = Field(default=None, ge=1, le=100)
    tags: list[str] | None = None
    config_json: dict[str, Any] | None = None

    @field_validator("name", "raw_kind")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return value
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("This field is required.")
        return trimmed


class SourceRead(BaseModel):
    id: str
    type: SourceType
    name: str
    raw_kind: str
    classification_mode: SourceClassificationMode = "fixed"
    decomposition_mode: SourceDecompositionMode = "none"
    url: str | None
    query: str | None
    description: str | None
    active: bool
    max_items: int
    tags: list[str]
    config_json: dict[str, Any]
    last_synced_at: datetime | None
    created_at: datetime
    updated_at: datetime
    has_custom_pipeline: bool = False
    custom_pipeline_id: str | None = None
    latest_extraction_run: SourceLatestExtractionRunRead | None = None


class SourceProbeRead(BaseModel):
    source_id: str
    source_name: str
    source_type: SourceType
    total_found: int
    sample_titles: list[str] = Field(default_factory=list)
    detail: str
    checked_at: datetime


class SourceInjectRequest(BaseModel):
    max_items: int | None = Field(default=None, ge=1, le=250)
    alphaxiv_sort: AlphaXivSort | None = None


class SourceLatestLogRead(BaseModel):
    run: IngestionRunHistoryRead
