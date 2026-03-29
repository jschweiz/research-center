from datetime import datetime

from pydantic import BaseModel, Field, field_validator, model_validator

from app.db.models import SourceType
from app.schemas.common import ORMModel


class SourceRuleRead(ORMModel):
    id: str
    rule_type: str
    value: str
    active: bool


class SourceRuleInput(BaseModel):
    rule_type: str
    value: str
    active: bool = True


class SourceCreate(BaseModel):
    type: SourceType
    name: str
    url: str | None = None
    query: str | None = None
    description: str | None = None
    active: bool = True
    priority: int = Field(default=50, ge=0, le=100)
    tags: list[str] = Field(default_factory=list)
    config_json: dict = Field(default_factory=dict)
    rules: list[SourceRuleInput] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("Source name is required.")
        return trimmed

    @model_validator(mode="after")
    def validate_locator(self) -> "SourceCreate":
        if not (self.url and self.url.strip()) and not (self.query and self.query.strip()):
            raise ValueError("Provide a URL or query for the source.")
        return self


class SourceUpdate(BaseModel):
    name: str | None = None
    url: str | None = None
    query: str | None = None
    description: str | None = None
    active: bool | None = None
    priority: int | None = Field(default=None, ge=0, le=100)
    tags: list[str] | None = None
    config_json: dict | None = None
    rules: list[SourceRuleInput] | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return value
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("Source name is required.")
        return trimmed


class SourceRead(ORMModel):
    id: str
    type: SourceType
    name: str
    url: str | None
    query: str | None
    description: str | None
    active: bool
    priority: int
    tags: list[str]
    config_json: dict
    last_synced_at: datetime | None
    created_at: datetime
    updated_at: datetime
    rules: list[SourceRuleRead] = Field(default_factory=list)


class SourceProbeRead(BaseModel):
    source_id: str
    source_name: str
    source_type: SourceType
    total_found: int
    sample_titles: list[str] = Field(default_factory=list)
    detail: str
    checked_at: datetime
