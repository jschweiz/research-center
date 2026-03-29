from datetime import datetime, time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator

from app.db.models import DataMode
from app.schemas.common import ORMModel


class ProfileRead(ORMModel):
    id: str
    favorite_topics: list[str] = Field(default_factory=list)
    favorite_authors: list[str] = Field(default_factory=list)
    favorite_sources: list[str] = Field(default_factory=list)
    ignored_topics: list[str] = Field(default_factory=list)
    digest_time: time
    timezone: str
    data_mode: DataMode
    summary_depth: str
    ranking_weights: dict
    created_at: datetime
    updated_at: datetime


class ProfileUpdate(BaseModel):
    favorite_topics: list[str] | None = None
    favorite_authors: list[str] | None = None
    favorite_sources: list[str] | None = None
    ignored_topics: list[str] | None = None
    digest_time: time | None = None
    timezone: str | None = None
    data_mode: DataMode | None = None
    summary_depth: str | None = None
    ranking_weights: dict | None = None

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str | None) -> str | None:
        if value is None:
            return value
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("Invalid IANA timezone.") from exc
        return value
