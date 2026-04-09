from datetime import datetime, time
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator, model_validator

from app.db.models import DataMode
from app.schemas.common import ORMModel

AlphaXivSort = Literal["Hot", "Comments", "Views", "Likes", "GitHub", "Twitter (X)", "Recommended"]
AlphaXivInterval = Literal["3 Days", "7 Days", "30 Days", "90 Days", "All time"]
AlphaXivSource = Literal["GitHub", "Twitter (X)"]


def _normalize_string_list(value: object) -> list[str]:
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, (list, tuple, set)):
        candidates = list(value)
    else:
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for entry in candidates:
        cleaned = str(entry or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


class BriefSectionSettings(BaseModel):
    editorial_shortlist_count: int = Field(default=3, ge=1, le=12)
    headlines_count: int = Field(default=4, ge=0, le=20)
    side_signals_count: int = Field(default=3, ge=0, le=12)
    remaining_reads_count: int = Field(default=5, ge=0, le=20)
    papers_count: int = Field(default=5, ge=0, le=20)
    follow_up_questions_count: int = Field(default=5, ge=0, le=12)


class BriefSectionSettingsUpdate(BaseModel):
    editorial_shortlist_count: int | None = Field(default=None, ge=1, le=12)
    headlines_count: int | None = Field(default=None, ge=0, le=20)
    side_signals_count: int | None = Field(default=None, ge=0, le=12)
    remaining_reads_count: int | None = Field(default=None, ge=0, le=20)
    papers_count: int | None = Field(default=None, ge=0, le=20)
    follow_up_questions_count: int | None = Field(default=None, ge=0, le=12)


class AudioBriefSettings(BaseModel):
    target_duration_minutes: int = Field(default=5, ge=1, le=30)
    max_items_per_section: int = Field(default=3, ge=1, le=10)


class AudioBriefSettingsUpdate(BaseModel):
    target_duration_minutes: int | None = Field(default=None, ge=1, le=30)
    max_items_per_section: int | None = Field(default=None, ge=1, le=10)


class PromptGuidanceSettings(BaseModel):
    enrichment: str = Field(default="", max_length=2000)
    editorial_note: str = Field(default="", max_length=2000)
    audio_brief: str = Field(default="", max_length=2000)

    @field_validator("enrichment", "editorial_note", "audio_brief")
    @classmethod
    def strip_guidance(cls, value: str) -> str:
        return value.strip()


class PromptGuidanceSettingsUpdate(BaseModel):
    enrichment: str | None = Field(default=None, max_length=2000)
    editorial_note: str | None = Field(default=None, max_length=2000)
    audio_brief: str | None = Field(default=None, max_length=2000)

    @field_validator("enrichment", "editorial_note", "audio_brief")
    @classmethod
    def strip_guidance(cls, value: str | None) -> str | None:
        return value.strip() if value is not None else value


class AlphaXivSearchSettings(BaseModel):
    topics: list[str] = Field(default_factory=list, max_length=25)
    organizations: list[str] = Field(default_factory=list, max_length=25)
    sort: AlphaXivSort = "Hot"
    interval: AlphaXivInterval = "30 Days"
    source: AlphaXivSource | None = None

    @field_validator("topics", "organizations", mode="before")
    @classmethod
    def normalize_lists(cls, value: object) -> list[str]:
        return _normalize_string_list(value)


class AlphaXivSearchSettingsUpdate(BaseModel):
    topics: list[str] | None = Field(default=None, max_length=25)
    organizations: list[str] | None = Field(default=None, max_length=25)
    sort: AlphaXivSort | None = None
    interval: AlphaXivInterval | None = None
    source: AlphaXivSource | None = None

    @field_validator("topics", "organizations", mode="before")
    @classmethod
    def normalize_lists(cls, value: object) -> list[str] | None:
        if value is None:
            return None
        return _normalize_string_list(value)


class RankingThresholdSettings(BaseModel):
    must_read_min: float = Field(default=0.72, ge=0.0, le=1.0)
    worth_a_skim_min: float = Field(default=0.45, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def validate_order(self) -> "RankingThresholdSettings":
        if self.must_read_min <= self.worth_a_skim_min:
            raise ValueError("Must-read threshold must be higher than worth-a-skim threshold.")
        return self


class RankingThresholdSettingsUpdate(BaseModel):
    must_read_min: float | None = Field(default=None, ge=0.0, le=1.0)
    worth_a_skim_min: float | None = Field(default=None, ge=0.0, le=1.0)


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
    ranking_thresholds: RankingThresholdSettings = Field(default_factory=RankingThresholdSettings)
    brief_sections: BriefSectionSettings = Field(default_factory=BriefSectionSettings)
    audio_brief_settings: AudioBriefSettings = Field(default_factory=AudioBriefSettings)
    prompt_guidance: PromptGuidanceSettings = Field(default_factory=PromptGuidanceSettings)
    alphaxiv_search_settings: AlphaXivSearchSettings = Field(
        default_factory=AlphaXivSearchSettings
    )
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
    ranking_thresholds: RankingThresholdSettingsUpdate | None = None
    brief_sections: BriefSectionSettingsUpdate | None = None
    audio_brief_settings: AudioBriefSettingsUpdate | None = None
    prompt_guidance: PromptGuidanceSettingsUpdate | None = None
    alphaxiv_search_settings: AlphaXivSearchSettingsUpdate | None = None

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
