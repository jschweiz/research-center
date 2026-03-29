from datetime import date, datetime

from pydantic import BaseModel, Field

from app.db.models import DataMode
from app.schemas.items import ItemListEntry


class DigestEntryRead(BaseModel):
    item: ItemListEntry
    note: str | None = None
    rank: int


class PaperTableEntryRead(BaseModel):
    item: ItemListEntry
    rank: int
    zotero_tags: list[str] = Field(default_factory=list)
    credibility_score: int | None = None


class AudioBriefChapterRead(BaseModel):
    item_id: str
    item_title: str
    section: str
    rank: int
    headline: str
    narration: str
    offset_seconds: int


class AudioBriefRead(BaseModel):
    status: str
    script: str | None = None
    chapters: list[AudioBriefChapterRead] = Field(default_factory=list)
    estimated_duration_seconds: int | None = None
    audio_url: str | None = None
    audio_duration_seconds: int | None = None
    provider: str | None = None
    voice: str | None = None
    error: str | None = None
    generated_at: datetime | None = None
    metadata: dict = Field(default_factory=dict)


class BriefAvailabilityDayRead(BaseModel):
    brief_date: date
    coverage_start: date
    coverage_end: date


class BriefAvailabilityWeekRead(BaseModel):
    week_start: date
    week_end: date
    coverage_start: date
    coverage_end: date


class BriefAvailabilityRead(BaseModel):
    default_day: date | None = None
    days: list[BriefAvailabilityDayRead] = Field(default_factory=list)
    weeks: list[BriefAvailabilityWeekRead] = Field(default_factory=list)


class DigestRead(BaseModel):
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
    audio_brief: AudioBriefRead | None = None
    generated_at: datetime
    headlines: list[DigestEntryRead] = Field(default_factory=list)
    editorial_shortlist: list[DigestEntryRead] = Field(default_factory=list)
    interesting_side_signals: list[DigestEntryRead] = Field(default_factory=list)
    remaining_reads: list[DigestEntryRead] = Field(default_factory=list)
    papers_table: list[PaperTableEntryRead] = Field(default_factory=list)
