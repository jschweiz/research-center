from datetime import date, datetime, time

from pydantic import BaseModel, ConfigDict, Field


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class PaginatedResponse[T](BaseModel):
    items: list[T]
    total: int


class StatusResponse(BaseModel):
    status: str
    detail: str


class TimePreference(BaseModel):
    digest_time: time
    timezone: str


class DateEnvelope(BaseModel):
    date: date
    generated_at: datetime


class AlphaXivSimilarPaperRead(BaseModel):
    title: str
    canonical_url: str
    app_item_id: str | None = None
    authors: list[str] = Field(default_factory=list)
    short_summary: str | None = None


class AlphaXivPaperRead(BaseModel):
    short_summary: str | None = None
    filed_text: str | None = None
    audio_url: str | None = None
    similar_papers: list[AlphaXivSimilarPaperRead] = Field(default_factory=list)
