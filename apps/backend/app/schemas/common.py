from datetime import date, datetime, time

from pydantic import BaseModel, ConfigDict


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
