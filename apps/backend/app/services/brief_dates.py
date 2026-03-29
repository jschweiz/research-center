from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _zoneinfo(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def local_date_for_timestamp(value: datetime | None, timezone_name: str) -> date | None:
    normalized = _ensure_utc(value)
    if normalized is None:
        return None
    return normalized.astimezone(_zoneinfo(timezone_name)).date()


def coverage_day_for_datetimes(
    *,
    published_at: datetime | None,
    first_seen_at: datetime | None,
    timezone_name: str,
) -> date | None:
    return local_date_for_timestamp(published_at or first_seen_at, timezone_name)


def edition_day_for_datetimes(
    *,
    published_at: datetime | None,
    first_seen_at: datetime | None,
    timezone_name: str,
) -> date | None:
    coverage_day = coverage_day_for_datetimes(
        published_at=published_at,
        first_seen_at=first_seen_at,
        timezone_name=timezone_name,
    )
    if coverage_day is None:
        return None
    return coverage_day + timedelta(days=1)


def coverage_day_for_edition(brief_date: date) -> date:
    return brief_date - timedelta(days=1)


def iso_week_start(value: date) -> date:
    return value - timedelta(days=value.weekday())


def iso_week_end(week_start: date) -> date:
    return week_start + timedelta(days=6)

