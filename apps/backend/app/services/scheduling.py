from __future__ import annotations

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.services.profile import ProfileService


def ensure_utc(value: datetime | None = None) -> datetime:
    current = value or datetime.now(UTC)
    return current if current.tzinfo else current.replace(tzinfo=UTC)


class ScheduleService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.settings = get_settings()
        self.profile_service = ProfileService(db)

    def _zoneinfo(self, timezone_name: str | None) -> ZoneInfo:
        candidate = timezone_name or self.settings.timezone
        try:
            return ZoneInfo(candidate)
        except ZoneInfoNotFoundError:
            return ZoneInfo(self.settings.timezone)

    def local_now(self, timezone_name: str | None = None, now: datetime | None = None) -> datetime:
        return ensure_utc(now).astimezone(self._zoneinfo(timezone_name))

    def current_profile_date(self, now: datetime | None = None) -> date:
        profile = self.profile_service.get_profile()
        return self.local_now(profile.timezone, now).date()

    def is_profile_digest_due(self, now: datetime | None = None) -> bool:
        profile = self.profile_service.get_profile()
        local_now = self.local_now(profile.timezone, now)
        due_at = datetime.combine(local_now.date(), profile.digest_time, tzinfo=local_now.tzinfo)
        return local_now >= due_at

    def is_daily_job_due(
        self,
        *,
        last_run_at: datetime | None,
        due_time: time,
        timezone_name: str | None = None,
        now: datetime | None = None,
    ) -> bool:
        local_now = self.local_now(timezone_name, now)
        if local_now.time() < due_time:
            return False
        if not last_run_at:
            return True
        last_local = ensure_utc(last_run_at).astimezone(local_now.tzinfo)
        return last_local.date() < local_now.date()
