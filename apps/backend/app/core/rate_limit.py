from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from threading import Lock

from app.core.config import get_settings


@dataclass
class _AttemptBucket:
    failures: deque[datetime] = field(default_factory=deque)
    blocked_until: datetime | None = None


class LoginRateLimiter:
    def __init__(
        self,
        *,
        max_attempts: int,
        window: timedelta,
        lockout: timedelta,
    ) -> None:
        self.max_attempts = max_attempts
        self.window = window
        self.lockout = lockout
        self._lock = Lock()
        self._buckets: dict[str, _AttemptBucket] = {}

    def blocked_until(self, *, email: str, client_ip: str | None) -> datetime | None:
        now = datetime.now(UTC)
        with self._lock:
            return self._blocked_until_locked(email=email, client_ip=client_ip, now=now)

    def record_failure(self, *, email: str, client_ip: str | None) -> datetime | None:
        now = datetime.now(UTC)
        with self._lock:
            existing_block = self._blocked_until_locked(email=email, client_ip=client_ip, now=now)
            if existing_block is not None:
                return existing_block

            blocked_untils: list[datetime] = []
            for key in self._scope_keys(email=email, client_ip=client_ip):
                bucket = self._get_or_create_bucket(key, now)
                bucket.failures.append(now)
                if len(bucket.failures) >= self.max_attempts:
                    bucket.failures.clear()
                    bucket.blocked_until = now + self.lockout
                    blocked_untils.append(bucket.blocked_until)

            return max(blocked_untils) if blocked_untils else None

    def record_success(self, *, email: str, client_ip: str | None) -> None:
        with self._lock:
            for key in self._scope_keys(email=email, client_ip=client_ip):
                self._buckets.pop(key, None)

    def reset(self) -> None:
        with self._lock:
            self._buckets.clear()

    def _blocked_until_locked(
        self,
        *,
        email: str,
        client_ip: str | None,
        now: datetime,
    ) -> datetime | None:
        blocked_untils: list[datetime] = []
        for key in self._scope_keys(email=email, client_ip=client_ip):
            bucket = self._refresh_bucket(key, now)
            if bucket is not None and bucket.blocked_until is not None:
                blocked_untils.append(bucket.blocked_until)
        return max(blocked_untils) if blocked_untils else None

    def _get_or_create_bucket(self, key: str, now: datetime) -> _AttemptBucket:
        bucket = self._refresh_bucket(key, now)
        if bucket is None:
            bucket = _AttemptBucket()
            self._buckets[key] = bucket
        return bucket

    def _refresh_bucket(self, key: str, now: datetime) -> _AttemptBucket | None:
        bucket = self._buckets.get(key)
        if bucket is None:
            return None

        if bucket.blocked_until is not None and bucket.blocked_until <= now:
            bucket.blocked_until = None

        while bucket.failures and now - bucket.failures[0] > self.window:
            bucket.failures.popleft()

        if bucket.blocked_until is None and not bucket.failures:
            self._buckets.pop(key, None)
            return None

        return bucket

    def _scope_keys(self, *, email: str, client_ip: str | None) -> tuple[str, str]:
        normalized_email = email.strip().lower() or "<empty>"
        normalized_ip = (client_ip or "unknown").strip().lower() or "unknown"
        return (f"email:{normalized_email}", f"ip:{normalized_ip}")


@lru_cache
def get_login_rate_limiter() -> LoginRateLimiter:
    settings = get_settings()
    return LoginRateLimiter(
        max_attempts=settings.login_rate_limit_max_attempts,
        window=timedelta(minutes=settings.login_rate_limit_window_minutes),
        lockout=timedelta(minutes=settings.login_rate_limit_lockout_minutes),
    )


def reset_login_rate_limiter() -> None:
    try:
        limiter = get_login_rate_limiter()
    except Exception:
        get_login_rate_limiter.cache_clear()
        return
    limiter.reset()
    get_login_rate_limiter.cache_clear()
