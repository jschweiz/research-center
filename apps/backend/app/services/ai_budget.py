from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from time import sleep
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError, OperationalError

from app.core.config import get_settings
from app.db.models import AIBudgetDay, AIBudgetReservation, IngestionRun, RunStatus
from app.db.session import get_session_factory

logger = logging.getLogger(__name__)

_ACTIVE_RESERVATION_STATE = "active"
_CONSUMED_RESERVATION_STATE = "consumed"
_RELEASED_RESERVATION_STATE = "released"
_COST_PRECISION = 6
_RESERVATION_RETRY_ATTEMPTS = 3
_SQLITE_LOCK_MESSAGES = (
    "database is locked",
    "database table is locked",
    "database schema is locked",
)


def _round_cost(value: float) -> float:
    return round(max(0.0, float(value)), _COST_PRECISION)


class AIBudgetExceededError(RuntimeError):
    pass


@dataclass(frozen=True)
class AIBudgetReservationHandle:
    reservation_id: str
    budget_date: date
    provider: str
    operation: str
    estimated_cost_usd: float


class AIBudgetService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._timezone = ZoneInfo(self.settings.timezone)

    def current_budget_date(self) -> date:
        return datetime.now(UTC).astimezone(self._timezone).date()

    def reserve_estimated_cost(
        self,
        *,
        provider: str,
        operation: str,
        estimated_cost_usd: float,
        metadata: dict[str, Any] | None = None,
    ) -> AIBudgetReservationHandle | None:
        normalized_cost = _round_cost(estimated_cost_usd)
        if normalized_cost <= 0:
            return None

        budget_date = self.current_budget_date()
        limit_usd = _round_cost(self.settings.ai_daily_cost_limit_usd)
        for attempt in range(_RESERVATION_RETRY_ATTEMPTS):
            with get_session_factory()() as db:
                try:
                    with db.begin():
                        day = self._ensure_budget_day_row(db, budget_date=budget_date)
                        released_total = self._release_stale_reservations(
                            db,
                            day=day,
                            budget_date=budget_date,
                        )
                        current_spent = _round_cost(day.spent_usd)
                        current_reserved = _round_cost(day.reserved_usd)
                        projected_total = _round_cost(
                            current_spent + current_reserved + normalized_cost
                        )
                        if projected_total > limit_usd:
                            raise self._budget_exceeded_error(
                                budget_date=budget_date,
                                provider=provider,
                                operation=operation,
                                current_spent=current_spent,
                                current_reserved=current_reserved,
                                requested_cost=normalized_cost,
                                limit_usd=limit_usd,
                            )

                        updated = db.execute(
                            update(AIBudgetDay)
                            .where(AIBudgetDay.budget_date == budget_date)
                            .where(
                                AIBudgetDay.spent_usd + AIBudgetDay.reserved_usd + normalized_cost
                                <= AIBudgetDay.limit_usd
                            )
                            .values(
                                reserved_usd=AIBudgetDay.reserved_usd + normalized_cost,
                                limit_usd=limit_usd,
                                updated_at=datetime.now(UTC),
                            )
                        )
                        if updated.rowcount != 1:
                            db.expire_all()
                            refreshed_day = db.get(AIBudgetDay, budget_date)
                            raise self._budget_exceeded_error(
                                budget_date=budget_date,
                                provider=provider,
                                operation=operation,
                                current_spent=_round_cost(
                                    refreshed_day.spent_usd if refreshed_day else current_spent
                                ),
                                current_reserved=_round_cost(
                                    refreshed_day.reserved_usd
                                    if refreshed_day
                                    else current_reserved
                                ),
                                requested_cost=normalized_cost,
                                limit_usd=limit_usd,
                            )

                        reservation = AIBudgetReservation(
                            budget_date=budget_date,
                            provider=provider,
                            operation=operation,
                            state=_ACTIVE_RESERVATION_STATE,
                            estimated_cost_usd=normalized_cost,
                            metadata_json=dict(metadata or {}),
                        )
                        db.add(reservation)
                        db.flush()

                    logger.info(
                        "ai_budget.reserved",
                        extra={
                            "budget_date": budget_date.isoformat(),
                            "provider": provider,
                            "operation": operation,
                            "estimated_cost_usd": normalized_cost,
                            "released_stale_cost_usd": released_total,
                        },
                    )
                    return AIBudgetReservationHandle(
                        reservation_id=reservation.id,
                        budget_date=budget_date,
                        provider=provider,
                        operation=operation,
                        estimated_cost_usd=normalized_cost,
                    )
                except IntegrityError:
                    if attempt + 1 < _RESERVATION_RETRY_ATTEMPTS:
                        continue
                    raise
                except OperationalError as exc:
                    if self._retryable_sqlite_lock_error(exc, attempt=attempt):
                        continue
                    raise

        raise RuntimeError(
            f"Failed to reserve AI budget after {_RESERVATION_RETRY_ATTEMPTS} attempts."
        )

    def consume_reservation(
        self,
        handle: AIBudgetReservationHandle | None,
        *,
        actual_cost_usd: float,
    ) -> None:
        if handle is None:
            return
        normalized_actual = _round_cost(actual_cost_usd)
        with get_session_factory()() as db, db.begin():
            reservation = db.get(AIBudgetReservation, handle.reservation_id)
            if reservation is None or reservation.state != _ACTIVE_RESERVATION_STATE:
                return

            day = db.get(AIBudgetDay, reservation.budget_date)
            if day is None:
                return

            day.reserved_usd = _round_cost(
                max(day.reserved_usd - reservation.estimated_cost_usd, 0.0)
            )
            day.spent_usd = _round_cost(day.spent_usd + normalized_actual)
            day.updated_at = datetime.now(UTC)
            reservation.state = _CONSUMED_RESERVATION_STATE
            reservation.actual_cost_usd = normalized_actual
            reservation.finalized_at = datetime.now(UTC)
            db.add(day)
            db.add(reservation)

        if normalized_actual > reservation.estimated_cost_usd:
            logger.error(
                "ai_budget.estimate_underflow",
                extra={
                    "budget_date": handle.budget_date.isoformat(),
                    "provider": handle.provider,
                    "operation": handle.operation,
                    "estimated_cost_usd": reservation.estimated_cost_usd,
                    "actual_cost_usd": normalized_actual,
                },
            )
        logger.info(
            "ai_budget.consumed",
            extra={
                "budget_date": handle.budget_date.isoformat(),
                "provider": handle.provider,
                "operation": handle.operation,
                "estimated_cost_usd": reservation.estimated_cost_usd,
                "actual_cost_usd": normalized_actual,
            },
        )

    def release_reservation(self, handle: AIBudgetReservationHandle | None) -> None:
        if handle is None:
            return
        with get_session_factory()() as db, db.begin():
            reservation = db.get(AIBudgetReservation, handle.reservation_id)
            if reservation is None or reservation.state != _ACTIVE_RESERVATION_STATE:
                return

            day = db.get(AIBudgetDay, reservation.budget_date)
            if day is None:
                return

            day.reserved_usd = _round_cost(
                max(day.reserved_usd - reservation.estimated_cost_usd, 0.0)
            )
            day.updated_at = datetime.now(UTC)
            reservation.state = _RELEASED_RESERVATION_STATE
            reservation.actual_cost_usd = 0.0
            reservation.finalized_at = datetime.now(UTC)
            db.add(day)
            db.add(reservation)

        logger.info(
            "ai_budget.released",
            extra={
                "budget_date": handle.budget_date.isoformat(),
                "provider": handle.provider,
                "operation": handle.operation,
                "estimated_cost_usd": reservation.estimated_cost_usd,
            },
        )

    def _ensure_budget_day_row(self, db, *, budget_date: date) -> AIBudgetDay:
        day = db.get(AIBudgetDay, budget_date)
        limit_usd = _round_cost(self.settings.ai_daily_cost_limit_usd)
        if day is not None:
            if _round_cost(day.limit_usd) != limit_usd:
                day.limit_usd = limit_usd
                day.updated_at = datetime.now(UTC)
                db.add(day)
                db.flush()
            return day

        spent_usd = self._seed_spent_from_history(db, budget_date=budget_date)
        day = AIBudgetDay(
            budget_date=budget_date,
            spent_usd=spent_usd,
            reserved_usd=0.0,
            limit_usd=limit_usd,
        )
        db.add(day)
        db.flush()
        return day

    def _retryable_sqlite_lock_error(
        self,
        exc: OperationalError,
        *,
        attempt: int,
    ) -> bool:
        if attempt + 1 >= _RESERVATION_RETRY_ATTEMPTS:
            return False
        if not self.settings.database_url.startswith("sqlite"):
            return False

        error_message = str(getattr(exc, "orig", exc)).lower()
        if not any(message in error_message for message in _SQLITE_LOCK_MESSAGES):
            return False

        logger.warning(
            "ai_budget.lock_retry",
            extra={
                "attempt": attempt + 1,
                "max_attempts": _RESERVATION_RETRY_ATTEMPTS,
                "error": error_message,
            },
        )
        sleep(0.05 * (attempt + 1))
        return True

    def _seed_spent_from_history(self, db, *, budget_date: date) -> float:
        window_start_utc, window_end_utc = self._budget_day_window_utc(budget_date)
        runs = list(
            db.scalars(
                select(IngestionRun).where(
                    IngestionRun.started_at >= window_start_utc,
                    IngestionRun.started_at < window_end_utc,
                    IngestionRun.status.in_([RunStatus.SUCCEEDED, RunStatus.FAILED]),
                )
            ).all()
        )
        return _round_cost(
            sum(self._run_total_cost_usd(run.metadata_json) for run in runs)
        )

    def _release_stale_reservations(self, db, *, day: AIBudgetDay, budget_date: date) -> float:
        cutoff = datetime.now(UTC) - timedelta(
            minutes=self.settings.ai_budget_reservation_ttl_minutes
        )
        stale_reservations = list(
            db.scalars(
                select(AIBudgetReservation).where(
                    AIBudgetReservation.budget_date == budget_date,
                    AIBudgetReservation.state == _ACTIVE_RESERVATION_STATE,
                    AIBudgetReservation.created_at < cutoff,
                )
            ).all()
        )
        if not stale_reservations:
            return 0.0

        released_total = _round_cost(
            sum(reservation.estimated_cost_usd for reservation in stale_reservations)
        )
        for reservation in stale_reservations:
            reservation.state = _RELEASED_RESERVATION_STATE
            reservation.actual_cost_usd = 0.0
            reservation.finalized_at = datetime.now(UTC)
            db.add(reservation)

        day.reserved_usd = _round_cost(max(day.reserved_usd - released_total, 0.0))
        day.updated_at = datetime.now(UTC)
        db.add(day)
        logger.warning(
            "ai_budget.released_stale_reservations",
            extra={
                "budget_date": budget_date.isoformat(),
                "released_cost_usd": released_total,
                "reservation_count": len(stale_reservations),
            },
        )
        return released_total

    def _budget_day_window_utc(self, budget_date: date) -> tuple[datetime, datetime]:
        local_start = datetime.combine(budget_date, time.min, tzinfo=self._timezone)
        local_end = local_start + timedelta(days=1)
        return local_start.astimezone(UTC), local_end.astimezone(UTC)

    def _run_total_cost_usd(self, metadata: Any) -> float:
        if not isinstance(metadata, dict):
            return 0.0
        value = metadata.get("total_cost_usd")
        try:
            return _round_cost(float(value))
        except (TypeError, ValueError):
            return 0.0

    def _budget_exceeded_error(
        self,
        *,
        budget_date: date,
        provider: str,
        operation: str,
        current_spent: float,
        current_reserved: float,
        requested_cost: float,
        limit_usd: float,
    ) -> AIBudgetExceededError:
        remaining_usd = _round_cost(max(limit_usd - current_spent - current_reserved, 0.0))
        logger.warning(
            "ai_budget.limit_reached",
            extra={
                "budget_date": budget_date.isoformat(),
                "provider": provider,
                "operation": operation,
                "spent_usd": current_spent,
                "reserved_usd": current_reserved,
                "requested_cost_usd": requested_cost,
                "remaining_usd": remaining_usd,
                "limit_usd": limit_usd,
            },
        )
        return AIBudgetExceededError(
            "Daily AI budget reached. "
            f"Requested ${requested_cost:.6f}, remaining ${remaining_usd:.6f}, "
            f"daily limit ${limit_usd:.2f} for {budget_date.isoformat()}."
        )
