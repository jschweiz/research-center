from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

from app.core.config import get_settings
from app.vault.models import LocalBudgetDayState, LocalBudgetReservationState
from app.vault.store import VaultStore

logger = logging.getLogger(__name__)

_ACTIVE_RESERVATION_STATE = "active"
_CONSUMED_RESERVATION_STATE = "consumed"
_RELEASED_RESERVATION_STATE = "released"
_COST_PRECISION = 6


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
        self.store = VaultStore()
        self.store.ensure_layout()

    def current_budget_date(self) -> date:
        return datetime.now(UTC).astimezone(self._timezone).date()

    def reserve_estimated_cost(
        self,
        *,
        provider: str,
        operation: str,
        estimated_cost_usd: float,
        metadata: dict | None = None,
    ) -> AIBudgetReservationHandle | None:
        normalized_cost = _round_cost(estimated_cost_usd)
        if normalized_cost <= 0:
            return None

        state = self.store.load_ai_budget()
        budget_date = self.current_budget_date()
        self._release_stale_reservations(state, budget_date=budget_date)
        day = self._ensure_budget_day(state, budget_date=budget_date)
        projected_total = _round_cost(day.spent_usd + day.reserved_usd + normalized_cost)
        if projected_total > day.limit_usd:
            raise AIBudgetExceededError(
                "AI daily budget exceeded. "
                f"Requested ${normalized_cost:.6f} for {provider}/{operation} with "
                f"${day.spent_usd:.6f} spent and ${day.reserved_usd:.6f} reserved "
                f"against a ${day.limit_usd:.6f} limit."
            )

        day.reserved_usd = _round_cost(day.reserved_usd + normalized_cost)
        day.updated_at = datetime.now(UTC)
        reservation = LocalBudgetReservationState(
            id=str(uuid4()),
            budget_date=budget_date,
            provider=provider,
            operation=operation,
            state=_ACTIVE_RESERVATION_STATE,
            estimated_cost_usd=normalized_cost,
            actual_cost_usd=None,
            metadata_json=dict(metadata or {}),
            created_at=datetime.now(UTC),
            finalized_at=None,
        )
        state.reservations.append(reservation)
        self.store.save_ai_budget(state)
        logger.info(
            "ai_budget.reserved",
            extra={
                "budget_date": budget_date.isoformat(),
                "provider": provider,
                "operation": operation,
                "estimated_cost_usd": normalized_cost,
            },
        )
        return AIBudgetReservationHandle(
            reservation_id=reservation.id,
            budget_date=budget_date,
            provider=provider,
            operation=operation,
            estimated_cost_usd=normalized_cost,
        )

    def consume_reservation(
        self,
        handle: AIBudgetReservationHandle | None,
        *,
        actual_cost_usd: float,
    ) -> None:
        if handle is None:
            return
        state = self.store.load_ai_budget()
        reservation = next(
            (
                current
                for current in state.reservations
                if current.id == handle.reservation_id and current.state == _ACTIVE_RESERVATION_STATE
            ),
            None,
        )
        if reservation is None:
            return
        day = self._ensure_budget_day(state, budget_date=reservation.budget_date)
        normalized_actual = _round_cost(actual_cost_usd)
        day.reserved_usd = _round_cost(max(day.reserved_usd - reservation.estimated_cost_usd, 0.0))
        day.spent_usd = _round_cost(day.spent_usd + normalized_actual)
        day.updated_at = datetime.now(UTC)
        reservation.state = _CONSUMED_RESERVATION_STATE
        reservation.actual_cost_usd = normalized_actual
        reservation.finalized_at = datetime.now(UTC)
        self.store.save_ai_budget(state)

    def release_reservation(self, handle: AIBudgetReservationHandle | None) -> None:
        if handle is None:
            return
        state = self.store.load_ai_budget()
        reservation = next(
            (
                current
                for current in state.reservations
                if current.id == handle.reservation_id and current.state == _ACTIVE_RESERVATION_STATE
            ),
            None,
        )
        if reservation is None:
            return
        day = self._ensure_budget_day(state, budget_date=reservation.budget_date)
        day.reserved_usd = _round_cost(max(day.reserved_usd - reservation.estimated_cost_usd, 0.0))
        day.updated_at = datetime.now(UTC)
        reservation.state = _RELEASED_RESERVATION_STATE
        reservation.actual_cost_usd = 0.0
        reservation.finalized_at = datetime.now(UTC)
        self.store.save_ai_budget(state)

    def _ensure_budget_day(self, state, *, budget_date: date) -> LocalBudgetDayState:
        day = next((entry for entry in state.days if entry.budget_date == budget_date), None)
        if day is not None:
            day.limit_usd = _round_cost(self.settings.ai_daily_cost_limit_usd)
            return day
        day = LocalBudgetDayState(
            budget_date=budget_date,
            spent_usd=0.0,
            reserved_usd=0.0,
            limit_usd=_round_cost(self.settings.ai_daily_cost_limit_usd),
            updated_at=datetime.now(UTC),
        )
        state.days.append(day)
        return day

    def _release_stale_reservations(self, state, *, budget_date: date) -> None:
        ttl = timedelta(minutes=self.settings.ai_budget_reservation_ttl_minutes)
        now = datetime.now(UTC)
        for reservation in state.reservations:
            if reservation.budget_date != budget_date or reservation.state != _ACTIVE_RESERVATION_STATE:
                continue
            created_at = reservation.created_at if reservation.created_at.tzinfo else reservation.created_at.replace(tzinfo=UTC)
            if created_at + ttl > now:
                continue
            day = self._ensure_budget_day(state, budget_date=reservation.budget_date)
            day.reserved_usd = _round_cost(max(day.reserved_usd - reservation.estimated_cost_usd, 0.0))
            day.updated_at = now
            reservation.state = _RELEASED_RESERVATION_STATE
            reservation.actual_cost_usd = 0.0
            reservation.finalized_at = now
