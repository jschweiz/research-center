"""add ai budget tables

Revision ID: 20260328_0006
Revises: 20260327_0005
Create Date: 2026-03-28 01:45:00.000000
"""

from sqlalchemy import inspect

from alembic import op
from app.db.models import AIBudgetDay, AIBudgetReservation

revision = "20260328_0006"
down_revision = "20260327_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    table_names = set(inspector.get_table_names())

    if "ai_budget_days" not in table_names:
        AIBudgetDay.__table__.create(bind, checkfirst=True)
    if "ai_budget_reservations" not in table_names:
        AIBudgetReservation.__table__.create(bind, checkfirst=True)


def downgrade() -> None:
    raise NotImplementedError("Downgrading the AI budget cap migration is not supported.")
