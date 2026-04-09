"""reset cached digests for shifted brief dates

Revision ID: 20260327_0004
Revises: 20260327_0003
Create Date: 2026-03-27 18:15:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "20260327_0004"
down_revision = "20260327_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("DELETE FROM digest_entries"))
    op.execute(sa.text("DELETE FROM digests"))


def downgrade() -> None:
    raise NotImplementedError("Downgrading the shifted-brief reset migration is not supported.")
