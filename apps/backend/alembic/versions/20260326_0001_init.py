"""initial schema

Revision ID: 20260326_0001
Revises:
Create Date: 2026-03-26 10:00:00.000000
"""

from alembic import op
from sqlalchemy import text

from app.db.base import Base
from app.db.models import *  # noqa: F403


revision = "20260326_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        bind.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
