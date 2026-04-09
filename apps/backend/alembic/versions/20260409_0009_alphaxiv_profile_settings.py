"""add alphaxiv profile settings

Revision ID: 20260409_0009
Revises: 20260407_0008
Create Date: 2026-04-09 11:10:00.000000
"""

from sqlalchemy import inspect
import sqlalchemy as sa

from alembic import op

revision = "20260409_0009"
down_revision = "20260407_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    table_names = set(inspector.get_table_names())

    if "profile_settings" not in table_names:
        return

    columns = {column["name"] for column in inspector.get_columns("profile_settings")}
    if "alphaxiv_search_settings" not in columns:
        op.add_column(
            "profile_settings",
            sa.Column(
                "alphaxiv_search_settings",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'{}'"),
            ),
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Downgrading the alphaXiv profile settings migration is not supported."
    )
