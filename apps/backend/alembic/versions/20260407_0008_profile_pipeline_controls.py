"""add profile pipeline control fields

Revision ID: 20260407_0008
Revises: 20260405_0007
Create Date: 2026-04-07 13:45:00.000000
"""

from sqlalchemy import inspect
import sqlalchemy as sa

from alembic import op

revision = "20260407_0008"
down_revision = "20260405_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    table_names = set(inspector.get_table_names())

    if "profile_settings" not in table_names:
        return

    columns = {column["name"] for column in inspector.get_columns("profile_settings")}
    additions = {
        "ranking_thresholds": sa.Column(
            "ranking_thresholds",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        "brief_sections": sa.Column(
            "brief_sections",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        "audio_brief_settings": sa.Column(
            "audio_brief_settings",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        "prompt_guidance": sa.Column(
            "prompt_guidance",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
    }

    for name, column in additions.items():
        if name not in columns:
            op.add_column("profile_settings", column)


def downgrade() -> None:
    raise NotImplementedError("Downgrading the profile pipeline controls migration is not supported.")
