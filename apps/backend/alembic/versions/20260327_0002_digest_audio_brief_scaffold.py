"""add digest audio brief scaffold

Revision ID: 20260327_0002
Revises: 20260326_0001
Create Date: 2026-03-27 11:30:00.000000
"""

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op

revision = "20260327_0002"
down_revision = "20260326_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    digest_columns = {column["name"] for column in inspector.get_columns("digests")}

    additions = [
        ("audio_brief_status", sa.Column("audio_brief_status", sa.String(length=50), nullable=True)),
        ("audio_brief_script", sa.Column("audio_brief_script", sa.Text(), nullable=True)),
        (
            "audio_brief_chapters",
            sa.Column(
                "audio_brief_chapters",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'[]'"),
            ),
        ),
        ("audio_brief_error", sa.Column("audio_brief_error", sa.Text(), nullable=True)),
        (
            "audio_brief_generated_at",
            sa.Column("audio_brief_generated_at", sa.DateTime(timezone=True), nullable=True),
        ),
        ("audio_artifact_url", sa.Column("audio_artifact_url", sa.String(length=2000), nullable=True)),
        (
            "audio_artifact_provider",
            sa.Column("audio_artifact_provider", sa.String(length=100), nullable=True),
        ),
        ("audio_artifact_voice", sa.Column("audio_artifact_voice", sa.String(length=100), nullable=True)),
        ("audio_duration_seconds", sa.Column("audio_duration_seconds", sa.Integer(), nullable=True)),
        (
            "audio_metadata_json",
            sa.Column(
                "audio_metadata_json",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'{}'"),
            ),
        ),
    ]

    for column_name, column in additions:
        if column_name not in digest_columns:
            op.add_column("digests", column)


def downgrade() -> None:
    op.drop_column("digests", "audio_metadata_json")
    op.drop_column("digests", "audio_duration_seconds")
    op.drop_column("digests", "audio_artifact_voice")
    op.drop_column("digests", "audio_artifact_provider")
    op.drop_column("digests", "audio_artifact_url")
    op.drop_column("digests", "audio_brief_generated_at")
    op.drop_column("digests", "audio_brief_error")
    op.drop_column("digests", "audio_brief_chapters")
    op.drop_column("digests", "audio_brief_script")
    op.drop_column("digests", "audio_brief_status")
