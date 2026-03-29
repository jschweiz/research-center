"""add digest audio brief scaffold

Revision ID: 20260327_0002
Revises: 20260326_0001
Create Date: 2026-03-27 11:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260327_0002"
down_revision = "20260326_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("digests", sa.Column("audio_brief_status", sa.String(length=50), nullable=True))
    op.add_column("digests", sa.Column("audio_brief_script", sa.Text(), nullable=True))
    op.add_column(
        "digests",
        sa.Column(
            "audio_brief_chapters",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'"),
        ),
    )
    op.add_column("digests", sa.Column("audio_brief_error", sa.Text(), nullable=True))
    op.add_column("digests", sa.Column("audio_brief_generated_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("digests", sa.Column("audio_artifact_url", sa.String(length=2000), nullable=True))
    op.add_column("digests", sa.Column("audio_artifact_provider", sa.String(length=100), nullable=True))
    op.add_column("digests", sa.Column("audio_artifact_voice", sa.String(length=100), nullable=True))
    op.add_column("digests", sa.Column("audio_duration_seconds", sa.Integer(), nullable=True))
    op.add_column(
        "digests",
        sa.Column(
            "audio_metadata_json",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
    )


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
