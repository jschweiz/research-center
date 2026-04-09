"""add data modes for profile and digests

Revision ID: 20260327_0003
Revises: 20260327_0002
Create Date: 2026-03-27 12:00:00.000000
"""

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op
from app.db.models import DataMode, DigestEntry

revision = "20260327_0003"
down_revision = "20260327_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    table_names = set(inspector.get_table_names())

    profile_columns = {column["name"] for column in inspector.get_columns("profile_settings")}
    if "data_mode" not in profile_columns:
        op.add_column(
            "profile_settings",
            sa.Column(
                "data_mode",
                sa.Enum(DataMode),
                nullable=False,
                server_default=DataMode.SEED.name,
            ),
        )

    digest_columns = {column["name"] for column in inspector.get_columns("digests")}
    if "data_mode" not in digest_columns:
        if "digest_entries" in table_names:
            op.drop_table("digest_entries")

        naming_convention = {
            "uq": "uq_%(table_name)s_%(column_0_name)s",
        }
        with op.batch_alter_table("digests", recreate="always", naming_convention=naming_convention) as batch_op:
            batch_op.add_column(
                sa.Column(
                    "data_mode",
                    sa.Enum(DataMode),
                    nullable=False,
                    server_default=DataMode.SEED.name,
                ),
            )
            batch_op.drop_constraint("uq_digests_brief_date", type_="unique")
            batch_op.create_unique_constraint(
                "uq_digests_brief_date_data_mode",
                ["brief_date", "data_mode"],
            )

        op.execute(sa.text("DELETE FROM digests"))
        DigestEntry.__table__.create(bind)


def downgrade() -> None:
    raise NotImplementedError("Downgrading the data mode migration is not supported.")
