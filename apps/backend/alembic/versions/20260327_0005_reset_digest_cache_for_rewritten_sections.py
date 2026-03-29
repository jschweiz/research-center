"""reset digest cache for rewritten brief sections

Revision ID: 20260327_0005
Revises: 20260327_0004
Create Date: 2026-03-27 20:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

from app.db.models import Digest, DigestEntry, DigestSection


revision = "20260327_0005"
down_revision = "20260327_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    table_names = set(inspector.get_table_names())

    if "digest_entries" in table_names:
        op.drop_table("digest_entries")
    if "digests" in table_names:
        op.drop_table("digests")

    if bind.dialect.name == "postgresql":
        digest_section_enum = sa.Enum(DigestSection, name="digestsection")
        digest_section_enum.drop(bind, checkfirst=True)
        digest_section_enum.create(bind, checkfirst=True)

    Digest.__table__.create(bind, checkfirst=True)
    DigestEntry.__table__.create(bind, checkfirst=True)


def downgrade() -> None:
    raise NotImplementedError("Downgrading the digest cache reset migration is not supported.")
