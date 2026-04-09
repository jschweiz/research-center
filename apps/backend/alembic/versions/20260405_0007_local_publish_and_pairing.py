"""add local publishing and pairing tables

Revision ID: 20260405_0007
Revises: 20260328_0006
Create Date: 2026-04-05 12:00:00.000000
"""

from sqlalchemy import inspect

from alembic import op
from app.db.models import LocalPairingCode, PairedDevice, PublishedEdition

revision = "20260405_0007"
down_revision = "20260328_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    table_names = set(inspector.get_table_names())

    if "published_editions" not in table_names:
        PublishedEdition.__table__.create(bind, checkfirst=True)
    if "local_pairing_codes" not in table_names:
        LocalPairingCode.__table__.create(bind, checkfirst=True)
    if "paired_devices" not in table_names:
        PairedDevice.__table__.create(bind, checkfirst=True)


def downgrade() -> None:
    raise NotImplementedError("Downgrading the local publishing/pairing migration is not supported.")
