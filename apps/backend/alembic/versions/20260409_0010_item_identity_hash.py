"""add item identity hashes

Revision ID: 20260409_0010
Revises: 20260409_0009
Create Date: 2026-04-09 11:15:00.000000
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from urllib.parse import urlparse

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op

revision = "20260409_0010"
down_revision = "20260409_0009"
branch_labels = None
depends_on = None

ARXIV_ABS_PATH_RE = re.compile(
    r"^/(?:abs|pdf)/(?P<identifier>\d{4}\.\d{4,5})(?:v\d+)?(?:\.pdf)?$",
    re.IGNORECASE,
)


def _canonicalize_identity(value: object) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    return " ".join(normalized.split())


def _identity_hash(*parts: object) -> str:
    digest = hashlib.sha256()
    candidate_parts = parts or ("",)
    for part in candidate_parts:
        digest.update(b"\x1f")
        digest.update(_canonicalize_identity(part).encode("utf-8"))
    return digest.hexdigest()


def _document_identity_hash(*, source_id: str | None, document_key: str | None, fallback_key: str) -> str:
    return _identity_hash(
        source_id or "unknown-source",
        document_key or fallback_key or "undocumented",
    )


def _identity_value(*values: object) -> str | None:
    for value in values:
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None


def _metadata_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _arxiv_identity_key(canonical_url: str | None) -> str | None:
    path = urlparse(str(canonical_url or "")).path or ""
    match = ARXIV_ABS_PATH_RE.match(path)
    if not match:
        return None
    return f"arxiv:{match.group('identifier')}"


def _identity_key(*, canonical_url: str | None, metadata_json: object) -> str | None:
    metadata = _metadata_dict(metadata_json)

    newsletter_message_id = _identity_value(metadata.get("newsletter_message_id"))
    if newsletter_message_id:
        newsletter_fact_index = _identity_value(metadata.get("newsletter_fact_index"), "1")
        return f"gmail:{newsletter_message_id}:{newsletter_fact_index}"

    feed_entry_id = _identity_value(metadata.get("feed_entry_id"))
    if feed_entry_id:
        return f"feed:{feed_entry_id}"

    doi = _identity_value(metadata.get("doi"))
    if doi:
        return f"doi:{doi}"

    arxiv_identity = _arxiv_identity_key(canonical_url)
    if arxiv_identity:
        return arxiv_identity

    return _identity_value(canonical_url)


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    table_names = set(inspector.get_table_names())

    if "items" not in table_names:
        return

    columns = {column["name"] for column in inspector.get_columns("items")}
    if "identity_hash" not in columns:
        op.add_column("items", sa.Column("identity_hash", sa.String(length=64), nullable=True))

    inspector = inspect(bind)
    indexes = {index["name"] for index in inspector.get_indexes("items")}
    if "ix_items_identity_hash" not in indexes:
        op.create_index("ix_items_identity_hash", "items", ["identity_hash"], unique=False)

    items_table = sa.table(
        "items",
        sa.column("id", sa.String()),
        sa.column("source_id", sa.String()),
        sa.column("canonical_url", sa.String()),
        sa.column("metadata_json", sa.JSON()),
        sa.column("identity_hash", sa.String()),
    )
    rows = bind.execute(
        sa.select(
            items_table.c.id,
            items_table.c.source_id,
            items_table.c.canonical_url,
            items_table.c.metadata_json,
            items_table.c.identity_hash,
        )
    ).mappings().all()

    for row in rows:
        if row["identity_hash"]:
            continue
        document_key = _identity_key(
            canonical_url=row["canonical_url"],
            metadata_json=row["metadata_json"],
        )
        bind.execute(
            items_table.update()
            .where(items_table.c.id == row["id"])
            .values(
                identity_hash=_document_identity_hash(
                    source_id=row["source_id"],
                    document_key=document_key,
                    fallback_key=row["id"],
                )
            )
        )


def downgrade() -> None:
    raise NotImplementedError("Downgrading the item identity hash migration is not supported.")
