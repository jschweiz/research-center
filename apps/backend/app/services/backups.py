from __future__ import annotations

import gzip
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.base import Base

BACKUP_FILE_PREFIX = "research-center-db-backup-"
BACKUP_FILE_SUFFIX = ".json.gz"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatabaseBackupResult:
    path: Path
    created_at: datetime
    alembic_version: str | None
    table_count: int
    row_count: int
    size_bytes: int
    sha256: str
    pruned_files: list[str]


class _HashingWriter:
    def __init__(self, target) -> None:
        self.target = target
        self._hasher = hashlib.sha256()

    def write(self, data: bytes) -> int:
        self._hasher.update(data)
        return self.target.write(data)

    def flush(self) -> None:
        self.target.flush()

    def hexdigest(self) -> str:
        return self._hasher.hexdigest()


class DatabaseBackupService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.settings = get_settings()

    def create_backup(self) -> DatabaseBackupResult:
        backup_dir = self.settings.database_backup_dir
        backup_dir.mkdir(parents=True, exist_ok=True)

        created_at = datetime.now(UTC)
        timestamp = created_at.strftime("%Y%m%dT%H%M%S%fZ")
        final_path = backup_dir / f"{BACKUP_FILE_PREFIX}{timestamp}{BACKUP_FILE_SUFFIX}"
        temp_path = final_path.with_suffix(final_path.suffix + ".tmp")

        payload, table_count, row_count, alembic_version = self._build_payload(created_at)

        try:
            encoded_payload = json.dumps(
                payload,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            with temp_path.open("wb") as raw_file:
                hashing_writer = _HashingWriter(raw_file)
                with gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=hashing_writer,
                    compresslevel=6,
                    mtime=0,
                ) as gzip_file:
                    gzip_file.write(encoded_payload)
                sha256 = hashing_writer.hexdigest()
            temp_path.replace(final_path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            raise

        size_bytes = final_path.stat().st_size
        pruned_files = self._prune_old_backups(backup_dir)
        logger.info(
            "backup.snapshot_created",
            extra={
                "backup_file": final_path.name,
                "backup_size_bytes": size_bytes,
                "backup_table_count": table_count,
                "backup_row_count": row_count,
                "backup_pruned_count": len(pruned_files),
            },
        )
        return DatabaseBackupResult(
            path=final_path,
            created_at=created_at,
            alembic_version=alembic_version,
            table_count=table_count,
            row_count=row_count,
            size_bytes=size_bytes,
            sha256=sha256,
            pruned_files=pruned_files,
        )

    def list_backup_paths(self) -> list[Path]:
        backup_dir = self.settings.database_backup_dir
        if not backup_dir.exists():
            return []
        return sorted(
            backup_dir.glob(f"{BACKUP_FILE_PREFIX}*{BACKUP_FILE_SUFFIX}"),
            reverse=True,
        )

    def _build_payload(
        self,
        created_at: datetime,
    ) -> tuple[dict[str, Any], int, int, str | None]:
        tables = []
        total_rows = 0
        for table in sorted(Base.metadata.tables.values(), key=lambda item: item.name):
            rows = self._dump_table(table)
            tables.append(
                {
                    "name": table.name,
                    "row_count": len(rows),
                    "rows": rows,
                }
            )
            total_rows += len(rows)

        alembic_version = self._read_alembic_version()
        payload = {
            "format": "research_center_database_backup_v1",
            "created_at": created_at.isoformat(),
            "app_env": self.settings.app_env,
            "database_dialect": self.db.bind.dialect.name if self.db.bind else "unknown",
            "alembic_version": alembic_version,
            "table_count": len(tables),
            "row_count": total_rows,
            "tables": tables,
        }
        return payload, len(tables), total_rows, alembic_version

    def _dump_table(self, table) -> list[dict[str, Any]]:
        statement = select(table)
        primary_key_columns = list(table.primary_key.columns)
        if primary_key_columns:
            statement = statement.order_by(*primary_key_columns)
        rows = self.db.execute(statement).mappings().all()
        return [
            {
                column_name: self._serialize_value(value)
                for column_name, value in row.items()
            }
            for row in rows
        ]

    def _serialize_value(self, value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, datetime):
            normalized = value if value.tzinfo else value.replace(tzinfo=UTC)
            return normalized.astimezone(UTC).isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, time):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, dict):
            return {
                str(key): self._serialize_value(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple, set)):
            return [self._serialize_value(item) for item in value]
        return str(value)

    def _read_alembic_version(self) -> str | None:
        try:
            statement = text("SELECT version_num FROM alembic_version")
            return self.db.execute(statement).scalar_one_or_none()
        except Exception:
            return None

    def _prune_old_backups(self, backup_dir: Path) -> list[str]:
        existing_backups = sorted(
            backup_dir.glob(f"{BACKUP_FILE_PREFIX}*{BACKUP_FILE_SUFFIX}"),
            reverse=True,
        )
        pruned_files: list[str] = []
        for stale_path in existing_backups[self.settings.database_backup_retention_count :]:
            stale_path.unlink(missing_ok=True)
            pruned_files.append(stale_path.name)
        return pruned_files
