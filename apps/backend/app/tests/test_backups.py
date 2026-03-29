from __future__ import annotations

import gzip
import json

from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.db.models import RunStatus
from app.db.session import get_session_factory
from app.services.backups import DatabaseBackupService
from app.services.ingestion import IngestionService
from app.services.operations import OperationService


def test_backup_service_prunes_old_snapshots(client: TestClient, tmp_path, monkeypatch) -> None:
    backup_dir = tmp_path / "backups"
    monkeypatch.setenv("DATABASE_BACKUP_DIR", str(backup_dir))
    monkeypatch.setenv("DATABASE_BACKUP_RETENTION_COUNT", "2")
    get_settings.cache_clear()

    with get_session_factory()() as db:
        service = DatabaseBackupService(db)
        first = service.create_backup()
        second = service.create_backup()
        third = service.create_backup()

        assert first.path.name in third.pruned_files
        assert len(service.list_backup_paths()) == 2
        assert service.list_backup_paths()[0].name == third.path.name
        assert service.list_backup_paths()[1].name == second.path.name


def test_scheduled_backup_records_scheduled_trigger(client: TestClient) -> None:
    with get_session_factory()() as db:
        operation_run_id = OperationService(db).enqueue_database_backup(
            trigger="scheduled_backup"
        )

    with get_session_factory()() as db:
        latest_run = IngestionService(db).list_recent_ingestion_cycles(limit=1)[0]

    assert latest_run["id"] == operation_run_id
    assert latest_run["operation_kind"] == "database_backup"
    assert latest_run["trigger"] == "scheduled_backup"
    assert latest_run["status"] == RunStatus.SUCCEEDED


def test_backup_now_endpoint_creates_snapshot_and_history_entry(
    authenticated_client: TestClient,
) -> None:
    response = authenticated_client.post("/api/ops/backup-now")

    assert response.status_code == 200
    payload = response.json()
    assert payload["queued"] is True
    assert payload["task_name"] == "database_backup"
    assert payload["operation_run_id"]

    backup_dir = get_settings().database_backup_dir
    backup_files = sorted(backup_dir.glob("research-center-db-backup-*.json.gz"))
    assert len(backup_files) == 1

    with gzip.open(backup_files[0], "rt", encoding="utf-8") as backup_file:
        backup_payload = json.load(backup_file)

    assert backup_payload["format"] == "research_center_database_backup_v1"
    assert backup_payload["database_dialect"] == "sqlite"
    assert backup_payload["table_count"] > 0
    assert any(table["name"] == "ingestion_runs" for table in backup_payload["tables"])

    history = authenticated_client.get("/api/ops/ingestion-runs")
    assert history.status_code == 200
    backup_run = next(
        entry for entry in history.json() if entry["operation_kind"] == "database_backup"
    )
    assert backup_run["status"] == "succeeded"
    assert backup_run["trigger"] == "manual_backup"
    assert backup_run["title"] == "Database backup"
    assert backup_run["summary"].startswith("Created database backup ")
    assert any(info["label"] == "File" for info in backup_run["basic_info"])
