from __future__ import annotations

from datetime import date

from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_operations import VaultOperationService


class OperationService:
    def __init__(self, _db=None) -> None:
        self.operations = VaultOperationService()
        self.ingestion = VaultIngestionService()

    def enqueue_ingest(self, background_tasks=None) -> str:
        del background_tasks
        return self.operations.run_ingest_pipeline() or ""

    def enqueue_digest(
        self,
        force: bool = False,
        brief_date: date | None = None,
        background_tasks=None,
    ) -> None:
        del force, background_tasks
        self.operations.regenerate_brief(brief_date=brief_date)

    def list_recent_operations(self, *, limit: int = 20):
        return self.ingestion.list_recent_runs(limit=limit)
