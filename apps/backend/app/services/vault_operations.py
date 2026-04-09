from __future__ import annotations

from datetime import date

from app.db.models import IngestionRunType, RunStatus
from app.schemas.advanced_enrichment import AdvancedOutputKind, HealthCheckScope
from app.schemas.briefs import AudioBriefRead
from app.schemas.ops import IngestionRunHistoryRead, OperationBasicInfoRead, PipelineStatusRead
from app.schemas.published import PublishedEditionSummaryRead
from app.services.vault_advanced_enrichment import VaultAdvancedEnrichmentService
from app.services.vault_briefs import VaultBriefService
from app.services.vault_git_sync import VaultGitStatus, VaultGitSyncService
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_lightweight_enrichment import VaultLightweightEnrichmentService
from app.services.vault_publishing import VaultPublisherService
from app.services.vault_runtime import RunRecorder
from app.services.vault_sources import VaultSourceIngestionService
from app.services.vault_wiki import VaultWikiService
from app.vault.store import VaultStore


class VaultOperationService:
    def __init__(self) -> None:
        self.store = VaultStore()
        self.source_ingestion = VaultSourceIngestionService()
        self.lightweight = VaultLightweightEnrichmentService()
        self.ingestion = VaultIngestionService()
        self.advanced = VaultAdvancedEnrichmentService()
        self.briefs = VaultBriefService()
        self.publisher = VaultPublisherService()
        self.sync = VaultGitSyncService()
        self.wiki = VaultWikiService()
        self.runs = RunRecorder(self.store)
        self.store.ensure_layout()

    def pipeline_status(self) -> PipelineStatusRead:
        raw_documents = self.store.list_raw_documents()
        return PipelineStatusRead(
            raw_document_count=len(raw_documents),
            lightweight_pending_count=self.lightweight.count_pending_documents(
                documents=raw_documents
            ),
            items_index=self.ingestion.items_index_status(documents=raw_documents),
        )

    def run_ingest_pipeline(self) -> str | None:
        self.sync.prepare_for_mutation()
        self.source_ingestion.sync_enabled_sources(trigger="default_ingest_pipeline")
        self.lightweight.enrich_stale_documents(trigger="default_ingest_pipeline")
        self.ingestion.rebuild_items_index(trigger="default_ingest_pipeline")
        self.sync.push_local_control_changes(message="Run staged ingest pipeline")
        return self.latest_run_id(["raw_fetch", "lightweight_enrichment", "vault_index"])

    def regenerate_brief(self, *, brief_date: date | None = None) -> PublishedEditionSummaryRead:
        target_date = brief_date or self.briefs.current_edition_date()
        self.sync.prepare_for_mutation()
        self.briefs.generate_digest(target_date, force=True, trigger="manual_digest")
        published = self.publisher.publish_date(target_date)
        self.sync.push_local_control_changes(
            message=f"Regenerate brief for {target_date.isoformat()}"
        )
        return published

    def generate_audio(
        self, *, brief_date: date | None = None
    ) -> tuple[AudioBriefRead | None, PublishedEditionSummaryRead]:
        target_date = brief_date or self.briefs.current_edition_date()
        self.sync.prepare_for_mutation()
        audio = self.briefs.generate_audio_brief(target_date)
        publication = self.publisher.publish_date(target_date)
        self.sync.push_local_control_changes(
            message=f"Generate audio for {target_date.isoformat()}"
        )
        return audio, publication

    def publish(self, *, brief_date: date | None = None) -> PublishedEditionSummaryRead:
        target_date = brief_date or self.briefs.current_edition_date()
        self.sync.prepare_for_mutation()
        publication = self.publisher.publish_date(target_date)
        self.sync.push_local_control_changes(
            message=f"Publish viewer artifacts for {target_date.isoformat()}"
        )
        return publication

    def synchronize(self) -> VaultGitStatus:
        return self.sync.synchronize(message="Synchronize vault")

    def synchronize_local_control(self) -> VaultGitStatus:
        run = self.runs.start(
            run_type=IngestionRunType.INGEST,
            operation_kind="vault_sync",
            trigger="manual_local_control_sync",
            title="Sync vault",
            summary="Synchronizing local-control artifacts with GitHub.",
        )
        run.basic_info.extend(
            [
                OperationBasicInfoRead(label="Scope", value="local-control artifacts"),
                OperationBasicInfoRead(label="Paths", value="raw/**, briefs/daily/**, outputs/viewer/**"),
            ]
        )
        self.runs.log(run, "Starting scoped Git sync for local-control artifacts.")
        try:
            status = self.sync.synchronize_local_control(message="Synchronize local-control outputs")
        except Exception as exc:
            run.errors.append(str(exc))
            self.runs.log(run, str(exc), level="error")
            self.runs.finish(
                run,
                status=RunStatus.FAILED,
                summary="Scoped Git sync failed for local-control artifacts.",
            )
            raise
        self.runs.log(run, "Completed scoped Git sync for local-control artifacts.")
        self.runs.finish(
            run,
            status=RunStatus.SUCCEEDED,
            summary="Scoped Git sync completed for local-control artifacts.",
        )
        return status

    def fetch_sources(self) -> str | None:
        self.sync.prepare_for_mutation()
        self.source_ingestion.sync_enabled_sources(trigger="manual_fetch")
        self.sync.push_local_control_changes(message="Fetch sources")
        return self.latest_run_id("raw_fetch")

    def sync_sources(self) -> str | None:
        return self.fetch_sources()

    def lightweight_enrich(
        self, *, source_id: str | None = None, doc_id: str | None = None, force: bool = False
    ) -> IngestionRunHistoryRead:
        self.sync.prepare_for_mutation()
        run = self.lightweight.enrich_stale_documents(
            trigger="manual_lightweight_enrich",
            source_id=source_id,
            doc_id=doc_id,
            force=force,
        )
        self.sync.push_local_control_changes(message="Run lightweight enrichment")
        return run

    def request_stop_for_lightweight(self) -> IngestionRunHistoryRead:
        return self.lightweight.request_stop_for_run()

    def run_source_pipeline(self, *, source_id: str, max_items: int | None = None) -> str | None:
        self.sync.prepare_for_mutation()
        fetch_run = self.source_ingestion.sync_source_by_id(
            source_id,
            trigger="manual_source_fetch",
            max_items=max_items,
        )
        self.lightweight.enrich_stale_documents(
            trigger=f"manual_source_enrich:{source_id}",
            source_id=source_id,
        )
        self.ingestion.rebuild_items_index(trigger=f"manual_source_index:{source_id}")
        self.wiki.compile(trigger=f"manual_source_wiki:{source_id}")
        self.sync.push_local_changes(message=f"Run source pipeline for {source_id}")
        return fetch_run.id

    def request_stop_for_source(self, *, source_id: str) -> IngestionRunHistoryRead:
        return self.source_ingestion.request_stop_for_source(source_id)

    def rebuild_index(self) -> str | None:
        self.sync.prepare_for_mutation()
        self.ingestion.rebuild_items_index(trigger="manual_index")
        self.sync.push_local_control_changes(message="Rebuild local DB indexes")
        return self.latest_run_id("vault_index")

    def compile_wiki(self) -> str | None:
        return self.run_advanced_compile().id

    def run_advanced_compile(
        self,
        *,
        source_id: str | None = None,
        doc_id: str | None = None,
        limit: int | None = None,
    ) -> IngestionRunHistoryRead:
        self.sync.prepare_for_mutation()
        run = self.advanced.run_compile(
            source_id=source_id,
            doc_id=doc_id,
            limit=limit,
            trigger="manual_advanced_compile",
        )
        self.sync.push_local_changes(message="Compile wiki with Codex")
        return run

    def run_health_check(
        self,
        *,
        scope: HealthCheckScope = "vault",
        topic: str | None = None,
    ) -> IngestionRunHistoryRead:
        self.sync.prepare_for_mutation()
        run = self.advanced.run_health_check(
            scope=scope, topic=topic, trigger="manual_health_check"
        )
        self.sync.push_local_changes(message=f"Run health check ({scope})")
        return run

    def run_answer_query(
        self,
        *,
        question: str,
        output_kind: AdvancedOutputKind = "answer",
    ) -> IngestionRunHistoryRead:
        self.sync.prepare_for_mutation()
        run = self.advanced.run_answer_query(
            question=question,
            output_kind=output_kind,
            trigger="manual_answer_query",
        )
        self.sync.push_local_changes(message=f"Answer query into {output_kind}")
        return run

    def run_file_output(self, *, path: str) -> IngestionRunHistoryRead:
        self.sync.prepare_for_mutation()
        run = self.advanced.run_file_output(path=path, trigger="manual_file_output")
        self.sync.push_local_changes(message=f"File output into wiki from {path}")
        return run

    def generate_audio_only(self, *, brief_date: date | None = None) -> str | None:
        target_date = brief_date or self.briefs.current_edition_date()
        self.sync.prepare_for_mutation()
        self.briefs.generate_audio_brief(target_date)
        self.sync.push_local_control_changes(
            message=f"Generate audio for {target_date.isoformat()}"
        )
        return self.latest_run_id("audio_generation")

    def publish_latest(self, *, brief_date: date | None = None) -> str | None:
        target_date = brief_date or self.briefs.current_edition_date()
        self.sync.prepare_for_mutation()
        self.publisher.publish_date(target_date)
        self.sync.push_local_control_changes(
            message=f"Publish viewer artifacts for {target_date.isoformat()}"
        )
        return self.latest_run_id("viewer_publish")

    def run_deep_enrichment_placeholder(self) -> IngestionRunHistoryRead:
        return self.run_advanced_compile()

    def latest_run_id(self, operation_kind: str | list[str]) -> str | None:
        operation_kinds = [operation_kind] if isinstance(operation_kind, str) else operation_kind
        for run in self.ingestion.list_recent_runs(limit=40):
            if run.operation_kind in operation_kinds:
                return run.id
        return None
