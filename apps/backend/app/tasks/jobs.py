from __future__ import annotations

import argparse
import json
import logging
from datetime import date

from app.core.logging import bind_task_context, reset_task_context
from app.core.metrics import track_task_metrics
from app.db.session import get_session_factory
from app.schemas.advanced_enrichment import AdvancedOutputKind, HealthCheckScope
from app.services.ingestion import IngestionService
from app.services.local_control import LocalControlService
from app.services.scheduling import ScheduleService
from app.services.vault_advanced_enrichment import VaultAdvancedEnrichmentService
from app.services.vault_briefs import VaultBriefService
from app.services.vault_export import VaultExporter
from app.services.vault_git_sync import VaultGitSyncService
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_lightweight_enrichment import VaultLightweightEnrichmentService
from app.services.vault_operations import VaultOperationService
from app.services.vault_publishing import VaultPublisherService
from app.services.vault_sources import VaultSourceIngestionService

logger = logging.getLogger(__name__)


def _task_metadata(default_name: str) -> tuple[str | None, str, tuple]:
    task_id = None
    task_name = default_name
    return task_id, task_name, bind_task_context(task_id=task_id, task_name=task_name)


def run_ingest_task(source_id: str | None = None, cycle_run_id: str | None = None) -> int:
    _, task_name, task_tokens = _task_metadata("research_center.run_ingest")
    try:
        with track_task_metrics(task_name):
            del cycle_run_id
            sync = VaultGitSyncService()
            sync.prepare_for_mutation()
            source_service = VaultSourceIngestionService()
            if source_id:
                source_service.sync_source_by_id(source_id, trigger="cli_ingest")
            else:
                source_service.sync_enabled_sources(trigger="cli_ingest")
            VaultLightweightEnrichmentService().enrich_stale_documents(
                trigger="cli_ingest",
                source_id=source_id,
            )
            index = VaultIngestionService().rebuild_items_index(trigger="cli_ingest")
            sync.push_local_control_changes(message="Run staged ingest pipeline")
            return len(index.items)
    finally:
        reset_task_context(task_tokens)


def fetch_sources_task(source_id: str | None = None) -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.fetch_sources")
    try:
        with track_task_metrics(task_name):
            sync = VaultGitSyncService()
            sync.prepare_for_mutation()
            source_service = VaultSourceIngestionService()
            if source_id:
                run = source_service.sync_source_by_id(source_id, trigger="cli_fetch")
                sync.push_local_control_changes(message=f"Fetch source {source_id}")
                return {
                    "source_id": source_id,
                    "operation_run_id": run.id,
                    "status": run.status.value,
                    "synced_documents": run.total_titles,
                    "created_count": run.created_count,
                    "updated_count": run.updated_count,
                }
            result = source_service.sync_enabled_sources(trigger="cli_fetch")
            sync.push_local_control_changes(message="Fetch sources")
            return {
                "source_count": result.source_count,
                "synced_documents": result.synced_document_count,
                "failed_sources": result.failed_source_count,
            }
    finally:
        reset_task_context(task_tokens)


def lightweight_enrich_task(
    *,
    source_id: str | None = None,
    doc_id: str | None = None,
    force: bool = False,
) -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.lightweight_enrich")
    try:
        with track_task_metrics(task_name):
            sync = VaultGitSyncService()
            sync.prepare_for_mutation()
            run = VaultLightweightEnrichmentService().enrich_stale_documents(
                trigger="cli_lightweight_enrich",
                source_id=source_id,
                doc_id=doc_id,
                force=force,
            )
            sync.push_local_control_changes(message="Run lightweight enrichment")
            return {
                "operation_run_id": run.id,
                "status": run.status.value,
                "documents": run.total_titles,
                "updated_count": run.updated_count,
                "failed_count": len(run.errors),
            }
    finally:
        reset_task_context(task_tokens)


def rebuild_items_index_task() -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.rebuild_items_index")
    try:
        with track_task_metrics(task_name):
            sync = VaultGitSyncService()
            sync.prepare_for_mutation()
            index = VaultIngestionService().rebuild_items_index(trigger="cli_index")
            sync.push_local_control_changes(message="Rebuild local DB indexes")
            return {
                "indexed_items": len(index.items),
                "index_path": "sqlite:vault_items,vault_item_fts,vault_topics",
            }
    finally:
        reset_task_context(task_tokens)


def advanced_compile_task(
    *,
    source_id: str | None = None,
    doc_id: str | None = None,
    limit: int | None = None,
) -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.advanced_compile")
    try:
        with track_task_metrics(task_name):
            run = VaultOperationService().run_advanced_compile(
                source_id=source_id,
                doc_id=doc_id,
                limit=limit,
            )
            return {
                "operation_run_id": run.id,
                "status": run.status.value,
                "changed_file_count": run.changed_file_count,
                "output_paths": run.output_paths,
            }
    finally:
        reset_task_context(task_tokens)


def health_check_task(*, scope: HealthCheckScope = "vault", topic: str | None = None) -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.health_check")
    try:
        with track_task_metrics(task_name):
            run = VaultOperationService().run_health_check(scope=scope, topic=topic)
            return {
                "operation_run_id": run.id,
                "status": run.status.value,
                "output_paths": run.output_paths,
                "summary": run.summary,
            }
    finally:
        reset_task_context(task_tokens)


def answer_query_task(*, question: str, output_kind: AdvancedOutputKind = "answer") -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.answer_query")
    try:
        with track_task_metrics(task_name):
            run = VaultOperationService().run_answer_query(question=question, output_kind=output_kind)
            return {
                "operation_run_id": run.id,
                "status": run.status.value,
                "output_paths": run.output_paths,
                "summary": run.summary,
            }
    finally:
        reset_task_context(task_tokens)


def file_output_task(*, path: str) -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.file_output")
    try:
        with track_task_metrics(task_name):
            run = VaultOperationService().run_file_output(path=path)
            return {
                "operation_run_id": run.id,
                "status": run.status.value,
                "changed_file_count": run.changed_file_count,
                "summary": run.summary,
            }
    finally:
        reset_task_context(task_tokens)


def vault_search_task(*, query: str, limit: int = 10) -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.vault_search")
    try:
        with track_task_metrics(task_name):
            return VaultAdvancedEnrichmentService().search_vault(query=query, limit=limit)
    finally:
        reset_task_context(task_tokens)


def vault_show_doc_task(*, doc_id: str) -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.vault_show_doc")
    try:
        with track_task_metrics(task_name):
            return VaultAdvancedEnrichmentService().show_raw_document(doc_id=doc_id)
    finally:
        reset_task_context(task_tokens)


def vault_related_task(*, doc_id: str, limit: int = 10) -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.vault_related")
    try:
        with track_task_metrics(task_name):
            return VaultAdvancedEnrichmentService().related_documents(doc_id=doc_id, limit=limit)
    finally:
        reset_task_context(task_tokens)


def vault_list_stale_task(*, source_id: str | None = None, limit: int | None = None) -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.vault_list_stale")
    try:
        with track_task_metrics(task_name):
            return VaultAdvancedEnrichmentService().list_stale_documents(source_id=source_id, limit=limit)
    finally:
        reset_task_context(task_tokens)


def vault_insights_task(*, query: str | None = None, limit: int = 10) -> dict[str, object]:
    _, task_name, task_tokens = _task_metadata("research_center.vault_insights")
    try:
        with track_task_metrics(task_name):
            return VaultAdvancedEnrichmentService().insight_radar(query=query, limit=limit)
    finally:
        reset_task_context(task_tokens)


def purge_raw_email_payloads_task() -> int:
    _, task_name, task_tokens = _task_metadata("research_center.purge_raw_email_payloads")
    try:
        with track_task_metrics(task_name):
            logger.info("task.raw_email_payload_purge.started")
            with get_session_factory()() as db:
                purged_count = IngestionService(db).purge_old_email_payloads()
            logger.info(
                "task.raw_email_payload_purge.completed",
                extra={"purged_count": purged_count},
            )
            return purged_count
    finally:
        reset_task_context(task_tokens)


def run_digest_task(
    force: bool = False,
    only_if_due: bool = False,
    brief_date: str | None = None,
    trigger: str | None = None,
    editorial_note_mode: str | None = None,
) -> str:
    _, task_name, task_tokens = _task_metadata("research_center.run_digest")
    try:
        with track_task_metrics(task_name) as set_outcome:
            if only_if_due:
                with get_session_factory()() as db:
                    if not ScheduleService(db).is_profile_digest_due():
                        set_outcome("skipped")
                        return "skipped:not_due"
            sync = VaultGitSyncService()
            sync.prepare_for_mutation()
            service = VaultBriefService()
            target_date = date.fromisoformat(brief_date) if brief_date else service.current_edition_date()
            digest = service.generate_digest(
                target_date,
                force=bool(force or brief_date),
                trigger=trigger or "cli_brief",
                editorial_note_mode=editorial_note_mode,
            )
            sync.push_local_control_changes(message=f"Generate brief for {target_date.isoformat()}")
            return digest.id
    finally:
        reset_task_context(task_tokens)


def generate_audio_task(brief_date: str | None = None) -> str:
    _, task_name, task_tokens = _task_metadata("research_center.generate_audio")
    try:
        with track_task_metrics(task_name):
            sync = VaultGitSyncService()
            sync.prepare_for_mutation()
            service = VaultBriefService()
            target_date = date.fromisoformat(brief_date) if brief_date else service.current_edition_date()
            audio = service.generate_audio_brief(target_date)
            if audio is None:
                raise RuntimeError(f"Audio brief generation returned nothing for {target_date.isoformat()}.")
            sync.push_local_control_changes(message=f"Generate audio for {target_date.isoformat()}")
            return audio.status
    finally:
        reset_task_context(task_tokens)


def publish_latest_task() -> dict:
    _, task_name, task_tokens = _task_metadata("research_center.publish_latest")
    try:
        with track_task_metrics(task_name):
            sync = VaultGitSyncService()
            sync.prepare_for_mutation()
            summary = VaultPublisherService().publish_latest()
            sync.push_local_control_changes(message="Publish latest viewer artifacts")
            return summary.model_dump(mode="json")
    finally:
        reset_task_context(task_tokens)


def publish_date_task(brief_date: str) -> dict:
    _, task_name, task_tokens = _task_metadata("research_center.publish_date")
    try:
        with track_task_metrics(task_name):
            sync = VaultGitSyncService()
            sync.prepare_for_mutation()
            summary = VaultPublisherService().publish_date(date.fromisoformat(brief_date))
            sync.push_local_control_changes(message=f"Publish viewer artifacts for {brief_date}")
            return summary.model_dump(mode="json")
    finally:
        reset_task_context(task_tokens)


def sync_vault_task() -> dict:
    _, task_name, task_tokens = _task_metadata("research_center.sync_vault")
    try:
        with track_task_metrics(task_name):
            status = VaultGitSyncService().synchronize_local_control(message="Synchronize local-control outputs")
            return {
                "branch": status.branch,
                "remote_url": status.remote_url,
                "current_commit": status.current_commit,
                "current_summary": status.current_summary,
                "has_uncommitted_changes": status.has_uncommitted_changes,
                "ahead_count": status.ahead_count,
                "behind_count": status.behind_count,
            }
    finally:
        reset_task_context(task_tokens)


def audit_vault_task() -> dict:
    _, task_name, task_tokens = _task_metadata("research_center.audit_vault")
    try:
        with track_task_metrics(task_name):
            return VaultIngestionService().audit_vault()
    finally:
        reset_task_context(task_tokens)


def export_sqlite_to_vault_task() -> dict:
    _, task_name, task_tokens = _task_metadata("research_center.export_sqlite_to_vault")
    try:
        with track_task_metrics(task_name):
            result = VaultExporter().export_from_sqlite()
            return {
                "exported_items": result.exported_items,
                "vault_root": result.vault_root,
            }
    finally:
        reset_task_context(task_tokens)


def main() -> None:
    parser = argparse.ArgumentParser(description="Research Center vault job helper.")
    parser.add_argument(
        "command",
        choices=[
            "audit-vault-inline",
            "advanced-compile-inline",
            "answer-query-inline",
            "compile-wiki-inline",
            "export-sqlite-to-vault-inline",
            "fetch-sources-inline",
            "generate-audio-inline",
            "generate-brief-inline",
            "health-check-inline",
            "file-output-inline",
            "lightweight-enrich-inline",
            "pair-device-code",
            "publish-date-inline",
            "publish-latest-inline",
            "rebuild-items-index-inline",
            "run-digest-inline",
            "run-ingest-inline",
            "sync-vault-inline",
            "vault-insights-inline",
            "vault-list-stale-inline",
            "vault-related-inline",
            "vault-search-inline",
            "vault-show-doc-inline",
        ],
    )
    parser.add_argument("--brief-date", dest="brief_date")
    parser.add_argument("--label", dest="label")
    parser.add_argument("--source-id", dest="source_id")
    parser.add_argument("--doc-id", dest="doc_id")
    parser.add_argument("--limit", dest="limit", type=int)
    parser.add_argument("--query", dest="query")
    parser.add_argument("--question", dest="question")
    parser.add_argument("--output-kind", dest="output_kind", choices=["answer", "slides", "chart"])
    parser.add_argument("--path", dest="path")
    parser.add_argument("--scope", dest="scope", choices=["vault", "wiki", "raw"])
    parser.add_argument("--topic", dest="topic")
    parser.add_argument("--force", dest="force", action="store_true")
    args = parser.parse_args()

    if args.command == "fetch-sources-inline":
        print(json.dumps(fetch_sources_task(source_id=args.source_id), ensure_ascii=True, indent=2))
        return

    if args.command == "lightweight-enrich-inline":
        print(
            json.dumps(
                lightweight_enrich_task(
                    source_id=args.source_id,
                    doc_id=args.doc_id,
                    force=bool(args.force),
                ),
                ensure_ascii=True,
                indent=2,
            )
        )
        return

    if args.command == "rebuild-items-index-inline":
        print(json.dumps(rebuild_items_index_task(), ensure_ascii=True, indent=2))
        return

    if args.command == "advanced-compile-inline":
        print(
            json.dumps(
                advanced_compile_task(source_id=args.source_id, doc_id=args.doc_id, limit=args.limit),
                ensure_ascii=True,
                indent=2,
            )
        )
        return

    if args.command == "run-ingest-inline":
        print(
            json.dumps(
                {
                    "indexed_items": run_ingest_task(source_id=args.source_id),
                    "source_id": args.source_id,
                },
                ensure_ascii=True,
                indent=2,
            )
        )
        return

    if args.command == "compile-wiki-inline":
        print(
            json.dumps(
                advanced_compile_task(source_id=args.source_id, doc_id=args.doc_id, limit=args.limit),
                ensure_ascii=True,
                indent=2,
            )
        )
        return

    if args.command == "health-check-inline":
        print(
            json.dumps(
                health_check_task(
                    scope=(args.scope or "vault"),
                    topic=args.topic,
                ),
                ensure_ascii=True,
                indent=2,
            )
        )
        return

    if args.command == "answer-query-inline":
        if not args.question:
            raise ValueError("--question is required for answer-query-inline.")
        print(
            json.dumps(
                answer_query_task(
                    question=args.question,
                    output_kind=(args.output_kind or "answer"),
                ),
                ensure_ascii=True,
                indent=2,
            )
        )
        return

    if args.command == "file-output-inline":
        if not args.path:
            raise ValueError("--path is required for file-output-inline.")
        print(json.dumps(file_output_task(path=args.path), ensure_ascii=True, indent=2))
        return

    if args.command == "vault-search-inline":
        if not args.query:
            raise ValueError("--query is required for vault-search-inline.")
        print(json.dumps(vault_search_task(query=args.query, limit=args.limit or 10), ensure_ascii=True, indent=2))
        return

    if args.command == "vault-insights-inline":
        print(
            json.dumps(
                vault_insights_task(query=args.query, limit=args.limit or 10),
                ensure_ascii=True,
                indent=2,
            )
        )
        return

    if args.command == "vault-show-doc-inline":
        if not args.doc_id:
            raise ValueError("--doc-id is required for vault-show-doc-inline.")
        print(json.dumps(vault_show_doc_task(doc_id=args.doc_id), ensure_ascii=True, indent=2))
        return

    if args.command == "vault-related-inline":
        if not args.doc_id:
            raise ValueError("--doc-id is required for vault-related-inline.")
        print(json.dumps(vault_related_task(doc_id=args.doc_id, limit=args.limit or 10), ensure_ascii=True, indent=2))
        return

    if args.command == "vault-list-stale-inline":
        print(
            json.dumps(
                vault_list_stale_task(source_id=args.source_id, limit=args.limit),
                ensure_ascii=True,
                indent=2,
            )
        )
        return

    if args.command in {"generate-brief-inline", "run-digest-inline"}:
        digest_id = run_digest_task(force=True, brief_date=args.brief_date, trigger="cli_brief")
        print(json.dumps({"digest_id": digest_id}, ensure_ascii=True, indent=2))
        return

    if args.command == "generate-audio-inline":
        print(
            json.dumps(
                {"status": generate_audio_task(brief_date=args.brief_date)},
                ensure_ascii=True,
                indent=2,
            )
        )
        return

    if args.command == "publish-latest-inline":
        print(json.dumps(publish_latest_task(), ensure_ascii=True, indent=2))
        return

    if args.command == "publish-date-inline":
        if not args.brief_date:
            raise ValueError("--brief-date is required for publish-date-inline.")
        print(json.dumps(publish_date_task(args.brief_date), ensure_ascii=True, indent=2))
        return

    if args.command == "sync-vault-inline":
        print(json.dumps(sync_vault_task(), ensure_ascii=True, indent=2))
        return

    if args.command == "audit-vault-inline":
        print(json.dumps(audit_vault_task(), ensure_ascii=True, indent=2))
        return

    if args.command == "export-sqlite-to-vault-inline":
        print(json.dumps(export_sqlite_to_vault_task(), ensure_ascii=True, indent=2))
        return

    if args.command == "pair-device-code":
        label = args.label or "iPad"
        result = LocalControlService().create_pairing_code(label=label)
        print(
            json.dumps(
                {
                    "device_label": result.device_label,
                    "pairing_token": result.pairing_token,
                    "pairing_url": result.pairing_url,
                    "expires_at": result.expires_at.isoformat(),
                    "hosted_return_url": result.hosted_return_url,
                    "qr_svg": result.qr_svg,
                },
                ensure_ascii=True,
                indent=2,
            )
        )
        return

    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
