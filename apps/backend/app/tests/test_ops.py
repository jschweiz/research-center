import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.db.models import IngestionRunType, RunStatus
from app.schemas.ops import IngestionRunHistoryRead
from app.services.vault_advanced_enrichment import VaultAdvancedEnrichmentService
from app.services.vault_operations import VaultOperationService
from app.services.vault_runtime import RunRecorder, content_hash
from app.tests.support_publication import seed_publishable_vault
from app.vault.store import VaultStore


def _build_run(operation_kind: str, title: str, summary: str, *, run_id: str) -> IngestionRunHistoryRead:
    timestamp = "2026-04-07T12:00:00Z"
    return IngestionRunHistoryRead.model_validate(
        {
            "id": run_id,
            "run_type": IngestionRunType.DEEPER_SUMMARY,
            "status": RunStatus.SUCCEEDED,
            "operation_kind": operation_kind,
            "trigger": "manual_test",
            "title": title,
            "summary": summary,
            "started_at": timestamp,
            "finished_at": timestamp,
            "affected_edition_days": [],
            "total_titles": 0,
            "source_count": 0,
            "failed_source_count": 0,
            "created_count": 0,
            "updated_count": 0,
            "duplicate_mention_count": 0,
            "extractor_fallback_count": 0,
            "ai_prompt_tokens": 0,
            "ai_completion_tokens": 0,
            "ai_total_tokens": 0,
            "ai_cost_usd": 0.0,
            "tts_cost_usd": 0.0,
            "total_cost_usd": 0.0,
            "average_extraction_confidence": None,
            "basic_info": [],
            "logs": [],
            "steps": [],
            "source_stats": [],
            "errors": [],
            "output_paths": [],
            "changed_file_count": 0,
        }
    )


def test_api_responses_disable_http_caching(authenticated_client: TestClient) -> None:
    history = authenticated_client.get("/api/ops/ingestion-runs")

    assert history.status_code == 200
    assert history.headers["cache-control"] == "no-store"
    assert history.headers["pragma"] == "no-cache"
    assert history.headers["expires"] == "0"


def test_operation_history_includes_live_running_updates(authenticated_client: TestClient) -> None:
    store = VaultStore()
    store.ensure_layout()
    recorder = RunRecorder(store)

    run = recorder.start(
        run_type=IngestionRunType.INGEST,
        operation_kind="raw_fetch",
        trigger="manual_fetch",
        title="Raw fetch",
        summary="Fetching configured sources into the raw vault.",
    )
    recorder.log(run, "Starting fetch.")
    step = recorder.start_step(run, step_kind="raw_fetch")
    recorder.log_step(run, step, "Downloading feed.", level="success")

    running_history = authenticated_client.get("/api/ops/ingestion-runs")
    assert running_history.status_code == 200
    running_payload = running_history.json()
    assert running_payload[0]["id"] == run.id
    assert running_payload[0]["status"] == "running"
    assert running_payload[0]["logs"][-1]["message"] == "Starting fetch."
    assert running_payload[0]["steps"][0]["logs"][-1]["message"] == "Downloading feed."

    recorder.finish_step(run, step, status=RunStatus.SUCCEEDED, created_count=2)
    recorder.finish(run, status=RunStatus.SUCCEEDED, summary="Raw source fetch completed.")

    finished_history = authenticated_client.get("/api/ops/ingestion-runs")
    assert finished_history.status_code == 200
    finished_payload = finished_history.json()
    matching = [entry for entry in finished_payload if entry["id"] == run.id]
    assert len(matching) == 1
    assert matching[0]["status"] == "succeeded"
    assert matching[0]["summary"] == "Raw source fetch completed."
    assert matching[0]["steps"][0]["created_count"] == 2


def test_operation_history_surfaces_ai_trace_artifacts(authenticated_client: TestClient) -> None:
    store = VaultStore()
    store.ensure_layout()
    recorder = RunRecorder(store)
    prompt_path = store.local_state_root / "ai-traces" / "test-run" / "prompt.md"
    trace_path = store.local_state_root / "ai-traces" / "test-run" / "trace.json"
    store.write_text(prompt_path, "# Prompt\n")
    store.write_json(trace_path, {"trace_id": "trace-1"})

    run = recorder.start(
        run_type=IngestionRunType.DIGEST,
        operation_kind="brief_generation",
        trigger="manual_digest",
        title="Brief generation",
        summary="Generating the brief.",
    )
    recorder.record_ai_trace(
        run,
        {
            "trace_id": "trace-1",
            "provider": "gemini",
            "model": "gemini-2.5-flash",
            "operation": "compose_editorial_note",
            "status": "succeeded",
            "recorded_at": "2026-04-08T07:15:00Z",
            "duration_ms": 412,
            "prompt_sha256": "abc123",
            "prompt_path": str(prompt_path),
            "trace_path": str(trace_path),
            "prompt_tokens": 128,
            "completion_tokens": 24,
            "total_tokens": 152,
            "cost_usd": 0.000098,
            "context": {"operation_run_id": run.id, "brief_date": "2026-04-08"},
        },
    )
    recorder.finish(run, status=RunStatus.SUCCEEDED, summary="Generated the brief.")

    response = authenticated_client.get("/api/ops/ingestion-runs")
    assert response.status_code == 200
    payload = next(entry for entry in response.json() if entry["id"] == run.id)
    assert payload["ai_total_tokens"] == 152
    assert payload["ai_cost_usd"] == 0.000098
    assert payload["prompt_path"] == str(prompt_path)
    assert payload["manifest_path"] is not None
    manifest = json.loads(Path(payload["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["trace_count"] == 1
    assert manifest["traces"][0]["trace_id"] == "trace-1"


def test_pipeline_status_reports_lightweight_pending_count(
    authenticated_client: TestClient,
) -> None:
    seed_publishable_vault()

    response = authenticated_client.get("/api/ops/pipeline-status")

    assert response.status_code == 200
    assert response.json() == {
        "raw_document_count": 1,
        "lightweight_pending_count": 1,
        "items_index": {
            "up_to_date": True,
            "stale_document_count": 0,
            "indexed_item_count": 1,
            "generated_at": response.json()["items_index"]["generated_at"],
        },
    }


def test_pipeline_status_counts_only_never_enriched_documents(
    authenticated_client: TestClient,
) -> None:
    seed_publishable_vault()
    store = VaultStore()
    document = store.read_raw_document_relative("raw/article/publish-fixture-item/source.md")
    assert document is not None
    updated_frontmatter = document.frontmatter.model_copy(
        update={
            "lightweight_enrichment_status": "succeeded",
            "lightweight_enriched_at": document.frontmatter.ingested_at,
            "lightweight_enrichment_model": "gemma4:e2b",
            "lightweight_enrichment_input_hash": "test-enrichment-hash",
        }
    )
    store.write_raw_document(
        kind=updated_frontmatter.kind,
        doc_id=updated_frontmatter.id,
        frontmatter=updated_frontmatter,
        body=document.body,
    )

    response = authenticated_client.get("/api/ops/pipeline-status")

    assert response.status_code == 200
    assert response.json()["lightweight_pending_count"] == 0


def test_pipeline_status_reports_stale_items_index_after_raw_change(
    authenticated_client: TestClient,
) -> None:
    seed_publishable_vault()
    store = VaultStore()
    document = store.read_raw_document_relative("raw/article/publish-fixture-item/source.md")
    assert document is not None
    updated_body = f"{document.body}\nAn extra indexed sentence."
    updated_frontmatter = document.frontmatter.model_copy(
        update={"content_hash": content_hash(document.frontmatter.title, updated_body)}
    )
    store.write_raw_document(
        kind=updated_frontmatter.kind,
        doc_id=updated_frontmatter.id,
        frontmatter=updated_frontmatter,
        body=updated_body,
    )

    response = authenticated_client.get("/api/ops/pipeline-status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["items_index"]["up_to_date"] is False
    assert payload["items_index"]["stale_document_count"] == 1
    assert payload["items_index"]["indexed_item_count"] == 1


def test_staged_worker_pipeline_endpoints_return_job_responses(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(VaultOperationService, "run_ingest_pipeline", lambda self: "run-ingest")
    monkeypatch.setattr(VaultOperationService, "fetch_sources", lambda self: "run-fetch-sources")
    monkeypatch.setattr(
        VaultOperationService,
        "lightweight_enrich",
        lambda self: _build_run(
            "lightweight_enrichment",
            "Lightweight enrichment",
            "Lightweight enrichment completed.",
            run_id="run-lightweight-enrich",
        ),
    )
    monkeypatch.setattr(VaultOperationService, "rebuild_index", lambda self: "run-rebuild-index")
    monkeypatch.setattr(
        VaultOperationService,
        "run_advanced_compile",
        lambda self, source_id=None, doc_id=None, limit=None: _build_run(
            "advanced_compile",
            "Compile wiki with Codex",
            "Codex compile completed.",
            run_id="run-compile-wiki",
        ),
    )
    monkeypatch.setattr(
        VaultOperationService,
        "run_health_check",
        lambda self, scope="vault", topic=None: _build_run(
            "health_check",
            "Run health check",
            "Health check completed.",
            run_id="run-health-check",
        ),
    )
    monkeypatch.setattr(
        VaultOperationService,
        "run_answer_query",
        lambda self, question, output_kind="answer": _build_run(
            "answer_query",
            "Ask Codex",
            "Answer query completed.",
            run_id="run-answer-query",
        ),
    )
    monkeypatch.setattr(
        VaultOperationService,
        "run_file_output",
        lambda self, path: _build_run(
            "file_output",
            "File output into wiki",
            "Filed output into wiki.",
            run_id="run-file-output",
        ),
    )
    monkeypatch.setattr(VaultOperationService, "generate_audio_only", lambda self, brief_date=None: "run-generate-audio")
    monkeypatch.setattr(VaultOperationService, "publish_latest", lambda self, brief_date=None: "run-publish")

    run_ingest = authenticated_client.post("/api/ops/run-ingest")
    assert run_ingest.status_code == 200
    assert run_ingest.json() == {
        "queued": False,
        "task_name": "ingest",
        "detail": "Fetch, lightweight enrichment, and index rebuild completed.",
        "operation_run_id": "run-ingest",
    }

    fetch_sources = authenticated_client.post("/api/ops/fetch-sources")
    assert fetch_sources.status_code == 200
    assert fetch_sources.json() == {
        "queued": False,
        "task_name": "fetch_sources",
        "detail": "Raw source fetch completed.",
        "operation_run_id": "run-fetch-sources",
    }

    sync_sources = authenticated_client.post("/api/ops/sync-sources")
    assert sync_sources.status_code == 200
    assert sync_sources.json() == {
        "queued": False,
        "task_name": "fetch_sources",
        "detail": "Raw source fetch completed.",
        "operation_run_id": "run-fetch-sources",
    }

    lightweight_enrich = authenticated_client.post("/api/ops/lightweight-enrich")
    assert lightweight_enrich.status_code == 200
    assert lightweight_enrich.json() == {
        "queued": False,
        "task_name": "lightweight_enrich",
        "detail": "Lightweight enrichment completed.",
        "operation_run_id": "run-lightweight-enrich",
    }

    rebuild_index = authenticated_client.post("/api/ops/rebuild-items-index")
    assert rebuild_index.status_code == 200
    assert rebuild_index.json() == {
        "queued": False,
        "task_name": "rebuild_items_index",
        "detail": "Local DB index rebuild completed.",
        "operation_run_id": "run-rebuild-index",
    }

    compile_wiki = authenticated_client.post("/api/ops/compile-wiki")
    assert compile_wiki.status_code == 200
    assert compile_wiki.json() == {
        "queued": False,
        "task_name": "compile_wiki",
        "detail": "Codex compile completed.",
        "operation_run_id": "run-compile-wiki",
    }

    advanced_compile = authenticated_client.post("/api/ops/advanced-compile", json={"limit": 4})
    assert advanced_compile.status_code == 200
    assert advanced_compile.json() == {
        "queued": False,
        "task_name": "advanced_compile",
        "detail": "Codex compile completed.",
        "operation_run_id": "run-compile-wiki",
    }

    health_check = authenticated_client.post("/api/ops/health-check", json={"scope": "wiki"})
    assert health_check.status_code == 200
    assert health_check.json() == {
        "queued": False,
        "task_name": "health_check",
        "detail": "Health check completed.",
        "operation_run_id": "run-health-check",
    }

    answer_query = authenticated_client.post(
        "/api/ops/answer-query",
        json={"question": "What changed?", "output_kind": "answer"},
    )
    assert answer_query.status_code == 200
    assert answer_query.json() == {
        "queued": False,
        "task_name": "answer_query",
        "detail": "Answer query completed.",
        "operation_run_id": "run-answer-query",
    }

    file_output = authenticated_client.post(
        "/api/ops/file-output",
        json={"path": "outputs/answers/2026-04-07/report.md"},
    )
    assert file_output.status_code == 200
    assert file_output.json() == {
        "queued": False,
        "task_name": "file_output",
        "detail": "Filed output into wiki.",
        "operation_run_id": "run-file-output",
    }

    generate_audio = authenticated_client.post("/api/ops/generate-audio")
    assert generate_audio.status_code == 200
    assert generate_audio.json() == {
        "queued": False,
        "task_name": "generate_audio",
        "detail": "Audio generation completed.",
        "operation_run_id": "run-generate-audio",
    }

    publish_latest = authenticated_client.post("/api/ops/publish-latest")
    assert publish_latest.status_code == 200
    assert publish_latest.json() == {
        "queued": False,
        "task_name": "publish_latest",
        "detail": "Viewer publish completed.",
        "operation_run_id": "run-publish",
    }

    monkeypatch.setattr(
        VaultAdvancedEnrichmentService,
        "codex_status",
        lambda self: {
            "available": True,
            "authenticated": True,
            "binary": "codex",
            "model": "gpt-5.4",
            "profile": "default",
            "search_enabled": True,
            "timeout_minutes": 20,
            "compile_batch_size": 12,
            "detail": "ready",
        },
    )
    runtime = authenticated_client.get("/api/ops/advanced-runtime")
    assert runtime.status_code == 200
    assert runtime.json()["model"] == "gpt-5.4"


def test_deep_enrichment_alias_returns_advanced_compile_run(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        VaultOperationService,
        "run_deep_enrichment_placeholder",
        lambda self: _build_run(
            "advanced_compile",
            "Compile wiki with Codex",
            "Codex compile completed.",
            run_id="run-deep-enrichment",
        ),
    )

    response = authenticated_client.post("/api/ops/deep-enrichment")

    assert response.status_code == 200
    payload = response.json()
    assert payload["queued"] is False
    assert payload["task_name"] == "deep_enrichment"
    assert payload["detail"] == "Codex compile completed."
    assert payload["operation_run_id"] == "run-deep-enrichment"
