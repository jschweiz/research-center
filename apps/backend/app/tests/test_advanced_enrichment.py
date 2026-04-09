from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from app.schemas.advanced_enrichment import CodexEnrichmentManifest
from app.services.vault_advanced_enrichment import VaultAdvancedEnrichmentService
from app.services.vault_runtime import content_hash
from app.vault.models import RawDocumentFrontmatter
from app.vault.store import VaultStore


def _seed_raw_document() -> RawDocumentFrontmatter:
    store = VaultStore()
    now = datetime(2026, 4, 7, 12, 0, tzinfo=UTC)
    body = "A raw source document about agent evaluations, verifier routing, and research workflows."
    frontmatter = RawDocumentFrontmatter(
        id="2026-04-07-openai-openai-evals-1234abcd",
        kind="blog-post",
        title="OpenAI evals note",
        source_url="https://openai.com/index/evals",
        source_name="OpenAI",
        authors=["OpenAI"],
        published_at=now,
        ingested_at=now,
        content_hash=content_hash("OpenAI evals note", body),
        tags=["evals", "agents"],
        status="active",
        asset_paths=[],
        source_id="openai-website",
        source_pipeline_id="openai-website",
        external_key="https://openai.com/index/evals",
        canonical_url="https://openai.com/index/evals",
        doc_role="primary",
        parent_id=None,
        index_visibility="visible",
        fetched_at=now,
        short_summary="An official blog post about eval workflows.",
        lightweight_enrichment_status="succeeded",
        lightweight_enriched_at=now,
        lightweight_enrichment_model="llama3.2",
        lightweight_enrichment_input_hash=content_hash("OpenAI evals note", body),
        lightweight_enrichment_error=None,
    )
    store.write_raw_document(kind=frontmatter.kind, doc_id=frontmatter.id, frontmatter=frontmatter, body=body)
    return frontmatter


def test_command_builder_emits_expected_codex_exec_invocation(client) -> None:
    service = VaultAdvancedEnrichmentService()
    manifest = CodexEnrichmentManifest(
        run_id="run-1",
        job_type="compile",
        vault_root=str(service.store.root),
        target_paths=["wiki/"],
        allowed_write_globs=["wiki/**/*.md"],
        web_allowed=True,
    )
    bundle_dir = service.runs_root / "run-1"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    bundle = {
        "schema_path": bundle_dir / "final-schema.json",
        "final_path": bundle_dir / "final.json",
    }

    command = service._build_command(manifest=manifest, bundle=bundle)

    assert command[0].endswith("codex")
    assert "--full-auto" in command
    assert "--search" in command
    assert "exec" in command
    assert "-C" in command
    assert str(service.store.root) in command
    assert "--output-schema" in command
    assert str(bundle["schema_path"]) in command
    assert "--output-last-message" in command
    assert str(bundle["final_path"]) in command
    assert "--json" in command


def test_run_bundle_writes_strict_output_schema(client) -> None:
    service = VaultAdvancedEnrichmentService()
    manifest = CodexEnrichmentManifest(
        run_id="run-schema",
        job_type="health_check",
        vault_root=str(service.store.root),
        target_paths=["outputs/health-checks/report.md"],
        allowed_write_globs=["outputs/health-checks/**"],
    )

    bundle = service._create_run_bundle("run-schema", manifest)  # noqa: SLF001
    schema = json.loads(bundle["schema_path"].read_text(encoding="utf-8"))

    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert schema["$defs"]["CodexFollowUpJob"]["type"] == "object"
    assert schema["$defs"]["CodexFollowUpJob"]["additionalProperties"] is False


def test_successful_advanced_compile_records_bundle_and_rebuilds_indexes(
    client,
    monkeypatch,
) -> None:
    frontmatter = _seed_raw_document()
    service = VaultAdvancedEnrichmentService()

    monkeypatch.setattr(
        VaultAdvancedEnrichmentService,
        "_ensure_codex_ready",
        lambda self: {
            "available": True,
            "authenticated": True,
            "model": "gpt-5.4",
            "profile": "default",
            "search_enabled": True,
        },
    )

    def _fake_run(self, *, command, prompt_text, events_path, stderr_path):
        del prompt_text
        final_path = Path(command[command.index("--output-last-message") + 1])
        events_path.write_text('{"type":"run.started"}\n', encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        self.store.write_text(
            self.store.wiki_dir / "concepts" / "agent-evals.md",
            "\n".join(
                [
                    "---",
                    "id: wiki:concepts:agent-evals",
                    "page_type: concept",
                    "title: Agent Evals",
                    "aliases:",
                    "  - Evaluation Agents",
                    "source_refs:",
                    f"  - {frontmatter.id}",
                    "backlinks:",
                    "updated_at: 2026-04-07T12:00:00+00:00",
                    "managed: true",
                    "---",
                    "# Agent Evals",
                    "",
                    "This page summarizes evaluation-agent work grounded in the raw document.",
                    "",
                    f"Source note: [[{frontmatter.title}]]",
                    "",
                ]
            ),
        )
        final_path.write_text(
            json.dumps(
                {
                    "job_type": "compile",
                    "summary": "Codex updated the wiki for the new raw document.",
                    "touched_files": ["wiki/concepts/agent-evals.md"],
                    "created_wiki_pages": ["wiki/concepts/agent-evals.md"],
                    "updated_wiki_pages": [],
                    "output_paths": [],
                    "unresolved_questions": [],
                    "suggested_follow_up_jobs": [],
                }
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(VaultAdvancedEnrichmentService, "_run_codex_process", _fake_run)

    run = service.run_compile(limit=5)

    assert run.status == "succeeded"
    assert run.operation_kind == "advanced_compile"
    assert run.prompt_path is not None
    assert Path(run.prompt_path).exists()
    assert run.manifest_path is not None
    assert Path(run.manifest_path).exists()
    assert run.final_summary is not None
    assert run.final_summary["job_type"] == "compile"
    assert run.changed_file_count >= 1
    assert service.store.load_pages_index().pages
    assert service.store.load_graph_index().nodes
    compile_state = service._load_compile_state()  # noqa: SLF001
    assert frontmatter.id in compile_state.documents
    assert compile_state.documents[frontmatter.id].last_compile_run_id == run.id


def test_failed_advanced_compile_preserves_compile_state_and_surfaces_stderr(
    client,
    monkeypatch,
) -> None:
    frontmatter = _seed_raw_document()
    service = VaultAdvancedEnrichmentService()

    monkeypatch.setattr(
        VaultAdvancedEnrichmentService,
        "_ensure_codex_ready",
        lambda self: {
            "available": True,
            "authenticated": True,
            "model": "gpt-5.4",
            "profile": "default",
            "search_enabled": True,
        },
    )

    def _fake_run(self, *, command, prompt_text, events_path, stderr_path):
        del self, prompt_text
        final_path = Path(command[command.index("--output-last-message") + 1])
        final_path.unlink(missing_ok=True)
        events_path.write_text('{"type":"run.started"}\n', encoding="utf-8")
        stderr_path.write_text("authentication failed while starting codex", encoding="utf-8")
        return 1

    monkeypatch.setattr(VaultAdvancedEnrichmentService, "_run_codex_process", _fake_run)

    run = service.run_compile(limit=5)

    assert run.status == "failed"
    assert run.exit_code == 1
    assert run.stderr_excerpt
    compile_state = service._load_compile_state()  # noqa: SLF001
    assert frontmatter.id not in compile_state.documents


def test_health_check_surfaces_event_log_failure_when_codex_exits_zero(
    client,
    monkeypatch,
) -> None:
    _seed_raw_document()
    service = VaultAdvancedEnrichmentService()

    monkeypatch.setattr(
        VaultAdvancedEnrichmentService,
        "_ensure_codex_ready",
        lambda self: {
            "available": True,
            "authenticated": True,
            "model": "gpt-5.4",
            "profile": "default",
            "search_enabled": True,
        },
    )

    def _fake_run(self, *, command, prompt_text, events_path, stderr_path):
        del self, prompt_text
        final_path = Path(command[command.index("--output-last-message") + 1])
        final_path.write_text("", encoding="utf-8")
        events_path.write_text(
            "\n".join(
                [
                    '{"type":"thread.started","thread_id":"thread-1"}',
                    '{"type":"turn.started"}',
                    '{"type":"error","message":"{\\"code\\":\\"InvalidParameter\\",\\"message\\":\\"{\\\\n  \\\\\\"error\\\\\\": {\\\\n    \\\\\\"message\\\\\\": \\\\\\"Invalid schema for response_format \\\\\'codex_output_schema\\\\\': In context=(), \\\\\'additionalProperties\\\\\' is required to be supplied and to be false.\\\\\\",\\\\n    \\\\\\"type\\\\\\": \\\\\\"invalid_request_error\\\\\\",\\\\n    \\\\\\"param\\\\\\": \\\\\\"text.format.schema\\\\\\",\\\\n    \\\\\\"code\\\\\\": \\\\\\"invalid_json_schema\\\\\\"\\\\n  }\\\\n}\\"}"}',
                    '{"type":"turn.failed","error":{"message":"{\\"code\\":\\"InvalidParameter\\",\\"message\\":\\"{\\\\n  \\\\\\"error\\\\\\": {\\\\n    \\\\\\"message\\\\\\": \\\\\\"Invalid schema for response_format \\\\\'codex_output_schema\\\\\': In context=(), \\\\\'additionalProperties\\\\\' is required to be supplied and to be false.\\\\\\",\\\\n    \\\\\\"type\\\\\\": \\\\\\"invalid_request_error\\\\\\",\\\\n    \\\\\\"param\\\\\\": \\\\\\"text.format.schema\\\\\\",\\\\n    \\\\\\"code\\\\\\": \\\\\\"invalid_json_schema\\\\\\"\\\\n  }\\\\n}\\"}"}}',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        stderr_path.write_text(
            "Reading prompt from stdin...\n"
            "Warning: no last agent message; wrote empty content to final.json\n",
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(VaultAdvancedEnrichmentService, "_run_codex_process", _fake_run)

    run = service.run_health_check(scope="vault")

    assert run.status == "failed"
    assert "Invalid schema for response_format" in run.summary
    assert any("Invalid schema for response_format" in error for error in run.errors)
    assert run.stderr_excerpt is None
