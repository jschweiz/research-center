from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path, PurePosixPath
from textwrap import dedent
from typing import Any

from app.core.config import BACKEND_ROOT, get_settings
from app.db.models import IngestionRunType, RunStatus
from app.schemas.advanced_enrichment import (
    AdvancedOutputKind,
    CodexEnrichmentManifest,
    CodexEnrichmentSummary,
    CodexManifestRawDoc,
    CodexManifestTopic,
    CodexManifestTopicRef,
    CodexManifestWikiPage,
    CompileState,
    CompileStateEntry,
    HealthCheckScope,
)
from app.schemas.ops import IngestionRunHistoryRead, OperationBasicInfoRead
from app.services.profile import load_profile_snapshot
from app.services.vault_insights import VaultInsightsService
from app.services.vault_runtime import RunRecorder, current_profile_date, slugify, utcnow
from app.services.vault_wiki import VaultWikiService
from app.services.vault_wiki_index import VaultWikiIndexService
from app.vault.models import PageIndexEntry, RawDocument, VaultItemRecord
from app.vault.store import LeaseBusyError, VaultStore

DEFAULT_COMMAND_TIMEOUT_SECONDS = 30
READ_ONLY_TOOL_LIMIT = 12


class CodexEnrichmentError(RuntimeError):
    pass


class VaultAdvancedEnrichmentService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.store = VaultStore()
        self.runs = RunRecorder(self.store)
        self.insights = VaultInsightsService()
        self.wiki = VaultWikiService()
        self.wiki_index = VaultWikiIndexService()
        self.store.ensure_layout()

    @property
    def enrichment_root(self) -> Path:
        return self.store.local_state_root / "codex-enrichment"

    @property
    def compile_state_path(self) -> Path:
        return self.enrichment_root / "compile-state.json"

    @property
    def runs_root(self) -> Path:
        return self.enrichment_root / "runs"

    def codex_status(self) -> dict[str, object]:
        binary_path = self._resolve_codex_binary()
        status: dict[str, object] = {
            "available": False,
            "authenticated": False,
            "binary": str(binary_path or self.settings.codex_binary),
            "model": self.settings.codex_model,
            "profile": self.settings.codex_profile,
            "search_enabled": self.settings.codex_search_enabled,
            "timeout_minutes": self.settings.codex_timeout_minutes,
            "compile_batch_size": self.settings.codex_compile_batch_size,
            "detail": None,
        }
        if binary_path is None:
            status["detail"] = f"Codex CLI '{self.settings.codex_binary}' is not installed."
            return status

        try:
            process = subprocess.run(
                [str(binary_path), "login", "status"],
                capture_output=True,
                text=True,
                timeout=DEFAULT_COMMAND_TIMEOUT_SECONDS,
                check=False,
            )
        except Exception as exc:
            status["detail"] = f"Codex CLI could not be checked: {exc}"
            return status

        combined = "\n".join(
            part.strip()
            for part in (process.stdout, process.stderr)
            if isinstance(part, str) and part.strip()
        )
        status["available"] = True
        status["authenticated"] = process.returncode == 0
        status["detail"] = combined or None
        return status

    def run_compile(
        self,
        *,
        source_id: str | None = None,
        doc_id: str | None = None,
        limit: int | None = None,
        trigger: str = "manual_advanced_compile",
    ) -> IngestionRunHistoryRead:
        run = self.runs.start(
            run_type=IngestionRunType.DEEPER_SUMMARY,
            operation_kind="advanced_compile",
            trigger=trigger,
            title="Compile wiki with Codex",
            summary="Codex is incrementally maintaining the wiki from the raw vault.",
        )
        lease = None
        try:
            lease = self._acquire_lease("advanced-compile")
            status = self._ensure_codex_ready()
            documents = self._select_compile_documents(source_id=source_id, doc_id=doc_id, limit=limit)
            run.basic_info.extend(
                [
                    OperationBasicInfoRead(label="Codex model", value=str(status.get("model") or "default")),
                    OperationBasicInfoRead(label="Codex profile", value=str(status.get("profile") or "default")),
                    OperationBasicInfoRead(label="Web search", value="enabled" if bool(status.get("search_enabled")) else "disabled"),
                    OperationBasicInfoRead(label="Candidates", value=str(len(documents))),
                ]
            )

            compile_step = self.runs.start_step(run, step_kind="advanced_compile", source_id=source_id, doc_id=doc_id)
            if doc_id and not documents:
                message = f"Raw document '{doc_id}' was not found in the vault."
                compile_step.errors.append(message)
                run.errors.append(message)
                self.runs.finish_step(run, compile_step, status=RunStatus.FAILED)
                return self.runs.finish(run, status=RunStatus.FAILED, summary=message)

            seeded_pages = self._seed_system_wiki_context(run, trigger=trigger)
            if not documents:
                graph = self.store.load_graph_index()
                self.runs.log_step(
                    run,
                    compile_step,
                    "No stale raw documents were selected. Refreshed the system research map and rebuilt wiki indexes.",
                    level="warning",
                )
                self.runs.finish_step(run, compile_step, status=RunStatus.SUCCEEDED, skipped_count=1)
                run.basic_info.extend(
                    [
                        OperationBasicInfoRead(label="Pages index", value="SQLite `vault_wiki_pages`"),
                        OperationBasicInfoRead(label="Graph index", value="SQLite projection `graph`"),
                        OperationBasicInfoRead(label="Wiki pages", value=str(len(seeded_pages.pages))),
                        OperationBasicInfoRead(label="Graph edges", value=str(len(graph.edges))),
                    ]
                )
                return self.runs.finish(
                    run,
                    status=RunStatus.SUCCEEDED,
                    summary="No stale raw documents required Codex compile. The system topic map and wiki indexes were refreshed.",
                )

            candidate_pages = self._compile_candidate_pages(documents)
            manifest = self._build_compile_manifest(run_id=run.id, documents=documents, pages=candidate_pages)
            bundle = self._create_run_bundle(run.id, manifest)
            wiki_snapshot_before = self._snapshot_paths(manifest.allowed_write_globs)

            self._record_run_bundle(run, bundle=bundle)
            summary = self._execute_codex_job(
                run=run,
                step=compile_step,
                manifest=manifest,
                bundle=bundle,
            )

            changed_files = self._changed_paths(wiki_snapshot_before, self._snapshot_paths(manifest.allowed_write_globs))
            self._validate_allowed_changes(changed_files, manifest.allowed_write_globs)
            wiki_changes = [path for path in changed_files if path.startswith("wiki/")]
            health_check_changes = [path for path in changed_files if path.startswith("outputs/health-checks/")]
            self.runs.finish_step(
                run,
                compile_step,
                status=RunStatus.SUCCEEDED,
                created_count=len(summary.created_wiki_pages),
                updated_count=len(summary.updated_wiki_pages),
                counts_by_kind={
                    "wiki": len(wiki_changes),
                    "health-check-report": len(health_check_changes),
                },
            )
            run.total_titles = len(documents)
            run.updated_count = len(summary.updated_wiki_pages)
            run.changed_file_count = len(changed_files)
            run.output_paths = list(dict.fromkeys(summary.output_paths + health_check_changes))
            run.final_summary = summary.model_dump(mode="json")
            pages, graph = self._rebuild_wiki_indexes(run)
            self._update_compile_state(documents=documents, wiki_paths=wiki_changes, run_id=run.id)
            run.basic_info.extend(
                [
                    OperationBasicInfoRead(label="Changed files", value=str(len(changed_files))),
                    OperationBasicInfoRead(label="Wiki pages created", value=str(len(summary.created_wiki_pages))),
                    OperationBasicInfoRead(label="Wiki pages updated", value=str(len(summary.updated_wiki_pages))),
                    OperationBasicInfoRead(label="Pages index", value="SQLite `vault_wiki_pages`"),
                    OperationBasicInfoRead(label="Graph index", value="SQLite projection `graph`"),
                    OperationBasicInfoRead(label="Indexed pages", value=str(len(pages.pages))),
                    OperationBasicInfoRead(label="Graph edges", value=str(len(graph.edges))),
                ]
            )
            return self.runs.finish(run, status=RunStatus.SUCCEEDED, summary=summary.summary)
        except CodexEnrichmentError as exc:
            run.errors.append(str(exc))
            return self.runs.finish(run, status=RunStatus.FAILED, summary=str(exc))
        except Exception as exc:
            run.errors.append(str(exc))
            return self.runs.finish(run, status=RunStatus.FAILED, summary=str(exc))
        finally:
            if lease is not None:
                self.store.release_lease(lease)

    def run_health_check(
        self,
        *,
        scope: HealthCheckScope = "vault",
        topic: str | None = None,
        trigger: str = "manual_health_check",
    ) -> IngestionRunHistoryRead:
        run = self.runs.start(
            run_type=IngestionRunType.DEEPER_SUMMARY,
            operation_kind="health_check",
            trigger=trigger,
            title="Run health check",
            summary="Codex is auditing the vault for integrity gaps and article candidates.",
        )
        lease = None
        try:
            lease = self._acquire_lease("health-check")
            status = self._ensure_codex_ready()
            pages = self.wiki_index.list_pages()[:READ_ONLY_TOOL_LIMIT]
            documents = self._recent_raw_docs(limit=READ_ONLY_TOOL_LIMIT)
            manifest = self._build_health_check_manifest(
                run_id=run.id,
                scope=scope,
                topic=topic,
                documents=documents,
                pages=pages,
            )
            bundle = self._create_run_bundle(run.id, manifest)
            snapshot_before = self._snapshot_paths(manifest.allowed_write_globs)
            step = self.runs.start_step(run, step_kind="health_check")
            self._record_run_bundle(run, bundle=bundle)
            run.basic_info.extend(
                [
                    OperationBasicInfoRead(label="Codex model", value=str(status.get("model") or "default")),
                    OperationBasicInfoRead(label="Scope", value=scope),
                    OperationBasicInfoRead(label="Topic", value=topic or "general"),
                    OperationBasicInfoRead(label="Target", value=", ".join(manifest.target_paths)),
                ]
            )
            summary = self._execute_codex_job(run=run, step=step, manifest=manifest, bundle=bundle)
            changed_files = self._changed_paths(snapshot_before, self._snapshot_paths(manifest.allowed_write_globs))
            self._validate_allowed_changes(changed_files, manifest.allowed_write_globs)
            run.changed_file_count = len(changed_files)
            run.output_paths = list(dict.fromkeys(summary.output_paths + changed_files))
            run.final_summary = summary.model_dump(mode="json")
            self.runs.finish_step(
                run,
                step,
                status=RunStatus.SUCCEEDED,
                created_count=len(changed_files),
                counts_by_kind={"health-check-report": len(changed_files)},
            )
            return self.runs.finish(run, status=RunStatus.SUCCEEDED, summary=summary.summary)
        except CodexEnrichmentError as exc:
            run.errors.append(str(exc))
            return self.runs.finish(run, status=RunStatus.FAILED, summary=str(exc))
        except Exception as exc:
            run.errors.append(str(exc))
            return self.runs.finish(run, status=RunStatus.FAILED, summary=str(exc))
        finally:
            if lease is not None:
                self.store.release_lease(lease)

    def run_answer_query(
        self,
        *,
        question: str,
        output_kind: AdvancedOutputKind = "answer",
        trigger: str = "manual_answer_query",
    ) -> IngestionRunHistoryRead:
        run = self.runs.start(
            run_type=IngestionRunType.DEEPER_SUMMARY,
            operation_kind="answer_query",
            trigger=trigger,
            title="Ask Codex",
            summary="Codex is answering a research question against the vault and writing a durable output.",
        )
        lease = None
        try:
            lease = self._acquire_lease("answer-query")
            status = self._ensure_codex_ready()
            documents, pages = self._search_candidates(question, limit=READ_ONLY_TOOL_LIMIT)
            manifest = self._build_answer_manifest(
                run_id=run.id,
                question=question,
                output_kind=output_kind,
                documents=documents,
                pages=pages,
            )
            bundle = self._create_run_bundle(run.id, manifest)
            snapshot_before = self._snapshot_paths(manifest.allowed_write_globs)
            step = self.runs.start_step(run, step_kind="answer_query")
            self._record_run_bundle(run, bundle=bundle)
            run.basic_info.extend(
                [
                    OperationBasicInfoRead(label="Codex model", value=str(status.get("model") or "default")),
                    OperationBasicInfoRead(label="Output kind", value=output_kind),
                    OperationBasicInfoRead(label="Target", value=", ".join(manifest.target_paths)),
                ]
            )
            summary = self._execute_codex_job(run=run, step=step, manifest=manifest, bundle=bundle)
            changed_files = self._changed_paths(snapshot_before, self._snapshot_paths(manifest.allowed_write_globs))
            self._validate_allowed_changes(changed_files, manifest.allowed_write_globs)
            run.changed_file_count = len(changed_files)
            run.output_paths = list(dict.fromkeys(summary.output_paths + changed_files))
            run.final_summary = summary.model_dump(mode="json")
            self.runs.finish_step(
                run,
                step,
                status=RunStatus.SUCCEEDED,
                created_count=len(changed_files),
                counts_by_kind={output_kind: len(changed_files)},
            )
            return self.runs.finish(run, status=RunStatus.SUCCEEDED, summary=summary.summary)
        except CodexEnrichmentError as exc:
            run.errors.append(str(exc))
            return self.runs.finish(run, status=RunStatus.FAILED, summary=str(exc))
        except Exception as exc:
            run.errors.append(str(exc))
            return self.runs.finish(run, status=RunStatus.FAILED, summary=str(exc))
        finally:
            if lease is not None:
                self.store.release_lease(lease)

    def run_file_output(
        self,
        *,
        path: str,
        trigger: str = "manual_file_output",
    ) -> IngestionRunHistoryRead:
        run = self.runs.start(
            run_type=IngestionRunType.DEEPER_SUMMARY,
            operation_kind="file_output",
            trigger=trigger,
            title="File output into wiki",
            summary="Codex is distilling a durable output back into the wiki.",
        )
        lease = None
        try:
            lease = self._acquire_lease("file-output")
            status = self._ensure_codex_ready()
            source_path = self._validate_vault_relative_path(path)
            if not source_path.exists():
                raise CodexEnrichmentError(f"Vault output '{path}' does not exist.")
            pages = self._search_pages(Path(path).stem, limit=READ_ONLY_TOOL_LIMIT)
            manifest = self._build_file_output_manifest(run_id=run.id, source_path=source_path, pages=pages)
            bundle = self._create_run_bundle(run.id, manifest)
            snapshot_before = self._snapshot_paths(manifest.allowed_write_globs)
            step = self.runs.start_step(run, step_kind="file_output")
            self._record_run_bundle(run, bundle=bundle)
            run.basic_info.extend(
                [
                    OperationBasicInfoRead(label="Codex model", value=str(status.get("model") or "default")),
                    OperationBasicInfoRead(label="Source output", value=str(path)),
                ]
            )
            summary = self._execute_codex_job(run=run, step=step, manifest=manifest, bundle=bundle)
            changed_files = self._changed_paths(snapshot_before, self._snapshot_paths(manifest.allowed_write_globs))
            self._validate_allowed_changes(changed_files, manifest.allowed_write_globs)
            run.changed_file_count = len(changed_files)
            run.output_paths = list(dict.fromkeys(summary.output_paths + changed_files))
            run.final_summary = summary.model_dump(mode="json")
            self._rebuild_wiki_indexes(run)
            self.runs.finish_step(
                run,
                step,
                status=RunStatus.SUCCEEDED,
                created_count=len(summary.created_wiki_pages),
                updated_count=len(summary.updated_wiki_pages),
                counts_by_kind={"wiki": len(changed_files)},
            )
            return self.runs.finish(run, status=RunStatus.SUCCEEDED, summary=summary.summary)
        except CodexEnrichmentError as exc:
            run.errors.append(str(exc))
            return self.runs.finish(run, status=RunStatus.FAILED, summary=str(exc))
        except Exception as exc:
            run.errors.append(str(exc))
            return self.runs.finish(run, status=RunStatus.FAILED, summary=str(exc))
        finally:
            if lease is not None:
                self.store.release_lease(lease)

    def search_vault(self, *, query: str, limit: int = 10) -> dict[str, object]:
        normalized = query.strip()
        if not normalized:
            return {"query": normalized, "raw_docs": [], "wiki_pages": [], "topics": [], "rising_topics": []}
        documents, pages = self._search_candidates(normalized, limit=limit)
        topics = self._search_topics(normalized, limit=limit)
        _items, insights = self.insights.ensure_index(persist=False)
        return {
            "query": normalized,
            "raw_docs": [doc.model_dump(mode="json") for doc in documents],
            "wiki_pages": [page.model_dump(mode="json") for page in pages],
            "topics": [topic.model_dump(mode="json") for topic in topics],
            "rising_topics": [topic.model_dump(mode="json") for topic in self._rising_manifest_topics(limit=min(limit, 10))],
            "map_page": insights.map_page_path,
            "trends_page": insights.trends_page_path,
        }

    def insight_radar(self, *, query: str | None = None, limit: int = 10) -> dict[str, object]:
        normalized = (query or "").strip()
        if normalized:
            return self.search_vault(query=normalized, limit=limit)
        _items, insights = self.insights.ensure_index(persist=False)
        return {
            "query": "",
            "raw_docs": [],
            "wiki_pages": [],
            "topics": [topic.model_dump(mode="json") for topic in self._top_manifest_topics(limit=limit)],
            "rising_topics": [topic.model_dump(mode="json") for topic in self._rising_manifest_topics(limit=limit)],
            "map_page": insights.map_page_path,
            "trends_page": insights.trends_page_path,
        }

    def show_raw_document(self, *, doc_id: str) -> dict[str, object]:
        document = next((doc for doc in self.store.list_raw_documents() if doc.frontmatter.id == doc_id), None)
        if document is None:
            raise CodexEnrichmentError(f"Raw document '{doc_id}' was not found.")
        return {
            "id": document.frontmatter.id,
            "path": document.path,
            "frontmatter": document.frontmatter.model_dump(mode="json"),
            "body": document.body,
        }

    def related_documents(self, *, doc_id: str, limit: int = 10) -> dict[str, object]:
        items = self.store.load_items_index().items
        target = next((item for item in items if item.id == doc_id), None)
        if target is None:
            raise CodexEnrichmentError(f"Indexed item '{doc_id}' was not found.")
        target_topic_ids = {ref.topic_id for ref in target.topic_refs}
        related = [
            item
            for item in items
            if item.id != target.id
            and (
                item.parent_id == target.id
                or target.parent_id == item.id
                or (target.source_id and item.source_id == target.source_id)
                or bool(set(item.tags).intersection(target.tags))
                or bool(target_topic_ids.intersection(ref.topic_id for ref in item.topic_refs))
            )
        ]
        related.sort(
            key=lambda item: (
                len(target_topic_ids.intersection(ref.topic_id for ref in item.topic_refs)),
                len(set(item.tags).intersection(target.tags)),
                item.published_at or item.fetched_at or item.ingested_at,
                item.title.lower(),
            ),
            reverse=True,
        )
        return {
            "id": target.id,
            "title": target.title,
            "related": [self._manifest_raw_doc_from_item(item).model_dump(mode="json") for item in related[:limit]],
        }

    def list_stale_documents(self, *, source_id: str | None = None, limit: int | None = None) -> dict[str, object]:
        documents = self._select_compile_documents(source_id=source_id, doc_id=None, limit=limit, stale_only=True)
        return {
            "source_id": source_id,
            "count": len(documents),
            "documents": [self._manifest_raw_doc(document).model_dump(mode="json") for document in documents],
        }

    def _build_compile_manifest(
        self,
        *,
        run_id: str,
        documents: list[RawDocument],
        pages: list[PageIndexEntry],
    ) -> CodexEnrichmentManifest:
        items_by_id = self._items_by_id()
        return CodexEnrichmentManifest(
            run_id=run_id,
            job_type="compile",
            vault_root=str(self.store.root),
            target_paths=["wiki/"],
            candidate_raw_docs=[self._manifest_raw_doc(document, item_lookup=items_by_id) for document in documents],
            candidate_wiki_pages=[self._manifest_page(page) for page in pages],
            candidate_topics=self._candidate_topics_for_documents(documents, limit=READ_ONLY_TOOL_LIMIT),
            rising_topics=self._rising_manifest_topics(limit=10),
            allowed_write_globs=["wiki/**/*.md", "outputs/health-checks/**/*.md"],
            web_allowed=self.settings.codex_search_enabled,
            profile_context=self._profile_context(),
            success_criteria=[
                "Read README.md and AGENTS.md first.",
                "Inspect candidate raw docs before editing wiki pages.",
                "Anchor new synthesis to existing topic pages before inventing duplicate concepts.",
                "Treat wiki/sources, wiki/topics, wiki/trends, and wiki/maps as system-generated context scaffolds unless a correction is necessary.",
                "Update or create wiki pages only under the allowed write scope.",
                "Do not rewrite raw documents in compile mode.",
                "Return a valid JSON summary matching the provided schema.",
            ],
        )

    def _build_health_check_manifest(
        self,
        *,
        run_id: str,
        scope: HealthCheckScope,
        topic: str | None,
        documents: list[RawDocument],
        pages: list[PageIndexEntry],
    ) -> CodexEnrichmentManifest:
        date_part = current_profile_date().isoformat()
        slug = slugify(topic or f"{scope}-health-check", fallback="health-check")
        target_path = f"outputs/health-checks/{date_part}/{slug}.md"
        items_by_id = self._items_by_id()
        return CodexEnrichmentManifest(
            run_id=run_id,
            job_type="health_check",
            vault_root=str(self.store.root),
            target_paths=[target_path],
            candidate_raw_docs=[self._manifest_raw_doc(document, item_lookup=items_by_id) for document in documents],
            candidate_wiki_pages=[self._manifest_page(page) for page in pages],
            candidate_topics=self._candidate_topics_for_documents(documents, limit=READ_ONLY_TOOL_LIMIT),
            rising_topics=self._rising_manifest_topics(limit=10),
            allowed_write_globs=[target_path],
            question=topic,
            web_allowed=self.settings.codex_search_enabled,
            profile_context=self._profile_context(),
            success_criteria=[
                "Read README.md and AGENTS.md first.",
                "Inspect the vault before using web search.",
                "Write a single durable health-check report under outputs/health-checks/.",
                "Do not modify wiki pages in v1 health-check mode.",
                "Return a valid JSON summary matching the provided schema.",
            ],
        )

    def _build_answer_manifest(
        self,
        *,
        run_id: str,
        question: str,
        output_kind: AdvancedOutputKind,
        documents: list[CodexManifestRawDoc],
        pages: list[PageIndexEntry],
    ) -> CodexEnrichmentManifest:
        date_part = current_profile_date().isoformat()
        slug = slugify(question, fallback=output_kind)[:72]
        if output_kind == "slides":
            target_path = f"outputs/slides/{date_part}/{slug}.md"
            allowed_globs = [target_path]
        elif output_kind == "chart":
            target_path = f"outputs/charts/{date_part}/{slug}/"
            allowed_globs = [f"outputs/charts/{date_part}/{slug}/**"]
        else:
            target_path = f"outputs/answers/{date_part}/{slug}.md"
            allowed_globs = [target_path]
        return CodexEnrichmentManifest(
            run_id=run_id,
            job_type="answer",
            vault_root=str(self.store.root),
            target_paths=[target_path],
            candidate_raw_docs=documents,
            candidate_wiki_pages=[self._manifest_page(page) for page in pages],
            candidate_topics=self._candidate_topics_for_question(question, limit=READ_ONLY_TOOL_LIMIT),
            rising_topics=self._rising_manifest_topics(limit=10),
            allowed_write_globs=allowed_globs,
            question=question,
            output_kind=output_kind,
            web_allowed=self.settings.codex_search_enabled,
            profile_context=self._profile_context(),
            success_criteria=[
                "Read README.md and AGENTS.md first.",
                "Answer from the vault first and use web search only when needed.",
                "Write the durable output only under the allowed output path.",
                "Do not modify wiki pages during answer jobs.",
                "Return a valid JSON summary matching the provided schema.",
            ],
        )

    def _build_file_output_manifest(
        self,
        *,
        run_id: str,
        source_path: Path,
        pages: list[PageIndexEntry],
    ) -> CodexEnrichmentManifest:
        return CodexEnrichmentManifest(
            run_id=run_id,
            job_type="file_output",
            vault_root=str(self.store.root),
            target_paths=[str(source_path.relative_to(self.store.root))],
            candidate_raw_docs=[],
            candidate_wiki_pages=[self._manifest_page(page) for page in pages],
            candidate_topics=[],
            rising_topics=self._rising_manifest_topics(limit=10),
            allowed_write_globs=["wiki/**/*.md"],
            question=str(source_path.relative_to(self.store.root)),
            web_allowed=self.settings.codex_search_enabled,
            profile_context=self._profile_context(),
            success_criteria=[
                "Read README.md and AGENTS.md first.",
                "Distill durable insights from the provided output artifact back into wiki pages.",
                "Link destination wiki pages back to the source output artifact when relevant.",
                "Do not modify raw documents during file-output jobs.",
                "Return a valid JSON summary matching the provided schema.",
            ],
        )

    def _profile_context(self) -> dict[str, object]:
        profile = load_profile_snapshot()
        return {
            "favorite_topics": profile.favorite_topics,
            "favorite_authors": profile.favorite_authors,
            "favorite_sources": profile.favorite_sources,
            "ignored_topics": profile.ignored_topics,
            "summary_depth": profile.summary_depth,
            "prompt_guidance": profile.prompt_guidance.model_dump(mode="json"),
        }

    def _items_by_id(self) -> dict[str, VaultItemRecord]:
        items, _insights = self.insights.ensure_index(persist=False)
        return {item.id: item for item in items}

    def _candidate_topics_for_documents(
        self,
        documents: list[RawDocument],
        *,
        limit: int,
    ) -> list[CodexManifestTopic]:
        items_by_id = self._items_by_id()
        candidate_items = [items_by_id[document.frontmatter.id] for document in documents if document.frontmatter.id in items_by_id]
        return [self._manifest_topic(topic) for topic in self.insights.candidate_topics_for_items(candidate_items, limit=limit)]

    def _candidate_topics_for_question(self, question: str, *, limit: int) -> list[CodexManifestTopic]:
        return self._search_topics(question, limit=limit)

    def _rising_manifest_topics(self, *, limit: int) -> list[CodexManifestTopic]:
        return [self._manifest_topic(topic) for topic in self.insights.rising_topics(limit=limit)]

    def _top_manifest_topics(self, *, limit: int) -> list[CodexManifestTopic]:
        _items, insights = self.insights.ensure_index(persist=False)
        topics = sorted(
            insights.topics,
            key=lambda topic: (
                topic.total_item_count,
                topic.source_diversity,
                topic.trend_score,
                topic.label.casefold(),
            ),
            reverse=True,
        )
        return [self._manifest_topic(topic) for topic in topics[:limit]]

    def _compile_candidate_pages(self, documents: list[RawDocument]) -> list[PageIndexEntry]:
        compile_state = self._load_compile_state()
        page_paths: list[str] = []
        doc_ids = {document.frontmatter.id for document in documents}
        for document in documents:
            entry = compile_state.documents.get(document.frontmatter.id)
            if entry:
                page_paths.extend(entry.affected_wiki_pages)
        pages_by_path = {page.path: page for page in self.wiki_index.list_pages()}
        selected_paths = list(dict.fromkeys(page_paths))
        selected_paths.extend(
            page.path
            for page in pages_by_path.values()
            if set(page.source_refs).intersection(doc_ids)
        )
        items_by_id = self._items_by_id()
        candidate_items = [items_by_id[doc_id] for doc_id in doc_ids if doc_id in items_by_id]
        for topic in self.insights.candidate_topics_for_items(candidate_items, limit=READ_ONLY_TOOL_LIMIT):
            if topic.page_path:
                selected_paths.append(topic.page_path)
        _items, insights = self.insights.ensure_index(persist=False)
        if insights.trends_page_path:
            selected_paths.append(insights.trends_page_path)
        if insights.map_page_path:
            selected_paths.append(insights.map_page_path)
        selected = [pages_by_path[path] for path in dict.fromkeys(selected_paths) if path in pages_by_path]
        return selected[: READ_ONLY_TOOL_LIMIT]

    def _recent_raw_docs(self, *, limit: int) -> list[RawDocument]:
        documents = self.store.list_raw_documents()
        documents.sort(
            key=lambda doc: (
                doc.frontmatter.published_at or doc.frontmatter.fetched_at or doc.frontmatter.ingested_at,
                doc.frontmatter.title.lower(),
            ),
            reverse=True,
        )
        return documents[:limit]

    def _search_candidates(
        self,
        query: str,
        *,
        limit: int,
    ) -> tuple[list[CodexManifestRawDoc], list[PageIndexEntry]]:
        normalized = query.casefold()
        items = self.store.load_items_index().items
        scored_items: list[tuple[int, VaultItemRecord]] = []
        for item in items:
            haystack = "\n".join(
                [
                    item.title,
                    item.source_name,
                    item.short_summary or "",
                    " ".join(item.tags),
                    " ".join(ref.label for ref in item.topic_refs),
                    item.cleaned_text or "",
                ]
            ).casefold()
            score = haystack.count(normalized) * 4
            score += int(item.title.casefold().count(normalized) * 6)
            score += sum(2 for tag in item.tags if normalized in tag.casefold())
            score += sum(3 for ref in item.topic_refs if normalized in ref.label.casefold())
            if score > 0:
                scored_items.append((score, item))
        scored_items.sort(
            key=lambda pair: (
                pair[0],
                pair[1].published_at or pair[1].fetched_at or pair[1].ingested_at,
            ),
            reverse=True,
        )
        pages = self._search_pages(query, limit=limit)
        return [self._manifest_raw_doc_from_item(item) for _score, item in scored_items[:limit]], pages

    def _search_topics(self, query: str, *, limit: int) -> list[CodexManifestTopic]:
        normalized = query.casefold()
        scored_topics: list[tuple[float, object]] = []
        _items, insights = self.insights.ensure_index(persist=False)
        for topic in insights.topics:
            haystack = "\n".join([topic.label, " ".join(topic.aliases)]).casefold()
            score = float(haystack.count(normalized) * 4 + topic.label.casefold().count(normalized) * 6)
            score += topic.trend_score * 0.1
            if score > 0:
                scored_topics.append((score, topic))
        scored_topics.sort(
            key=lambda pair: (
                pair[0],
                pair[1].trend_score,
                pair[1].last_seen_at or utcnow(),
            ),
            reverse=True,
        )
        return [self._manifest_topic(topic) for _score, topic in scored_topics[:limit]]

    def _search_pages(self, query: str, *, limit: int) -> list[PageIndexEntry]:
        normalized = query.casefold()
        page_lookup = self.wiki_index.list_pages()
        scored_pages: list[tuple[int, PageIndexEntry]] = []
        for page in page_lookup:
            haystack = "\n".join([page.title, " ".join(page.aliases), " ".join(page.source_refs)]).casefold()
            score = haystack.count(normalized) * 4 + page.title.casefold().count(normalized) * 6
            if score > 0:
                scored_pages.append((score, page))
        scored_pages.sort(key=lambda pair: (pair[0], pair[1].updated_at), reverse=True)
        return [page for _score, page in scored_pages[:limit]]

    def _manifest_raw_doc(
        self,
        document: RawDocument,
        *,
        item_lookup: dict[str, VaultItemRecord] | None = None,
    ) -> CodexManifestRawDoc:
        fm = document.frontmatter
        item = (item_lookup or self._items_by_id()).get(fm.id)
        return CodexManifestRawDoc(
            id=fm.id,
            kind=fm.kind,
            title=fm.title,
            source_id=fm.source_id,
            source_name=fm.source_name,
            content_hash=fm.content_hash,
            identity_hash=fm.identity_hash,
            canonical_url=fm.canonical_url or fm.source_url,
            raw_doc_path=document.path,
            short_summary=fm.short_summary,
            published_at=fm.published_at,
            fetched_at=fm.fetched_at,
            doc_role=fm.doc_role,
            parent_id=fm.parent_id,
            index_visibility=fm.index_visibility,
            topic_refs=[CodexManifestTopicRef.model_validate(ref.model_dump(mode="json")) for ref in (item.topic_refs if item else [])],
            trend_score=item.trend_score if item else 0.0,
            novelty_score=item.novelty_score if item else 0.0,
        )

    def _manifest_raw_doc_from_item(self, item: VaultItemRecord) -> CodexManifestRawDoc:
        return CodexManifestRawDoc(
            id=item.id,
            kind=item.kind,
            title=item.title,
            source_id=item.source_id,
            source_name=item.source_name,
            content_hash=item.content_hash,
            identity_hash=item.identity_hash,
            canonical_url=item.canonical_url,
            raw_doc_path=item.raw_doc_path,
            short_summary=item.short_summary,
            published_at=item.published_at,
            fetched_at=item.fetched_at,
            doc_role=item.doc_role,
            parent_id=item.parent_id,
            index_visibility=item.index_visibility,
            topic_refs=[CodexManifestTopicRef.model_validate(ref.model_dump(mode="json")) for ref in item.topic_refs],
            trend_score=item.trend_score,
            novelty_score=item.novelty_score,
        )

    @staticmethod
    def _manifest_page(page: PageIndexEntry) -> CodexManifestWikiPage:
        return CodexManifestWikiPage.model_validate(page.model_dump(mode="json"))

    @staticmethod
    def _manifest_topic(topic) -> CodexManifestTopic:
        return CodexManifestTopic.model_validate(topic.model_dump(mode="json"))

    def _build_command(self, *, manifest: CodexEnrichmentManifest, bundle: dict[str, Path]) -> list[str]:
        binary_path = self._resolve_codex_binary()
        if binary_path is None:
            raise CodexEnrichmentError(f"Codex CLI '{self.settings.codex_binary}' is not installed.")
        command = [str(binary_path), "--full-auto"]
        if manifest.web_allowed and self.settings.codex_search_enabled:
            command.append("--search")
        command.extend(["exec", "-C", str(self.store.root)])
        for add_dir in self.settings.codex_add_dirs:
            command.extend(["--add-dir", str(add_dir)])
        if self.settings.codex_model:
            command.extend(["-m", self.settings.codex_model])
        if self.settings.codex_profile:
            command.extend(["-p", self.settings.codex_profile])
        command.extend(
            [
                "--output-schema",
                str(bundle["schema_path"]),
                "--output-last-message",
                str(bundle["final_path"]),
                "--json",
            ]
        )
        return command

    def _create_run_bundle(self, run_id: str, manifest: CodexEnrichmentManifest) -> dict[str, Path]:
        bundle_dir = self.runs_root / run_id
        bundle_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = bundle_dir / "manifest.json"
        prompt_path = bundle_dir / "prompt.md"
        schema_path = bundle_dir / "final-schema.json"
        final_path = bundle_dir / "final.json"
        events_path = bundle_dir / "events.jsonl"
        stderr_path = bundle_dir / "stderr.log"

        self.store.write_json(manifest_path, manifest)
        self.store.write_text(prompt_path, self._render_prompt(manifest))
        self.store.write_json(schema_path, self._strict_json_schema(CodexEnrichmentSummary.model_json_schema()))

        return {
            "bundle_dir": bundle_dir,
            "manifest_path": manifest_path,
            "prompt_path": prompt_path,
            "schema_path": schema_path,
            "final_path": final_path,
            "events_path": events_path,
            "stderr_path": stderr_path,
        }

    def _record_run_bundle(self, run, *, bundle: dict[str, Path]) -> None:
        run.prompt_path = str(bundle["prompt_path"])
        run.manifest_path = str(bundle["manifest_path"])

    def _execute_codex_job(
        self,
        *,
        run,
        step,
        manifest: CodexEnrichmentManifest,
        bundle: dict[str, Path],
    ) -> CodexEnrichmentSummary:
        command = self._build_command(manifest=manifest, bundle=bundle)
        run.codex_command = command
        self.runs.log_step(run, step, f"Executing Codex job '{manifest.job_type}'.")
        exit_code = self._run_codex_process(
            command=command,
            prompt_text=bundle["prompt_path"].read_text(encoding="utf-8"),
            events_path=bundle["events_path"],
            stderr_path=bundle["stderr_path"],
        )
        run.exit_code = exit_code
        event_error = self._event_failure_message(bundle["events_path"])
        if exit_code != 0:
            excerpt = self._stderr_excerpt(bundle["stderr_path"])
            run.stderr_excerpt = excerpt
            if event_error:
                self.runs.log_step(run, step, event_error, level="error")
            if excerpt:
                self.runs.log_step(run, step, excerpt, level="error")
            detail = event_error or excerpt
            raise CodexEnrichmentError(
                f"Codex {manifest.job_type} run failed with exit code {exit_code}."
                + (f" {detail}" if detail else "")
            )

        if event_error:
            self.runs.log_step(run, step, event_error, level="error")
            raise CodexEnrichmentError(f"Codex {manifest.job_type} run failed: {event_error}")

        summary = self._load_final_summary(bundle["final_path"])
        self.runs.log_step(run, step, f"Codex completed {manifest.job_type} successfully.", level="success")
        return summary

    def _run_codex_process(
        self,
        *,
        command: list[str],
        prompt_text: str,
        events_path: Path,
        stderr_path: Path,
    ) -> int:
        events_path.parent.mkdir(parents=True, exist_ok=True)
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with events_path.open("w", encoding="utf-8") as stdout_file, stderr_path.open("w", encoding="utf-8") as stderr_file:
                process = subprocess.run(
                    command,
                    input=prompt_text,
                    text=True,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    timeout=self.settings.codex_timeout_minutes * 60,
                    check=False,
                )
        except subprocess.TimeoutExpired as exc:
            raise CodexEnrichmentError(
                f"Codex run exceeded the timeout of {self.settings.codex_timeout_minutes} minute(s)."
            ) from exc
        return process.returncode

    def _render_prompt(self, manifest: CodexEnrichmentManifest) -> str:
        toolbelt = self._toolbelt_instructions()
        manifest_json = json.dumps(manifest.model_dump(mode="json"), indent=2, ensure_ascii=True)
        schema_json = json.dumps(CodexEnrichmentSummary.model_json_schema(), indent=2, ensure_ascii=True)
        if manifest.job_type == "compile":
            job_instructions = dedent(
                """
                Job: incrementally maintain the wiki from the raw vault.

                Required behavior:
                - Read `README.md` and `AGENTS.md` in the vault root first.
                - Inspect the candidate raw documents first, then the related wiki pages.
                - Use the candidate topics and rising topics to anchor new work to canonical topic pages before inventing new concepts.
                - Treat `wiki/sources/**`, `wiki/topics/**`, `wiki/trends/**`, and `wiki/maps/**` as system-generated scaffolds; prefer linking to them and creating higher-order synthesis pages around them.
                - Update or create concept pages, category pages, backlinks, and cross-links under `wiki/**`.
                - You may write supporting markdown under `outputs/health-checks/**` only if a compile note is genuinely useful.
                - Do not modify `raw/**` in compile mode.
                - Keep the vault as the primary source of truth. Web search is secondary and only for gap-checking when needed.
                - Prefer durable, linked markdown over throwaway prose.
                - Use readable wiki page frontmatter with: `id`, `page_type`, `title`, `aliases`, `source_refs`, `backlinks`, `updated_at`, and `managed`.
                """
            ).strip()
        elif manifest.job_type == "health_check":
            job_instructions = dedent(
                """
                Job: inspect the vault for integrity issues and write a durable report.

                Required behavior:
                - Read `README.md` and `AGENTS.md` in the vault root first.
                - Inspect the vault for inconsistent facts, missing metadata, weak links, stale or duplicated pages, article candidates, and follow-up questions.
                - Web search is allowed for fact-gap verification or missing-data repair suggestions, but the vault comes first.
                - Write exactly the requested health-check report under `outputs/health-checks/**`.
                - Do not modify `wiki/**` in v1 health-check mode.
                """
            ).strip()
        elif manifest.job_type == "answer":
            output_format = {
                "answer": "a durable Markdown answer or report",
                "slides": "a Marp-compatible Markdown slide deck",
                "chart": "a chart bundle with supporting markdown and generated assets",
            }[manifest.output_kind or "answer"]
            job_instructions = dedent(
                f"""
                Job: answer the user question and produce {output_format}.

                Required behavior:
                - Read `README.md` and `AGENTS.md` in the vault root first.
                - Use the vault first and web search second.
                - Use local shell tools if useful.
                - Write only under the requested output path.
                - Do not modify `wiki/**` during answer jobs.
                """
            ).strip()
        else:
            job_instructions = dedent(
                """
                Job: distill durable insights from an existing output artifact back into the wiki.

                Required behavior:
                - Read `README.md` and `AGENTS.md` in the vault root first.
                - Read the referenced output artifact before editing the wiki.
                - Update or create `wiki/**` pages only.
                - Add backlinks from the destination wiki pages to the source output artifact when relevant.
                - Do not modify `raw/**`.
                """
            ).strip()

        return dedent(
            f"""
            You are maintaining a file-native Obsidian knowledge base.
            Vault first, web second.

            {job_instructions}

            Allowed write globs:
            {json.dumps(manifest.allowed_write_globs, indent=2, ensure_ascii=True)}

            Target paths:
            {json.dumps(manifest.target_paths, indent=2, ensure_ascii=True)}

            Success criteria:
            {json.dumps(manifest.success_criteria, indent=2, ensure_ascii=True)}

            Read-only toolbelt:
            {toolbelt}

            Machine-readable manifest:
            ```json
            {manifest_json}
            ```

            Final response contract:
            - Return only JSON matching the schema below.
            - Do not wrap the JSON in markdown fences.

            ```json
            {schema_json}
            ```
            """
        ).strip() + "\n"

    def _toolbelt_instructions(self) -> str:
        python_binary = Path(sys.executable)
        backend_dir = BACKEND_ROOT
        return dedent(
            f"""
            Use these read-only helpers when they reduce context stuffing:
            - `cd {backend_dir} && {python_binary} -m app.tasks.jobs vault-search-inline --query "<text>"`
            - `cd {backend_dir} && {python_binary} -m app.tasks.jobs vault-insights-inline --query "<text>"`
            - `cd {backend_dir} && {python_binary} -m app.tasks.jobs vault-show-doc-inline --doc-id <doc-id>`
            - `cd {backend_dir} && {python_binary} -m app.tasks.jobs vault-related-inline --doc-id <doc-id>`
            - `cd {backend_dir} && {python_binary} -m app.tasks.jobs vault-list-stale-inline`
            """
        ).strip()

    def _resolve_codex_binary(self) -> Path | None:
        candidate = Path(self.settings.codex_binary)
        if candidate.is_absolute() and candidate.exists():
            return candidate
        resolved = shutil.which(self.settings.codex_binary)
        return Path(resolved) if resolved else None

    def _ensure_codex_ready(self) -> dict[str, object]:
        status = self.codex_status()
        if not bool(status.get("available")):
            raise CodexEnrichmentError(str(status.get("detail") or "Codex CLI is unavailable."))
        if not bool(status.get("authenticated")):
            raise CodexEnrichmentError(str(status.get("detail") or "Codex CLI is not authenticated."))
        return status

    def _acquire_lease(self, name: str):
        try:
            return self.store.acquire_lease(name=name, owner="mac", ttl_seconds=self.settings.codex_timeout_minutes * 60)
        except LeaseBusyError as exc:
            raise CodexEnrichmentError(str(exc)) from exc

    def _load_compile_state(self) -> CompileState:
        return self.store._load_json_model(self.compile_state_path, CompileState, default=CompileState())  # noqa: SLF001

    def _save_compile_state(self, state: CompileState) -> None:
        self.store.write_json(self.compile_state_path, state)

    def _select_compile_documents(
        self,
        *,
        source_id: str | None,
        doc_id: str | None,
        limit: int | None,
        stale_only: bool = False,
    ) -> list[RawDocument]:
        documents = self.store.list_raw_documents()
        compile_state = self._load_compile_state()
        if source_id:
            documents = [document for document in documents if document.frontmatter.source_id == source_id]
        if doc_id:
            documents = [document for document in documents if document.frontmatter.id == doc_id]

        stale_documents: list[RawDocument] = []
        for document in documents:
            state_entry = compile_state.documents.get(document.frontmatter.id)
            is_stale = state_entry is None or state_entry.last_compiled_content_hash != document.frontmatter.content_hash
            if stale_only and not is_stale:
                continue
            if doc_id or source_id:
                stale_documents.append(document)
                continue
            if is_stale:
                stale_documents.append(document)

        stale_documents.sort(
            key=lambda document: (
                document.frontmatter.published_at or document.frontmatter.fetched_at or document.frontmatter.ingested_at,
                document.frontmatter.title.lower(),
            ),
            reverse=True,
        )
        if doc_id:
            return stale_documents[:1]
        effective_limit = limit or self.settings.codex_compile_batch_size
        return stale_documents[:effective_limit]

    def _update_compile_state(self, *, documents: list[RawDocument], wiki_paths: list[str], run_id: str) -> None:
        state = self._load_compile_state()
        compiled_at = utcnow()
        deduped_paths = list(dict.fromkeys(wiki_paths))
        for document in documents:
            state.documents[document.frontmatter.id] = CompileStateEntry(
                doc_id=document.frontmatter.id,
                last_compiled_content_hash=document.frontmatter.content_hash,
                last_compile_run_id=run_id,
                affected_wiki_pages=deduped_paths,
                compiled_at=compiled_at,
            )
        self._save_compile_state(state)

    def _seed_system_wiki_context(self, run, *, trigger: str):
        step = self.runs.start_step(run, step_kind="system_wiki_seed")
        try:
            pages = self.wiki.compile(trigger=f"{trigger}:system_wiki")
            self.runs.log_step(run, step, f"Refreshed {len(pages.pages)} system wiki pages.", level="success")
            self.runs.finish_step(
                run,
                step,
                status=RunStatus.SUCCEEDED,
                created_count=len(pages.pages),
            )
            return pages
        except Exception as exc:
            step.errors.append(str(exc))
            self.runs.finish_step(run, step, status=RunStatus.FAILED)
            raise

    def _rebuild_wiki_indexes(self, run) -> tuple:
        step = self.runs.start_step(run, step_kind="wiki_index")
        pages, graph = self.wiki_index.rebuild()
        self.runs.log_step(run, step, f"Rebuilt wiki indexes for {len(pages.pages)} pages.", level="success")
        self.runs.finish_step(
            run,
            step,
            status=RunStatus.SUCCEEDED,
            created_count=len(pages.pages),
            counts_by_kind={"graph-edge": len(graph.edges)},
        )
        return pages, graph

    def _load_final_summary(self, final_path: Path) -> CodexEnrichmentSummary:
        if not final_path.exists():
            raise CodexEnrichmentError(f"Codex did not write a final response file at {final_path}.")
        raw_text = final_path.read_text(encoding="utf-8").strip()
        if not raw_text:
            raise CodexEnrichmentError("Codex final response was empty.")
        candidates = [raw_text]
        first_brace = raw_text.find("{")
        last_brace = raw_text.rfind("}")
        if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
            candidates.append(raw_text[first_brace : last_brace + 1])
        for candidate in candidates:
            try:
                return CodexEnrichmentSummary.model_validate_json(candidate)
            except Exception:
                continue
        raise CodexEnrichmentError("Codex final response did not match the required JSON summary schema.")

    @classmethod
    def _strict_json_schema(cls, node: Any) -> Any:
        if isinstance(node, dict):
            normalized = {key: cls._strict_json_schema(value) for key, value in node.items()}
            if normalized.get("type") == "object" or "properties" in normalized:
                normalized["additionalProperties"] = False
            return normalized
        if isinstance(node, list):
            return [cls._strict_json_schema(item) for item in node]
        return node

    def _event_failure_message(self, events_path: Path) -> str | None:
        if not events_path.exists():
            return None

        failure_message = None
        for raw_line in events_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            event_type = event.get("type")
            if event_type == "error":
                failure_message = self._normalize_event_error(event.get("message")) or failure_message
            elif event_type == "turn.failed":
                failure_message = self._normalize_event_error(event.get("error")) or failure_message

        return failure_message

    @classmethod
    def _normalize_event_error(cls, payload: Any) -> str | None:
        if payload is None:
            return None
        if isinstance(payload, str):
            text = payload.strip()
            if not text:
                return None
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return " ".join(text.split())
            return cls._normalize_event_error(parsed) or " ".join(text.split())
        if isinstance(payload, dict):
            for key in ("error", "message", "detail"):
                normalized = cls._normalize_event_error(payload.get(key))
                if normalized:
                    return normalized
        return None

    def _validate_allowed_changes(self, changed_files: list[str], allowed_globs: list[str]) -> None:
        disallowed = [
            path
            for path in changed_files
            if not any(PurePosixPath(path).match(pattern) for pattern in allowed_globs)
        ]
        if disallowed:
            raise CodexEnrichmentError(
                "Codex modified files outside the allowed write scope: "
                + ", ".join(disallowed[:10])
            )

    def _snapshot_paths(self, patterns: list[str]) -> dict[str, str]:
        snapshot: dict[str, str] = {}
        for pattern in patterns:
            if pattern.endswith("/**"):
                base = (self.store.root / pattern[:-3]).resolve()
                if base.exists():
                    for path in base.rglob("*"):
                        if path.is_file():
                            snapshot[str(path.relative_to(self.store.root))] = self._hash_file(path)
                continue
            for path in self.store.root.glob(pattern):
                if path.is_file():
                    snapshot[str(path.relative_to(self.store.root))] = self._hash_file(path)
        return snapshot

    @staticmethod
    def _changed_paths(before: dict[str, str], after: dict[str, str]) -> list[str]:
        changed = {
            path
            for path, digest in after.items()
            if before.get(path) != digest
        }
        changed.update(path for path in before if path not in after)
        return sorted(changed)

    @staticmethod
    def _hash_file(path: Path) -> str:
        digest = hashlib.sha256()
        digest.update(path.read_bytes())
        return digest.hexdigest()

    @staticmethod
    def _stderr_excerpt(path: Path, *, lines: int = 12) -> str | None:
        if not path.exists():
            return None
        content = [line.strip() for line in path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
        if not content:
            return None
        return " ".join(content[-lines:])

    def _validate_vault_relative_path(self, path: str) -> Path:
        candidate = (self.store.root / path).resolve()
        try:
            candidate.relative_to(self.store.root.resolve())
        except ValueError as exc:
            raise CodexEnrichmentError("Path must stay inside the vault root.") from exc
        return candidate
