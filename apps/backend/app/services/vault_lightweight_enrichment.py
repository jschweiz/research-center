from __future__ import annotations

import json
import math
import re
import unicodedata
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from app.core.logging import bind_log_context, reset_log_context
from app.db.models import IngestionRunType, RunStatus
from app.integrations.llm import LLMClient
from app.schemas.ops import IngestionRunHistoryRead, OperationBasicInfoRead, OperationLogRead
from app.services.profile import load_profile_snapshot
from app.services.text import normalize_whitespace
from app.services.vault_runtime import RunRecorder, content_hash, utcnow
from app.vault.models import LightweightJudgeScore, RawDocument, RawDocumentFrontmatter
from app.vault.store import LeaseBusyError, VaultStore

TLDR_SECTION_RE = re.compile(r"^##\s+(?P<title>.+?)\s*$")
TLDR_STORY_RE = re.compile(r"^###\s+(?:\[(?P<link_title>.+?)\]\([^)]+\)|(?P<plain_title>.+?))\s*$")
TLDR_TITLE_RE = re.compile(r"^#\s+(?P<title>.+?)\s*$")
TLDR_QUICK_LINK_SECTIONS = {"quick links"}
LIGHTWEIGHT_ENRICHMENT_PARALLELISM = 4
LIGHTWEIGHT_ENRICHMENT_LEASE_NAME = "lightweight-enrichment"
LIGHTWEIGHT_ENRICHMENT_LEASE_TTL_SECONDS = 900
ALPHAXIV_SOURCE_ID = "alphaxiv-paper"
ALPHAXIV_METADATA_FILENAME = "alphaxiv-metadata.json"
ALPHAXIV_METRIC_MODEL_SUFFIX = "alphaxiv-metrics-v1"


class LightweightEnrichmentCancelledError(RuntimeError):
    def __init__(self, message: str, *, run_id: str) -> None:
        super().__init__(message)
        self.run_id = run_id


class VaultLightweightEnrichmentService:
    def __init__(self) -> None:
        self.store = VaultStore()
        self.llm = LLMClient()
        self.runs = RunRecorder(self.store)
        self.store.ensure_layout()

    def ollama_status(self) -> dict[str, object]:
        return self.llm.ollama_status()

    def list_stale_documents(
        self,
        *,
        source_id: str | None = None,
        doc_id: str | None = None,
        documents: list[RawDocument] | None = None,
    ) -> list[RawDocument]:
        candidates = self._list_target_documents(
            source_id=source_id, doc_id=doc_id, documents=documents
        )
        profile_context = self._profile_context()
        source_lookup = self._source_lookup()
        return [
            document
            for document in candidates
            if self._should_process_document(
                document,
                profile_context=profile_context,
                source_lookup=source_lookup,
            )
        ]

    def list_pending_documents(
        self,
        *,
        source_id: str | None = None,
        doc_id: str | None = None,
        documents: list[RawDocument] | None = None,
    ) -> list[RawDocument]:
        candidates = self._list_target_documents(
            source_id=source_id,
            doc_id=doc_id,
            documents=documents,
        )
        return [
            document
            for document in candidates
            if document.frontmatter.lightweight_enriched_at is None
        ]

    def count_pending_documents(
        self,
        *,
        source_id: str | None = None,
        doc_id: str | None = None,
        documents: list[RawDocument] | None = None,
    ) -> int:
        return len(
            self.list_pending_documents(
                source_id=source_id,
                doc_id=doc_id,
                documents=documents,
            )
        )

    def count_stale_documents(
        self,
        *,
        source_id: str | None = None,
        doc_id: str | None = None,
        documents: list[RawDocument] | None = None,
    ) -> int:
        return len(
            self.list_stale_documents(
                source_id=source_id,
                doc_id=doc_id,
                documents=documents,
            )
        )

    def request_stop_for_run(
        self,
        *,
        trigger: str | None = None,
    ) -> IngestionRunHistoryRead:
        run = self.latest_run(trigger=trigger, live_only=True)
        if run is None:
            raise RuntimeError("No running lightweight enrichment exists.")

        if not self.store.is_operation_stop_requested(run.id):
            self.store.request_operation_stop(
                run_id=run.id,
                requested_by="local-control",
            )
        return self._load_run_record(run.id) or run

    def latest_run(
        self,
        *,
        trigger: str | None = None,
        live_only: bool = False,
    ) -> IngestionRunHistoryRead | None:
        records = self.store.load_run_records()
        for payload in reversed(records):
            if payload.get("operation_kind") != "lightweight_enrichment":
                continue
            if trigger is not None and payload.get("trigger") != trigger:
                continue
            if live_only and payload.get("status") not in {RunStatus.RUNNING, RunStatus.PENDING}:
                continue
            return IngestionRunHistoryRead.model_validate(payload)
        return None

    def _list_other_live_runs(
        self,
        *,
        exclude_run_id: str,
    ) -> list[IngestionRunHistoryRead]:
        records = self.store.load_run_records()
        runs: list[IngestionRunHistoryRead] = []
        for payload in reversed(records):
            if payload.get("operation_kind") != "lightweight_enrichment":
                continue
            if payload.get("id") == exclude_run_id:
                continue
            if payload.get("status") not in {RunStatus.RUNNING, RunStatus.PENDING}:
                continue
            runs.append(IngestionRunHistoryRead.model_validate(payload))
        return runs

    def _interrupt_stale_live_runs(
        self,
        *,
        current_run: IngestionRunHistoryRead,
    ) -> list[IngestionRunHistoryRead]:
        interrupted_runs: list[IngestionRunHistoryRead] = []
        for stale_run in self._list_other_live_runs(exclude_run_id=current_run.id):
            interrupted_runs.append(
                self._interrupt_live_run(
                    stale_run,
                    replacement_run_id=current_run.id,
                )
            )
        if interrupted_runs:
            label = f"{len(interrupted_runs)} stale lightweight run"
            if len(interrupted_runs) != 1:
                label += "s"
            self.runs.log(
                current_run,
                f"Interrupted {label} that were still marked live after their lease expired.",
                level="warning",
            )
        return interrupted_runs

    def _interrupt_live_run(
        self,
        run: IngestionRunHistoryRead,
        *,
        replacement_run_id: str | None,
    ) -> IngestionRunHistoryRead:
        if run.status not in {RunStatus.RUNNING, RunStatus.PENDING}:
            return run

        finished_at = utcnow()
        if replacement_run_id:
            message = (
                "Lightweight enrichment was interrupted after its lease expired and a newer "
                f"run took over ({replacement_run_id})."
            )
            summary = (
                "Lightweight enrichment was interrupted after its lease expired and a newer "
                "run took over."
            )
            self._upsert_basic_info(run, label="Interrupted by", value=replacement_run_id)
        else:
            message = (
                "Lightweight enrichment was interrupted during manual cleanup of stale live runs."
            )
            summary = (
                "Lightweight enrichment was interrupted during manual cleanup of stale live runs."
            )
            self._upsert_basic_info(run, label="Interrupted by", value="manual cleanup")

        run.status = RunStatus.INTERRUPTED
        run.summary = summary
        run.finished_at = finished_at
        if run.duration_seconds is None:
            run.duration_seconds = round((finished_at - run.started_at).total_seconds(), 2)
        if message not in run.errors:
            run.errors.append(message)
        run.logs.append(
            OperationLogRead(
                logged_at=finished_at,
                level="warning",
                message=message,
            )
        )
        for step in run.steps:
            if step.status not in {RunStatus.RUNNING, RunStatus.PENDING}:
                continue
            step.status = RunStatus.INTERRUPTED
            step.finished_at = finished_at
            if message not in step.errors:
                step.errors.append(message)
            step.logs.append(
                OperationLogRead(
                    logged_at=finished_at,
                    level="warning",
                    message=message,
                )
            )
        self.store.upsert_run_record(run.model_dump(mode="json"))
        return run

    def _renew_lightweight_lease(
        self,
        *,
        run: IngestionRunHistoryRead,
        lease,
    ) -> None:
        if lease is None:
            return
        try:
            self.store.renew_lease(
                lease,
                ttl_seconds=LIGHTWEIGHT_ENRICHMENT_LEASE_TTL_SECONDS,
            )
        except LeaseBusyError as exc:
            raise LightweightEnrichmentCancelledError(
                "Lightweight enrichment lost its lease while another run was taking over.",
                run_id=run.id,
            ) from exc

    def _acquire_lightweight_lease(
        self,
        *,
        run: IngestionRunHistoryRead,
    ):
        try:
            return self.store.acquire_lease(
                name=LIGHTWEIGHT_ENRICHMENT_LEASE_NAME,
                owner="mac",
                ttl_seconds=LIGHTWEIGHT_ENRICHMENT_LEASE_TTL_SECONDS,
            )
        except LeaseBusyError:
            conflicting_runs = self._list_other_live_runs(exclude_run_id=run.id)
            if conflicting_runs:
                raise
            self.store.clear_lease(name=LIGHTWEIGHT_ENRICHMENT_LEASE_NAME)
            self.runs.log(
                run,
                "Recovered a stale lightweight-enrichment lease left behind by an interrupted run.",
                level="warning",
            )
            return self.store.acquire_lease(
                name=LIGHTWEIGHT_ENRICHMENT_LEASE_NAME,
                owner="mac",
                ttl_seconds=LIGHTWEIGHT_ENRICHMENT_LEASE_TTL_SECONDS,
            )

    def enrich_stale_documents(
        self,
        *,
        trigger: str = "manual_lightweight_enrich",
        source_id: str | None = None,
        doc_id: str | None = None,
        force: bool = False,
    ) -> IngestionRunHistoryRead:
        run = self.runs.start(
            run_type=IngestionRunType.INGEST,
            operation_kind="lightweight_enrichment",
            trigger=trigger,
            title="Lightweight enrichment",
            summary="Refreshing small per-document metadata with the local Ollama model.",
        )
        run.basic_info.append(
            OperationBasicInfoRead(label="Force refresh", value="yes" if force else "no")
        )
        if source_id:
            run.basic_info.append(OperationBasicInfoRead(label="Source filter", value=source_id))
        if doc_id:
            run.basic_info.append(OperationBasicInfoRead(label="Document filter", value=doc_id))
        self.runs.log(
            run,
            self._run_start_message(source_id=source_id, doc_id=doc_id, force=force),
        )
        lease = None
        step = None
        documents: list[RawDocument] = []
        try:
            try:
                lease = self._acquire_lightweight_lease(run=run)
                self._renew_lightweight_lease(run=run, lease=lease)
                self._interrupt_stale_live_runs(current_run=run)
            except LeaseBusyError as exc:
                run.errors.append(str(exc))
                return self.runs.finish(
                    run,
                    status=RunStatus.FAILED,
                    summary="Lightweight enrichment skipped because another enrichment run is already active.",
                )

            step = self.runs.start_step(
                run,
                step_kind="lightweight_enrichment",
                source_id=source_id,
                doc_id=doc_id,
            )
            documents = self._list_target_documents(source_id=source_id, doc_id=doc_id)
            self.runs.log(
                run,
                f"Loaded {len(documents)} raw document{'s' if len(documents) != 1 else ''} for evaluation.",
            )
            self.runs.log_step(
                run,
                step,
                f"Loaded {len(documents)} candidate raw document{'s' if len(documents) != 1 else ''}.",
            )
            profile_context = self._profile_context()
            source_lookup = self._source_lookup()

            updated_count = 0
            skipped_count = 0
            failed_count = 0
            target_count = 0
            ollama_status: dict[str, object] | None = None
            ollama_status_recorded = False
            candidates: list[dict[str, Any]] = []

            try:
                self._raise_if_stop_requested(run=run)
                for document in documents:
                    self._raise_if_stop_requested(run=run)
                    self._renew_lightweight_lease(run=run, lease=lease)
                    fm = document.frontmatter
                    metadata_input_hash = self._enrichment_input_hash(document)
                    metadata_needs_refresh = self._should_refresh_metadata(
                        document,
                        force=force,
                        enrichment_input_hash=metadata_input_hash,
                    )
                    existing_metadata_payload = self._metadata_payload_from_frontmatter(fm)
                    source_context = self._source_context_for_document(
                        document, source_lookup=source_lookup
                    )
                    scoring_input_hash = self._scoring_input_hash(
                        document=document,
                        metadata_payload=existing_metadata_payload,
                        profile_context=profile_context,
                        source_context=source_context,
                    )
                    scoring_needs_refresh = self._should_refresh_scoring(
                        document,
                        force=force,
                        scoring_input_hash=scoring_input_hash,
                    )
                    if not metadata_needs_refresh and not scoring_needs_refresh:
                        skipped_count += 1
                        self.runs.log_step(
                            run,
                            step,
                            f"Skipped {self._document_ref(fm)} because lightweight metadata and score are current.",
                        )
                        continue

                    deterministic_metadata_payload = self._deterministic_lightweight_payload(
                        document
                    )
                    candidates.append(
                        {
                            "document": document,
                            "frontmatter": fm,
                            "metadata_input_hash": metadata_input_hash,
                            "metadata_needs_refresh": metadata_needs_refresh,
                            "existing_metadata_payload": existing_metadata_payload,
                            "metadata_payload": deterministic_metadata_payload,
                            "scoring_needs_refresh": scoring_needs_refresh,
                            "source_context": source_context,
                            "scoring_input_hash": scoring_input_hash,
                        }
                    )
                    self.runs.log_step(
                        run,
                        step,
                        f"Queued {self._document_ref(fm)} for {self._refresh_plan_label(candidate=candidates[-1])}.",
                    )

                target_count = len(candidates)
                self._sync_live_progress(
                    run=run,
                    step=step,
                    scanned_count=len(documents),
                    target_count=target_count,
                    updated_count=updated_count,
                    skipped_count=skipped_count,
                    failed_count=failed_count,
                )
                self.runs.log(
                    run,
                    f"Scanned {len(documents)} raw document{'s' if len(documents) != 1 else ''}: "
                    f"{target_count} queued for refresh and {skipped_count} already current.",
                )
                if not candidates:
                    self.runs.log(
                        run,
                        "No documents required lightweight metadata or scoring refresh.",
                    )

                self._raise_if_stop_requested(run=run)
                ollama_requested = bool(
                    [
                        candidate
                        for candidate in candidates
                        if (
                            (
                                candidate["metadata_needs_refresh"]
                                and candidate["metadata_payload"] is None
                            )
                            or candidate["scoring_needs_refresh"]
                        )
                    ]
                )
                if ollama_requested:
                    ollama_status = self.ollama_status()
                    timeout_seconds = int(self.llm.settings.ollama_timeout_seconds)
                    run.basic_info.extend(
                        [
                            OperationBasicInfoRead(
                                label="Model", value=str(ollama_status.get("model") or "unknown")
                            ),
                            OperationBasicInfoRead(
                                label="Available",
                                value="yes" if bool(ollama_status.get("available")) else "no",
                            ),
                            OperationBasicInfoRead(
                                label="Parallelism", value=str(self._ollama_parallelism())
                            ),
                            OperationBasicInfoRead(
                                label="Ollama timeout", value=f"{timeout_seconds}s"
                            ),
                        ]
                    )
                    self.runs.log(
                        run,
                        f"Ollama status: {'ready' if bool(ollama_status.get('available')) else 'unavailable'} "
                        f"for model {ollama_status.get('model') or 'unknown'} "
                        f"(timeout {timeout_seconds}s, parallelism {self._ollama_parallelism()}).",
                        level="success" if bool(ollama_status.get("available")) else "warning",
                    )
                    ollama_status_recorded = True

                metadata_requests = [
                    candidate
                    for candidate in candidates
                    if candidate["metadata_needs_refresh"] and candidate["metadata_payload"] is None
                ]
                if metadata_requests:
                    self._raise_if_stop_requested(run=run)
                    if not bool((ollama_status or {}).get("available")):
                        message = str(
                            (ollama_status or {}).get("detail") or "Ollama is unavailable."
                        )
                        self.runs.log(
                            run,
                            f"Metadata phase cannot start for {len(metadata_requests)} document"
                            f"{'' if len(metadata_requests) == 1 else 's'}: {message}",
                            level="error",
                        )
                        for candidate in metadata_requests:
                            candidate["metadata_failed"] = True
                            candidate["metadata_error"] = message
                            failed_count += 1
                            run.errors.append(message)
                    else:
                        metadata_results = self._run_parallel_llm_requests(
                            run=run,
                            lease=lease,
                            step=step,
                            phase_label="Metadata",
                            requests=metadata_requests,
                            worker=self._call_lightweight_metadata,
                            artifact="lightweight_metadata",
                        )
                        self._raise_if_stop_requested(run=run)
                        for candidate in metadata_requests:
                            fm = candidate["frontmatter"]
                            outcome = metadata_results.get(fm.id)
                            if outcome is None:
                                candidate["metadata_failed"] = True
                                candidate["metadata_error"] = (
                                    "Ollama lightweight enrichment returned no result."
                                )
                                failed_count += 1
                                run.errors.append(str(candidate["metadata_error"]))
                                continue
                            trace_payload = outcome.get("trace")
                            if isinstance(trace_payload, dict):
                                self.runs.record_ai_trace(run, trace_payload)
                            error = outcome.get("error")
                            if isinstance(error, Exception):
                                candidate["metadata_failed"] = True
                                candidate["metadata_error"] = str(error)
                                failed_count += 1
                                run.errors.append(str(error))
                                continue
                            candidate["metadata_payload"] = outcome.get("payload")
                        metadata_failed_count = sum(
                            1 for candidate in metadata_requests if candidate.get("metadata_failed")
                        )
                        self.runs.log(
                            run,
                            f"Metadata phase completed for {len(metadata_requests)} document"
                            f"{'' if len(metadata_requests) == 1 else 's'}: "
                            f"{len(metadata_requests) - metadata_failed_count} succeeded, "
                            f"{metadata_failed_count} failed.",
                            level="warning" if metadata_failed_count else "success",
                        )
                elif target_count:
                    self.runs.log(
                        run,
                        "Metadata phase skipped: every queued document already had reusable metadata.",
                    )

                self._raise_if_stop_requested(run=run)
                for candidate in candidates:
                    self._raise_if_stop_requested(run=run)
                    if candidate.get("metadata_failed"):
                        continue
                    fm = candidate["frontmatter"]
                    document = candidate["document"]
                    metadata_payload = (
                        candidate["metadata_payload"] or candidate["existing_metadata_payload"]
                    )
                    normalized_authors = (
                        self._normalize_string_list(metadata_payload.get("authors")) or fm.authors
                    )
                    normalized_tags = (
                        self._normalize_string_list(metadata_payload.get("tags")) or fm.tags
                    )
                    normalized_summary = (
                        self._normalize_optional_string(metadata_payload.get("short_summary"))
                        or fm.short_summary
                    )
                    metadata_payload = {
                        **metadata_payload,
                        "authors": normalized_authors,
                        "tags": normalized_tags,
                        "short_summary": normalized_summary,
                    }
                    candidate["metadata_payload"] = metadata_payload
                    candidate["normalized_authors"] = normalized_authors
                    candidate["normalized_tags"] = normalized_tags
                    candidate["normalized_summary"] = normalized_summary
                    candidate["scoring_input_hash"] = self._scoring_input_hash(
                        document=document,
                        metadata_payload=metadata_payload,
                        profile_context=profile_context,
                        source_context=candidate["source_context"],
                    )
                    candidate["scoring_needs_refresh"] = self._should_refresh_scoring(
                        document,
                        force=force,
                        scoring_input_hash=candidate["scoring_input_hash"],
                    )
                    if not candidate["scoring_needs_refresh"]:
                        candidate["score_payload"] = self._score_payload_from_frontmatter(fm)

                scoring_requests = [
                    candidate
                    for candidate in candidates
                    if not candidate.get("metadata_failed") and candidate["scoring_needs_refresh"]
                ]
                if scoring_requests:
                    self._raise_if_stop_requested(run=run)
                    if not bool((ollama_status or {}).get("available")):
                        message = str(
                            (ollama_status or {}).get("detail") or "Ollama is unavailable."
                        )
                        self.runs.log(
                            run,
                            f"Scoring phase is using heuristic fallback for {len(scoring_requests)} document"
                            f"{'' if len(scoring_requests) == 1 else 's'} because {message}",
                            level="warning",
                        )
                        for candidate in scoring_requests:
                            candidate["score_payload"] = self._heuristic_score_payload(
                                document=candidate["document"],
                                metadata_payload=candidate["metadata_payload"],
                                profile_context=profile_context,
                                source_context=candidate["source_context"],
                            )
                            candidate["score_payload"] = self._apply_source_scoring_priors(
                                score_payload=candidate["score_payload"],
                                source_context=candidate["source_context"],
                            )
                            candidate["score_fallback_reason"] = message
                    else:
                        score_results = self._run_parallel_llm_requests(
                            run=run,
                            lease=lease,
                            step=step,
                            phase_label="Scoring",
                            requests=scoring_requests,
                            worker=lambda llm, candidate: self._call_lightweight_score(
                                llm,
                                candidate,
                                profile_context=profile_context,
                            ),
                            artifact="lightweight_score",
                        )
                        self._raise_if_stop_requested(run=run)
                        for candidate in scoring_requests:
                            fm = candidate["frontmatter"]
                            outcome = score_results.get(fm.id)
                            if outcome is None:
                                error = RuntimeError(
                                    "Ollama lightweight scoring returned no result."
                                )
                                candidate["score_payload"] = self._heuristic_score_payload(
                                    document=candidate["document"],
                                    metadata_payload=candidate["metadata_payload"],
                                    profile_context=profile_context,
                                    source_context=candidate["source_context"],
                                )
                                candidate["score_payload"] = self._apply_source_scoring_priors(
                                    score_payload=candidate["score_payload"],
                                    source_context=candidate["source_context"],
                                )
                                candidate["score_fallback_reason"] = str(error)
                                continue
                            trace_payload = outcome.get("trace")
                            if isinstance(trace_payload, dict):
                                self.runs.record_ai_trace(run, trace_payload)
                            error = outcome.get("error")
                            if isinstance(error, Exception):
                                candidate["score_payload"] = self._heuristic_score_payload(
                                    document=candidate["document"],
                                    metadata_payload=candidate["metadata_payload"],
                                    profile_context=profile_context,
                                    source_context=candidate["source_context"],
                                )
                                candidate["score_payload"] = self._apply_source_scoring_priors(
                                    score_payload=candidate["score_payload"],
                                    source_context=candidate["source_context"],
                                )
                                candidate["score_fallback_reason"] = str(error)
                                continue
                            if isinstance(outcome.get("payload"), dict):
                                candidate["score_payload"] = self._apply_source_scoring_priors(
                                    score_payload=outcome["payload"],
                                    source_context=candidate["source_context"],
                                )
                            else:
                                candidate["score_payload"] = outcome.get("payload")
                        score_fallback_count = sum(
                            1
                            for candidate in scoring_requests
                            if candidate.get("score_fallback_reason")
                        )
                        self.runs.log(
                            run,
                            f"Scoring phase completed for {len(scoring_requests)} document"
                            f"{'' if len(scoring_requests) == 1 else 's'}: "
                            f"{len(scoring_requests) - score_fallback_count} Ollama score"
                            f"{'' if len(scoring_requests) - score_fallback_count == 1 else 's'} and "
                            f"{score_fallback_count} heuristic fallback"
                            f"{'' if score_fallback_count == 1 else 's'}.",
                            level="warning" if score_fallback_count else "success",
                        )
                elif target_count:
                    self.runs.log(
                        run,
                        "Scoring phase skipped: every queued document already had a current lightweight score.",
                    )

                self._raise_if_stop_requested(run=run)
                if target_count:
                    self.runs.log(
                        run,
                        f"Persisting lightweight enrichment results for {target_count} queued document"
                        f"{'' if target_count == 1 else 's'}.",
                    )
                for candidate in candidates:
                    self._raise_if_stop_requested(run=run)
                    self._renew_lightweight_lease(run=run, lease=lease)
                    fm = candidate["frontmatter"]
                    document = candidate["document"]
                    if candidate.get("metadata_failed"):
                        updated_frontmatter = fm.model_copy(
                            update={
                                "lightweight_enrichment_status": "failed",
                                "lightweight_enrichment_error": str(
                                    candidate.get("metadata_error")
                                    or "Lightweight enrichment failed."
                                ),
                                "lightweight_enrichment_model": str(
                                    (ollama_status or {}).get("model") or ""
                                ),
                                "lightweight_enrichment_input_hash": candidate[
                                    "metadata_input_hash"
                                ],
                            }
                        )
                        self.store.write_raw_document(
                            kind=updated_frontmatter.kind,
                            doc_id=updated_frontmatter.id,
                            frontmatter=updated_frontmatter,
                            body=document.body,
                        )
                        self._sync_live_progress(
                            run=run,
                            step=step,
                            scanned_count=len(documents),
                            target_count=target_count,
                            updated_count=updated_count,
                            skipped_count=skipped_count,
                            failed_count=failed_count,
                        )
                        self.runs.log_step(
                            run,
                            step,
                            f"{self._document_ref(fm)}: {candidate['metadata_error']}",
                            level="error",
                        )
                        self._log_document_progress(
                            run=run,
                            target_count=target_count,
                            updated_count=updated_count,
                            skipped_count=skipped_count,
                            failed_count=failed_count,
                            document_label=self._document_ref(fm),
                            outcome="failed",
                            level="warning",
                        )
                        continue

                    score_payload = candidate.get(
                        "score_payload"
                    ) or self._score_payload_from_frontmatter(fm)
                    if candidate.get("score_fallback_reason"):
                        self.runs.log_step(
                            run,
                            step,
                            f"{self._document_ref(fm)}: lightweight score fell back to heuristics because "
                            f"{candidate['score_fallback_reason']}",
                            level="warning",
                        )
                    updated_frontmatter = fm.model_copy(
                        update={
                            "authors": candidate["normalized_authors"],
                            "tags": candidate["normalized_tags"],
                            "short_summary": candidate["normalized_summary"],
                            "lightweight_enrichment_status": "succeeded",
                            "lightweight_enriched_at": utcnow(),
                            "lightweight_enrichment_model": candidate["metadata_payload"].get(
                                "model"
                            )
                            or fm.lightweight_enrichment_model
                            or str((ollama_status or {}).get("model") or ""),
                            "lightweight_enrichment_input_hash": candidate["metadata_input_hash"],
                            "lightweight_enrichment_error": None,
                            "lightweight_scoring_model": score_payload.get("model")
                            or fm.lightweight_scoring_model,
                            "lightweight_scoring_input_hash": candidate["scoring_input_hash"],
                            "lightweight_score": LightweightJudgeScore.model_validate(
                                {
                                    key: value
                                    for key, value in score_payload.items()
                                    if key
                                    in {
                                        "relevance_score",
                                        "source_fit_score",
                                        "topic_fit_score",
                                        "author_fit_score",
                                        "evidence_fit_score",
                                        "confidence_score",
                                        "bucket_hint",
                                        "reason",
                                        "evidence_quotes",
                                    }
                                }
                            ),
                        }
                    )
                    self.store.write_raw_document(
                        kind=updated_frontmatter.kind,
                        doc_id=updated_frontmatter.id,
                        frontmatter=updated_frontmatter,
                        body=document.body,
                    )
                    updated_count += 1
                    self._sync_live_progress(
                        run=run,
                        step=step,
                        scanned_count=len(documents),
                        target_count=target_count,
                        updated_count=updated_count,
                        skipped_count=skipped_count,
                        failed_count=failed_count,
                    )
                    self.runs.log_step(
                        run,
                        step,
                        f"Enriched {self._document_ref(fm)} with lightweight metadata and score "
                        f"{updated_frontmatter.lightweight_score.relevance_score:.2f}.",
                        level="success",
                    )
                    self._log_document_progress(
                        run=run,
                        target_count=target_count,
                        updated_count=updated_count,
                        skipped_count=skipped_count,
                        failed_count=failed_count,
                        document_label=self._document_ref(fm),
                        outcome="updated",
                    )

                if not ollama_status_recorded:
                    run.basic_info.extend(
                        [
                            OperationBasicInfoRead(label="Model", value="not used"),
                            OperationBasicInfoRead(label="Available", value="n/a"),
                        ]
                    )

                self.runs.finish_step(
                    run,
                    step,
                    status=RunStatus.SUCCEEDED if failed_count == 0 else RunStatus.FAILED,
                    updated_count=updated_count,
                    skipped_count=skipped_count,
                )
                self._sync_live_progress(
                    run=run,
                    step=step,
                    scanned_count=len(documents),
                    target_count=target_count,
                    updated_count=updated_count,
                    skipped_count=skipped_count,
                    failed_count=failed_count,
                )
                self.runs.log(
                    run,
                    f"Finished lightweight enrichment processing: updated {updated_count}, "
                    f"failed {failed_count}, skipped {skipped_count}.",
                    level="warning" if failed_count else "success",
                )
                summary = (
                    f"Lightweight enrichment updated {updated_count} document"
                    f"{'' if updated_count == 1 else 's'}."
                )
                if failed_count:
                    summary += f" {failed_count} document{'s' if failed_count != 1 else ''} failed."
                if skipped_count:
                    summary += f" {skipped_count} unchanged document{'s' if skipped_count != 1 else ''} were skipped."
                return self.runs.finish(
                    run,
                    status=RunStatus.SUCCEEDED if failed_count == 0 else RunStatus.FAILED,
                    summary=summary,
                )
            except LightweightEnrichmentCancelledError as exc:
                message = str(exc)
                run.errors.append(message)
                if not self._run_was_cancelled(run):
                    run.basic_info.append(
                        OperationBasicInfoRead(label="Canceled", value="local-control")
                    )
                self.runs.log(run, message, level="warning")
                self.runs.finish_step(
                    run,
                    step,
                    status=RunStatus.FAILED,
                    updated_count=updated_count,
                    skipped_count=skipped_count,
                )
                self._sync_live_progress(
                    run=run,
                    step=step,
                    scanned_count=len(documents),
                    target_count=target_count,
                    updated_count=updated_count,
                    skipped_count=skipped_count,
                    failed_count=failed_count,
                )
                summary = "Lightweight enrichment canceled from local-control."
                if updated_count:
                    summary = (
                        f"Lightweight enrichment canceled after updating {updated_count} document"
                        f"{'' if updated_count == 1 else 's'}."
                    )
                return self.runs.finish(
                    run,
                    status=RunStatus.FAILED,
                    summary=summary,
                )
        finally:
            self.store.clear_operation_stop_request(run.id)
            if lease is not None:
                self.store.release_lease(lease)

    @staticmethod
    def _ollama_parallelism() -> int:
        return LIGHTWEIGHT_ENRICHMENT_PARALLELISM

    @staticmethod
    def _upsert_basic_info(
        run: IngestionRunHistoryRead,
        *,
        label: str,
        value: str,
    ) -> None:
        for entry in run.basic_info:
            if entry.label == label:
                entry.value = value
                return
        run.basic_info.append(OperationBasicInfoRead(label=label, value=value))

    def _sync_live_progress(
        self,
        *,
        run: IngestionRunHistoryRead,
        step,
        scanned_count: int,
        target_count: int,
        updated_count: int,
        skipped_count: int,
        failed_count: int,
    ) -> None:
        run.total_titles = target_count
        run.updated_count = updated_count
        step.updated_count = updated_count
        step.skipped_count = skipped_count
        self._upsert_basic_info(run, label="Documents", value=str(target_count))
        self._upsert_basic_info(run, label="Documents scanned", value=str(scanned_count))
        self._upsert_basic_info(run, label="Updated", value=str(updated_count))
        self._upsert_basic_info(run, label="Skipped", value=str(skipped_count))
        self._upsert_basic_info(run, label="Failed", value=str(failed_count))

    @staticmethod
    def _run_start_message(
        *,
        source_id: str | None,
        doc_id: str | None,
        force: bool,
    ) -> str:
        scope = "all raw documents"
        if doc_id:
            scope = f"document {doc_id}"
        elif source_id:
            scope = f"source {source_id}"
        if force:
            return f"Starting lightweight enrichment for {scope} with force refresh enabled."
        return f"Starting lightweight enrichment for {scope}."

    @staticmethod
    def _document_ref(frontmatter: RawDocumentFrontmatter) -> str:
        title = normalize_whitespace(frontmatter.title or "")
        if len(title) > 72:
            title = title[:69].rstrip() + "..."
        return f"{frontmatter.id} ({title})" if title else frontmatter.id

    @staticmethod
    def _refresh_plan_label(*, candidate: dict[str, Any]) -> str:
        phases: list[str] = []
        if candidate.get("metadata_needs_refresh"):
            metadata_mode = (
                "deterministic metadata"
                if candidate.get("metadata_payload") is not None
                else "metadata"
            )
            phases.append(metadata_mode)
        if candidate.get("scoring_needs_refresh"):
            phases.append("score refresh")
        if not phases:
            return "no refresh work"
        if len(phases) == 1:
            return phases[0]
        return ", ".join(phases[:-1]) + f", and {phases[-1]}"

    @staticmethod
    def _should_emit_progress_log(*, completed_count: int, total_count: int) -> bool:
        if completed_count <= 0 or total_count <= 0:
            return False
        if completed_count == total_count or completed_count == 1:
            return True
        if total_count <= 8:
            return True
        if total_count <= 20:
            return completed_count % 2 == 0
        return completed_count % 5 == 0

    def _log_document_progress(
        self,
        *,
        run,
        target_count: int,
        updated_count: int,
        skipped_count: int,
        failed_count: int,
        document_label: str,
        outcome: str,
        level: str = "info",
    ) -> None:
        processed_count = updated_count + failed_count
        if not self._should_emit_progress_log(
            completed_count=processed_count,
            total_count=target_count,
        ):
            return
        self.runs.log(
            run,
            f"Enrichment progress {processed_count}/{target_count}: {outcome} {document_label}. "
            f"Updated {updated_count}, failed {failed_count}, skipped {skipped_count}.",
            level=level,
        )

    def _log_parallel_phase_progress(
        self,
        *,
        run,
        phase_label: str,
        completed_count: int,
        total_count: int,
        latest_message: str,
        level: str = "info",
    ) -> None:
        if not self._should_emit_progress_log(
            completed_count=completed_count,
            total_count=total_count,
        ):
            return
        self.runs.log(
            run,
            f"{phase_label} phase progress {completed_count}/{total_count}: {latest_message}",
            level=level,
        )

    def _run_parallel_llm_requests(
        self,
        *,
        run: IngestionRunHistoryRead,
        lease,
        step,
        phase_label: str,
        requests: list[dict[str, Any]],
        worker: Callable[[LLMClient, dict[str, Any]], dict[str, Any]],
        artifact: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        if not requests:
            return {}

        outcomes: dict[str, dict[str, Any]] = {}
        max_workers = min(self._ollama_parallelism(), len(requests))
        self.runs.log(
            run,
            f"{phase_label} phase starting for {len(requests)} document"
            f"{'' if len(requests) == 1 else 's'} with up to {max_workers} parallel Ollama request"
            f"{'' if max_workers == 1 else 's'}.",
        )
        self.runs.log_step(
            run,
            step,
            f"{phase_label} phase queued {len(requests)} document"
            f"{'' if len(requests) == 1 else 's'} for Ollama.",
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            request_lookup = {request["frontmatter"].id: request for request in requests}
            future_to_doc_id = {
                executor.submit(
                    self._execute_llm_request_with_log_context,
                    run=run,
                    request=request,
                    worker=worker,
                    artifact=artifact,
                ): request["frontmatter"].id
                for request in requests
            }
            completed_count = 0
            for future in as_completed(future_to_doc_id):
                doc_id = future_to_doc_id[future]
                completed_count += 1
                request = request_lookup[doc_id]
                document_label = self._document_ref(request["frontmatter"])
                self._renew_lightweight_lease(run=run, lease=lease)
                try:
                    payload = future.result()
                except Exception as exc:
                    trace_payload = getattr(exc, "ai_trace", None)
                    outcomes[doc_id] = {
                        "error": exc,
                        "trace": trace_payload if isinstance(trace_payload, dict) else None,
                    }
                    self.runs.log_step(
                        run,
                        step,
                        f"{phase_label} phase {completed_count}/{len(requests)} failed for {document_label}: {exc}",
                        level="error",
                    )
                    self._log_parallel_phase_progress(
                        run=run,
                        phase_label=phase_label,
                        completed_count=completed_count,
                        total_count=len(requests),
                        latest_message=f"latest failure for {document_label}",
                        level="warning",
                    )
                    continue
                trace_payload = payload.get("_trace") if isinstance(payload, dict) else None
                outcomes[doc_id] = {
                    "payload": payload,
                    "trace": trace_payload if isinstance(trace_payload, dict) else None,
                }
                self.runs.log_step(
                    run,
                    step,
                    f"{phase_label} phase {completed_count}/{len(requests)} completed for {document_label}.",
                )
                self._log_parallel_phase_progress(
                    run=run,
                    phase_label=phase_label,
                    completed_count=completed_count,
                    total_count=len(requests),
                    latest_message=f"latest completion for {document_label}",
                )
        self.runs.log(
            run,
            f"{phase_label} phase finished all {len(requests)} Ollama request"
            f"{'' if len(requests) == 1 else 's'}.",
        )
        return outcomes

    @staticmethod
    def _execute_llm_request_with_log_context(
        *,
        run: IngestionRunHistoryRead,
        request: dict[str, Any],
        worker: Callable[[LLMClient, dict[str, Any]], dict[str, Any]],
        artifact: str | None = None,
    ) -> dict[str, Any]:
        frontmatter = request["frontmatter"]
        token = bind_log_context(
            operation_run_id=run.id,
            operation_kind=run.operation_kind,
            source_id=frontmatter.source_id,
            doc_id=frontmatter.id,
            artifact=artifact,
        )
        try:
            if VaultStore().is_operation_stop_requested(run.id):
                raise LightweightEnrichmentCancelledError(
                    "Lightweight enrichment was canceled from local-control.",
                    run_id=run.id,
                )
            return worker(LLMClient(), request)
        finally:
            reset_log_context(token)

    @staticmethod
    def _call_lightweight_metadata(
        llm: LLMClient,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        frontmatter = request["frontmatter"]
        document = request["document"]
        return llm.lightweight_enrich_raw_document(
            {
                "title": frontmatter.title,
                "source_name": frontmatter.source_name,
                "source_id": frontmatter.source_id,
                "content_type": frontmatter.kind,
                "authors": frontmatter.authors,
                "tags": frontmatter.tags,
            },
            document.body,
        )

    @staticmethod
    def _call_lightweight_score(
        llm: LLMClient,
        request: dict[str, Any],
        *,
        profile_context: dict[str, Any],
    ) -> dict[str, Any]:
        frontmatter = request["frontmatter"]
        document = request["document"]
        return llm.judge_lightweight_document(
            {
                "title": frontmatter.title,
                "source_name": frontmatter.source_name,
                "source_id": frontmatter.source_id,
                "content_type": frontmatter.kind,
                "authors": request["normalized_authors"],
                "tags": request["normalized_tags"],
                "short_summary": request["normalized_summary"],
            },
            document.body,
            profile=profile_context,
            source_context=request["source_context"],
        )

    def _list_target_documents(
        self,
        *,
        source_id: str | None = None,
        doc_id: str | None = None,
        documents: list[RawDocument] | None = None,
    ) -> list[RawDocument]:
        candidates = list(documents) if documents is not None else self.store.list_raw_documents()
        if source_id:
            candidates = [
                document for document in candidates if document.frontmatter.source_id == source_id
            ]
        if doc_id:
            candidates = [document for document in candidates if document.frontmatter.id == doc_id]
        return candidates

    @staticmethod
    def _run_was_cancelled(run: IngestionRunHistoryRead) -> bool:
        return any(
            entry.label == "Canceled" and entry.value == "local-control" for entry in run.basic_info
        )

    def _load_run_record(self, run_id: str) -> IngestionRunHistoryRead | None:
        for payload in reversed(self.store.load_run_records()):
            if str(payload.get("id") or "").strip() == run_id:
                return IngestionRunHistoryRead.model_validate(payload)
        return None

    def _raise_if_stop_requested(
        self,
        *,
        run: IngestionRunHistoryRead,
    ) -> None:
        if not self.store.is_operation_stop_requested(run.id):
            return
        raise LightweightEnrichmentCancelledError(
            "Lightweight enrichment was canceled from local-control.",
            run_id=run.id,
        )

    def _metadata_pipeline_signature(self, frontmatter: RawDocumentFrontmatter) -> str:
        if frontmatter.kind == "newsletter" and frontmatter.source_id == "tldr-email":
            return "deterministic:tldr-newsletter:2026-04-08-v1"
        if frontmatter.kind == "newsletter" and frontmatter.source_id == "medium-email":
            return "deterministic:medium-newsletter:2026-04-08-v1"
        return self.llm.lightweight_enrichment_pipeline_signature()

    def _scoring_pipeline_signature(self) -> str:
        return self.llm.lightweight_scoring_pipeline_signature()

    def _enrichment_input_hash(self, document: RawDocument) -> str:
        payload = {
            "title": document.frontmatter.title,
            "body_hash": content_hash(document.frontmatter.title, document.body),
            "pipeline": self._metadata_pipeline_signature(document.frontmatter),
        }
        return content_hash(
            document.frontmatter.title,
            json.dumps(payload, sort_keys=True, ensure_ascii=True),
        )

    def _should_process_document(
        self,
        document: RawDocument,
        *,
        force: bool = False,
        profile_context: dict[str, Any],
        source_lookup: dict[str, dict[str, Any]],
    ) -> bool:
        metadata_input_hash = self._enrichment_input_hash(document)
        if self._should_refresh_metadata(
            document,
            force=force,
            enrichment_input_hash=metadata_input_hash,
        ):
            return True
        scoring_input_hash = self._scoring_input_hash(
            document=document,
            metadata_payload=self._metadata_payload_from_frontmatter(document.frontmatter),
            profile_context=profile_context,
            source_context=self._source_context_for_document(
                document, source_lookup=source_lookup
            ),
        )
        return self._should_refresh_scoring(
            document,
            force=force,
            scoring_input_hash=scoring_input_hash,
        )

    def _should_refresh_metadata(
        self,
        document: RawDocument,
        *,
        force: bool = False,
        enrichment_input_hash: str | None = None,
    ) -> bool:
        if force:
            return True
        fm = document.frontmatter
        next_input_hash = enrichment_input_hash or self._enrichment_input_hash(document)
        return (
            fm.lightweight_enrichment_input_hash != next_input_hash
            or fm.lightweight_enrichment_status != "succeeded"
        )

    def _should_refresh_scoring(
        self,
        document: RawDocument,
        *,
        force: bool = False,
        scoring_input_hash: str | None = None,
    ) -> bool:
        if force:
            return True
        fm = document.frontmatter
        if fm.lightweight_enrichment_status != "succeeded":
            return True
        next_input_hash = scoring_input_hash or ""
        return fm.lightweight_scoring_input_hash != next_input_hash or fm.lightweight_score is None

    def _metadata_payload_from_frontmatter(
        self,
        frontmatter: RawDocumentFrontmatter,
    ) -> dict[str, Any]:
        return {
            "short_summary": frontmatter.short_summary,
            "authors": list(frontmatter.authors),
            "tags": list(frontmatter.tags),
            "model": frontmatter.lightweight_enrichment_model,
        }

    def _score_payload_from_frontmatter(
        self,
        frontmatter: RawDocumentFrontmatter,
    ) -> dict[str, Any]:
        if frontmatter.lightweight_score is None:
            return {
                "relevance_score": 0.0,
                "source_fit_score": 0.0,
                "topic_fit_score": 0.0,
                "author_fit_score": 0.0,
                "evidence_fit_score": 0.0,
                "confidence_score": 0.0,
                "bucket_hint": "archive",
                "reason": "No lightweight score is stored yet.",
                "evidence_quotes": [],
                "model": frontmatter.lightweight_scoring_model or "heuristic:empty",
            }
        payload = frontmatter.lightweight_score.model_dump(mode="json")
        payload["model"] = (
            frontmatter.lightweight_scoring_model or frontmatter.lightweight_enrichment_model
        )
        return payload

    @staticmethod
    def _source_lookup() -> dict[str, dict[str, Any]]:
        store = VaultStore()
        return {
            source.id: {
                "source_id": source.id,
                "name": source.name,
                "type": source.type,
                "description": source.description,
                "tags": list(source.tags),
            }
            for source in store.load_sources_config().sources
        }

    @staticmethod
    def _profile_context() -> dict[str, Any]:
        profile = load_profile_snapshot()
        prompt_guidance = getattr(profile, "prompt_guidance", None)
        enrichment_guidance = ""
        if hasattr(prompt_guidance, "enrichment"):
            enrichment_guidance = normalize_whitespace(str(prompt_guidance.enrichment or ""))
        elif isinstance(prompt_guidance, dict):
            enrichment_guidance = normalize_whitespace(str(prompt_guidance.get("enrichment") or ""))
        return {
            "favorite_topics": list(getattr(profile, "favorite_topics", []) or []),
            "favorite_authors": list(getattr(profile, "favorite_authors", []) or []),
            "favorite_sources": list(getattr(profile, "favorite_sources", []) or []),
            "ignored_topics": list(getattr(profile, "ignored_topics", []) or []),
            "prompt_guidance": {"enrichment": enrichment_guidance},
        }

    def _source_context_for_document(
        self,
        document: RawDocument,
        *,
        source_lookup: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        frontmatter = document.frontmatter
        stored = source_lookup.get(frontmatter.source_id or "")
        context = {
            "source_id": frontmatter.source_id,
            "name": frontmatter.source_name,
            "type": None,
            "description": None,
            "tags": [],
        }
        if stored:
            context |= stored
        if frontmatter.source_id == ALPHAXIV_SOURCE_ID:
            context |= self._alphaxiv_metric_context(document)
        return context

    def _scoring_input_hash(
        self,
        *,
        document: RawDocument,
        metadata_payload: dict[str, Any],
        profile_context: dict[str, Any],
        source_context: dict[str, Any],
    ) -> str:
        payload = {
            "title": document.frontmatter.title,
            "body_hash": content_hash(document.frontmatter.title, document.body),
            "summary": self._normalize_optional_string(metadata_payload.get("short_summary")),
            "authors": self._normalize_string_list(metadata_payload.get("authors")),
            "tags": self._normalize_string_list(metadata_payload.get("tags")),
            "profile": profile_context,
            "source": source_context,
            "pipeline": self._scoring_pipeline_signature(),
        }
        return content_hash(
            document.frontmatter.title,
            json.dumps(payload, sort_keys=True, ensure_ascii=True),
        )

    def _heuristic_score_payload(
        self,
        *,
        document: RawDocument,
        metadata_payload: dict[str, Any],
        profile_context: dict[str, Any],
        source_context: dict[str, Any],
    ) -> dict[str, Any]:
        title = normalize_whitespace(document.frontmatter.title)
        summary = self._normalize_optional_string(metadata_payload.get("short_summary")) or ""
        tags = self._normalize_string_list(metadata_payload.get("tags"))
        authors = self._normalize_string_list(metadata_payload.get("authors"))
        text_haystack = normalize_whitespace(
            "\n".join(
                [
                    title,
                    summary,
                    " ".join(tags),
                    " ".join(authors),
                    document.body[:4000],
                    str(source_context.get("description") or ""),
                    " ".join(source_context.get("tags") or []),
                ]
            )
        ).casefold()

        favorite_topics = [
            value.casefold() for value in profile_context.get("favorite_topics") or []
        ]
        ignored_topics = [value.casefold() for value in profile_context.get("ignored_topics") or []]
        favorite_authors = [
            value.casefold() for value in profile_context.get("favorite_authors") or []
        ]
        favorite_sources = [
            value.casefold() for value in profile_context.get("favorite_sources") or []
        ]

        topic_hits = sum(1 for value in favorite_topics if value and value in text_haystack)
        ignored_hits = sum(1 for value in ignored_topics if value and value in text_haystack)
        author_hits = sum(1 for author in authors if author.casefold() in set(favorite_authors))
        source_tokens = {
            normalize_whitespace(str(document.frontmatter.source_name or "")).casefold(),
            normalize_whitespace(str(document.frontmatter.source_id or "")).casefold(),
            normalize_whitespace(str(source_context.get("name") or "")).casefold(),
            normalize_whitespace(str(source_context.get("source_id") or "")).casefold(),
        }
        source_match = any(token and token in set(favorite_sources) for token in source_tokens)

        topic_fit_score = self._clamp_unit_score(
            (0.55 if not favorite_topics and not ignored_topics else 0.18 + topic_hits * 0.22)
            - min(ignored_hits * 0.24, 0.6)
        )
        source_fit_score = self._clamp_unit_score(
            1.0 if source_match else (0.55 if not favorite_sources else 0.35)
        )
        author_fit_score = self._clamp_unit_score(
            1.0 if author_hits else (0.5 if not favorite_authors else 0.18)
        )
        evidence_fit_score = self._clamp_unit_score(
            0.25
            + min(len(summary) / 220.0, 0.35)
            + min(len(normalize_whitespace(document.body)) / 4000.0, 0.25)
            + (0.08 if tags else 0.0)
            + (0.07 if authors else 0.0)
        )
        relevance_score = self._clamp_unit_score(
            topic_fit_score * 0.45
            + source_fit_score * 0.2
            + author_fit_score * 0.15
            + evidence_fit_score * 0.2
            - min(ignored_hits * 0.08, 0.2)
        )
        bucket_hint = "archive"
        if relevance_score >= 0.76:
            bucket_hint = "must_read"
        elif relevance_score >= 0.36:
            bucket_hint = "worth_a_skim"
        reasons: list[str] = []
        if topic_hits:
            reasons.append(f"{topic_hits} favorite-topic match{'es' if topic_hits != 1 else ''}")
        if source_match:
            reasons.append("favorite-source match")
        if author_hits:
            reasons.append(f"{author_hits} favorite-author match{'es' if author_hits != 1 else ''}")
        if ignored_hits:
            reasons.append(f"{ignored_hits} ignored-topic hit{'s' if ignored_hits != 1 else ''}")
        if not reasons:
            reasons.append("generic profile-fit fallback")
        evidence_quotes = []
        if summary:
            evidence_quotes.append(summary[:160])
        elif title:
            evidence_quotes.append(title[:160])
        return {
            "relevance_score": relevance_score,
            "source_fit_score": source_fit_score,
            "topic_fit_score": topic_fit_score,
            "author_fit_score": author_fit_score,
            "evidence_fit_score": evidence_fit_score,
            "confidence_score": 0.45,
            "bucket_hint": bucket_hint,
            "reason": f"Heuristic fallback based on {', '.join(reasons)}.",
            "evidence_quotes": evidence_quotes,
            "model": "heuristic:profile-fallback",
        }

    def _alphaxiv_metric_context(self, document: RawDocument) -> dict[str, Any]:
        metadata = self._load_document_asset_json(
            document,
            filename=ALPHAXIV_METADATA_FILENAME,
        )
        if not isinstance(metadata, dict):
            return {}

        metrics = metadata.get("metrics") if isinstance(metadata.get("metrics"), dict) else {}
        visits = metrics.get("visits_count") if isinstance(metrics.get("visits_count"), dict) else {}
        public_total_votes = self._coerce_non_negative_float(metrics.get("public_total_votes"))
        total_votes = self._coerce_non_negative_float(metrics.get("total_votes"))
        visits_all = self._coerce_non_negative_float(visits.get("all"))
        visits_last_7_days = self._coerce_non_negative_float(
            visits.get("last_7_days") or visits.get("last7Days")
        )
        x_likes = self._coerce_non_negative_float(metrics.get("x_likes"))
        citations_count = self._coerce_non_negative_float(metadata.get("citations_count"))

        weighted_sum = 0.0
        total_weight = 0.0
        for weight, value, reference in (
            (0.38, public_total_votes, 500.0),
            (0.22, total_votes, 150.0),
            (0.22, visits_last_7_days, 10_000.0),
            (0.12, visits_all, 20_000.0),
            (0.04, x_likes, 100.0),
            (0.02, citations_count, 50.0),
        ):
            if value is None or value <= 0:
                continue
            weighted_sum += weight * self._log_scaled_metric(value, reference=reference)
            total_weight += weight
        if total_weight <= 0:
            return {}

        engagement_score = self._clamp_unit_score(weighted_sum / total_weight)
        summary_bits: list[str] = []
        if public_total_votes is not None:
            summary_bits.append(f"{int(public_total_votes)} public votes")
        if total_votes is not None:
            summary_bits.append(f"{int(total_votes)} total votes")
        if visits_last_7_days is not None:
            summary_bits.append(f"{int(visits_last_7_days)} visits in the last 7 days")
        elif visits_all is not None:
            summary_bits.append(f"{int(visits_all)} lifetime visits")

        return {
            "alphaxiv_metrics": {
                "public_total_votes": public_total_votes,
                "total_votes": total_votes,
                "visits_all": visits_all,
                "visits_last_7_days": visits_last_7_days,
                "x_likes": x_likes,
                "citations_count": citations_count,
            },
            "alphaxiv_engagement_score": engagement_score,
            "alphaxiv_engagement_summary": ", ".join(summary_bits[:3]),
        }

    def _load_document_asset_json(
        self,
        document: RawDocument,
        *,
        filename: str,
    ) -> dict[str, Any] | None:
        path = (self.store.root / document.path).parent / filename
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _coerce_non_negative_float(value: object) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if parsed < 0:
            return None
        return parsed

    @staticmethod
    def _log_scaled_metric(value: float, *, reference: float) -> float:
        if value <= 0 or reference <= 0:
            return 0.0
        return min(math.log1p(value) / math.log1p(reference), 1.0)

    def _apply_source_scoring_priors(
        self,
        *,
        score_payload: dict[str, Any],
        source_context: dict[str, Any],
    ) -> dict[str, Any]:
        engagement_score = self._coerce_non_negative_float(
            source_context.get("alphaxiv_engagement_score")
        )
        if engagement_score is None or engagement_score <= 0:
            return score_payload

        payload = dict(score_payload)
        topic_fit_score = self._coerce_score_value(payload.get("topic_fit_score"))
        source_fit_score = self._clamp_unit_score(
            max(
                self._coerce_score_value(payload.get("source_fit_score")),
                0.45 + engagement_score * 0.35,
            )
        )
        author_fit_score = self._coerce_score_value(payload.get("author_fit_score"))
        evidence_fit_score = self._clamp_unit_score(
            max(
                self._coerce_score_value(payload.get("evidence_fit_score")),
                0.35 + engagement_score * 0.3,
            )
        )
        confidence_score = self._clamp_unit_score(
            max(
                self._coerce_score_value(payload.get("confidence_score")),
                0.45 + engagement_score * 0.35,
            )
        )
        relevance_score = self._clamp_unit_score(
            self._coerce_score_value(payload.get("relevance_score"))
            + engagement_score
            * (0.04 + topic_fit_score * 0.08 + source_fit_score * 0.04)
        )
        reason = self._normalize_optional_string(payload.get("reason")) or "Profile-fit judgment."
        engagement_summary = self._normalize_optional_string(
            source_context.get("alphaxiv_engagement_summary")
        )
        if engagement_summary:
            reason = (
                f"{reason} alphaXiv engagement signals: {engagement_summary}."
            )[:400]

        model = self._normalize_optional_string(payload.get("model"))
        if model and ALPHAXIV_METRIC_MODEL_SUFFIX not in model:
            model = f"{model}+{ALPHAXIV_METRIC_MODEL_SUFFIX}"
        elif not model:
            model = ALPHAXIV_METRIC_MODEL_SUFFIX

        payload.update(
            {
                "relevance_score": relevance_score,
                "source_fit_score": source_fit_score,
                "topic_fit_score": topic_fit_score,
                "author_fit_score": author_fit_score,
                "evidence_fit_score": evidence_fit_score,
                "confidence_score": confidence_score,
                "bucket_hint": self._bucket_hint_for_relevance(relevance_score),
                "reason": reason,
                "model": model,
            }
        )
        return payload

    @classmethod
    def _coerce_score_value(cls, value: object) -> float:
        try:
            return cls._clamp_unit_score(float(value))
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _bucket_hint_for_relevance(relevance_score: float) -> str:
        if relevance_score >= 0.76:
            return "must_read"
        if relevance_score >= 0.36:
            return "worth_a_skim"
        return "archive"

    @staticmethod
    def _clamp_unit_score(value: float) -> float:
        return round(min(max(value, 0.0), 1.0), 4)

    def _deterministic_lightweight_payload(
        self,
        document: RawDocument,
    ) -> dict[str, object] | None:
        frontmatter = document.frontmatter
        body = document.body
        if frontmatter.kind != "newsletter":
            if frontmatter.source_id == ALPHAXIV_SOURCE_ID:
                metadata = self._load_document_asset_json(
                    document,
                    filename=ALPHAXIV_METADATA_FILENAME,
                )
                if not isinstance(metadata, dict):
                    return None
                summary = self._normalize_optional_string(metadata.get("short_summary"))
                authors = self._normalize_string_list(metadata.get("authors"))
                topics = self._normalize_string_list(metadata.get("topics"))
                tags = list(dict.fromkeys([*frontmatter.tags, *topics]))[:10]
                if not summary and not authors and not tags:
                    return None
                return {
                    "short_summary": summary or frontmatter.short_summary,
                    "authors": authors or list(frontmatter.authors),
                    "tags": tags,
                    "generation_mode": "deterministic",
                    "model": "deterministic:alphaxiv-metadata",
                }
            return None
        if frontmatter.source_id == "tldr-email":
            summary = self._build_tldr_newsletter_summary(frontmatter.title, body)
            if not summary:
                return None
            return {
                "short_summary": summary,
                "authors": list(frontmatter.authors),
                "tags": list(frontmatter.tags),
                "generation_mode": "deterministic",
                "model": "deterministic:tldr-newsletter",
            }
        if frontmatter.source_id == "medium-email":
            summary = self._build_medium_newsletter_summary(frontmatter.title, body)
            if not summary:
                return None
            return {
                "short_summary": summary,
                "authors": list(frontmatter.authors),
                "tags": list(frontmatter.tags),
                "generation_mode": "deterministic",
                "model": "deterministic:medium-newsletter",
            }
        return None

    def _build_tldr_newsletter_summary(self, title: str, body: str) -> str | None:
        issue_title = self._clean_tldr_issue_title(title) or self._extract_tldr_body_title(body)
        sections = self._parse_tldr_sections(body)
        editorial_sections = [
            section
            for section in sections
            if section["stories"] and section["title"].casefold() not in TLDR_QUICK_LINK_SECTIONS
        ]
        quick_link_count = sum(
            len(section["stories"])
            for section in sections
            if section["title"].casefold() in TLDR_QUICK_LINK_SECTIONS
        )
        editorial_story_count = sum(len(section["stories"]) for section in editorial_sections)

        headline = issue_title
        if not headline and editorial_sections:
            preview_titles = [section["title"] for section in editorial_sections[:3]]
            headline = normalize_whitespace(", ".join(preview_titles))
        if not headline and not editorial_story_count and not quick_link_count:
            return None

        first_sentence = "TLDR roundup."
        if headline:
            first_sentence = f"TLDR roundup on {headline.rstrip('.!?')}."

        if editorial_story_count:
            detail = (
                f"Includes {editorial_story_count} editorial "
                f"{'story' if editorial_story_count == 1 else 'stories'} across "
                f"{len(editorial_sections)} {'section' if len(editorial_sections) == 1 else 'sections'}"
            )
            if quick_link_count:
                detail += f", plus {quick_link_count} quick {'link' if quick_link_count == 1 else 'links'}"
            return f"{first_sentence} {detail}."

        if quick_link_count:
            return f"{first_sentence} Includes {quick_link_count} quick {'link' if quick_link_count == 1 else 'links'}."
        return first_sentence

    def _build_medium_newsletter_summary(self, title: str, body: str) -> str | None:
        story_titles = self._parse_medium_story_titles(body)
        headline = story_titles[0] if story_titles else self._clean_medium_issue_title(title)
        if not headline:
            return None

        first_sentence = f'Medium digest led by "{headline.rstrip(".!?")}".'
        story_count = len(story_titles)
        if story_count <= 1:
            return first_sentence

        secondary_title = story_titles[1]
        summary = (
            f"{first_sentence} Includes {story_count} highlighted stories, including "
            f'"{secondary_title.rstrip(".!?")}".'
        )
        if len(summary) <= 240:
            return summary
        return f"{first_sentence} Includes {story_count} highlighted stories."

    @staticmethod
    def _clean_tldr_issue_title(value: str) -> str:
        normalized = normalize_whitespace(value)
        if not normalized:
            return ""
        cleaned = re.sub(r"^(?:TLDR(?:\s+\w+)?:\s*)", "", normalized, flags=re.IGNORECASE)
        cleaned = "".join(
            character
            for character in cleaned
            if unicodedata.category(character) != "So" and character not in {"\ufe0e", "\ufe0f"}
        )
        cleaned = normalize_whitespace(cleaned).strip(" -,:")
        cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
        if len(cleaned) > 140:
            cleaned = f"{cleaned[:137].rstrip(' ,;:')}..."
        return cleaned

    @staticmethod
    def _clean_medium_issue_title(value: str) -> str:
        normalized = normalize_whitespace(value)
        if not normalized:
            return ""
        cleaned = normalize_whitespace(normalized.split("|", 1)[0])
        return cleaned.strip(" -,:")

    @staticmethod
    def _extract_tldr_body_title(body: str) -> str:
        for raw_line in body.splitlines():
            match = TLDR_TITLE_RE.match(raw_line.strip())
            if match:
                return normalize_whitespace(match.group("title"))
        return ""

    @staticmethod
    def _parse_tldr_sections(body: str) -> list[dict[str, str | list[str]]]:
        sections: list[dict[str, str | list[str]]] = []
        current_section: dict[str, str | list[str]] | None = None
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            section_match = TLDR_SECTION_RE.match(line)
            if section_match:
                current_section = {
                    "title": normalize_whitespace(section_match.group("title")),
                    "stories": [],
                }
                sections.append(current_section)
                continue

            story_match = TLDR_STORY_RE.match(line)
            if story_match and current_section is not None:
                story_title = normalize_whitespace(
                    story_match.group("link_title") or story_match.group("plain_title")
                )
                if story_title:
                    stories = current_section["stories"]
                    if isinstance(stories, list):
                        stories.append(story_title)
        return sections

    @staticmethod
    def _parse_medium_story_titles(body: str) -> list[str]:
        titles: list[str] = []
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            story_match = TLDR_STORY_RE.match(line)
            if not story_match:
                continue
            story_title = normalize_whitespace(
                story_match.group("link_title") or story_match.group("plain_title")
            )
            if story_title and story_title not in titles:
                titles.append(story_title)
        return titles

    @staticmethod
    def _normalize_string_list(value: object) -> list[str]:
        if not isinstance(value, list):
            return []
        normalized: list[str] = []
        for entry in value:
            cleaned = normalize_whitespace(entry)
            if cleaned:
                normalized.append(cleaned)
        return list(dict.fromkeys(normalized))

    @staticmethod
    def _normalize_optional_string(value: object) -> str | None:
        cleaned = normalize_whitespace(value)
        return cleaned or None
