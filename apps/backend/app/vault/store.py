from __future__ import annotations

import os
import secrets
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar

import orjson
from pydantic import BaseModel

from app.core.config import get_settings
from app.vault.frontmatter import parse_frontmatter_document, render_frontmatter_document
from app.vault.models import (
    AIRunManifest,
    AITraceArtifact,
    GraphIndex,
    InsightsIndex,
    ItemsIndex,
    LocalBudgetState,
    PagesIndex,
    PairedDevicesState,
    PairingCodesState,
    PublishedIndex,
    RawDocument,
    RawDocumentFrontmatter,
    StarredItemsState,
    VaultItemRecord,
    VaultSourcesConfig,
    WikiPage,
    WikiPageFrontmatter,
)

TModel = TypeVar("TModel", bound=BaseModel)


class LeaseBusyError(RuntimeError):
    pass


@dataclass(frozen=True)
class LeaseHandle:
    name: str
    token: str
    path: Path


class VaultStore:
    def __init__(self) -> None:
        settings = get_settings()
        self.settings = settings
        self.root = settings.vault_root_dir
        self.local_state_root = settings.local_state_dir
        from app.services.vault_state import VaultStateRepository

        self.state = VaultStateRepository(self)

    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    @property
    def wiki_dir(self) -> Path:
        return self.root / "wiki"

    @property
    def briefs_dir(self) -> Path:
        return self.root / "briefs" / "daily"

    @property
    def outputs_dir(self) -> Path:
        return self.root / "outputs"

    @property
    def viewer_dir(self) -> Path:
        return self.outputs_dir / "viewer"

    @property
    def indexes_dir(self) -> Path:
        return self.root / "system" / "indexes"

    @property
    def runs_dir(self) -> Path:
        return self.root / "system" / "runs"

    @property
    def config_dir(self) -> Path:
        return self.root / "system" / "config"

    @property
    def leases_dir(self) -> Path:
        return self.root / "system" / "leases"

    @property
    def items_index_path(self) -> Path:
        return self.indexes_dir / "items.json"

    @property
    def pages_index_path(self) -> Path:
        return self.indexes_dir / "pages.json"

    @property
    def graph_index_path(self) -> Path:
        return self.indexes_dir / "graph.json"

    @property
    def insights_index_path(self) -> Path:
        return self.indexes_dir / "insights.json"

    @property
    def published_index_path(self) -> Path:
        return self.indexes_dir / "published.json"

    @property
    def sources_config_path(self) -> Path:
        return self.config_dir / "sources.json"

    @property
    def run_log_path(self) -> Path:
        return self.runs_dir / "run-log.jsonl"

    @property
    def pairing_codes_path(self) -> Path:
        return self.local_state_root / "local-control" / "pairing-codes.json"

    @property
    def paired_devices_path(self) -> Path:
        return self.local_state_root / "local-control" / "paired-devices.json"

    @property
    def operation_stop_requests_dir(self) -> Path:
        return self.local_state_root / "local-control" / "operation-stop-requests"

    @property
    def ai_budget_path(self) -> Path:
        return self.local_state_root / "budgets" / "ai-budget.json"

    @property
    def ai_traces_dir(self) -> Path:
        return self.local_state_root / "ai-traces"

    @property
    def ai_run_manifests_dir(self) -> Path:
        return self.ai_traces_dir / "runs"

    @property
    def starred_items_path(self) -> Path:
        return self.local_state_root / "preferences" / "starred-items.json"

    def ensure_layout(self) -> None:
        from app.db.session import ensure_schema

        ensure_schema()
        for path in (
            self.raw_dir,
            self.wiki_dir,
            self.briefs_dir,
            self.outputs_dir,
            self.viewer_dir,
            self.indexes_dir,
            self.runs_dir,
            self.config_dir,
            self.leases_dir,
            self.local_state_root / "local-control",
            self.operation_stop_requests_dir,
            self.local_state_root / "budgets",
            self.local_state_root / "preferences",
            self.ai_traces_dir,
            self.ai_run_manifests_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        self.state.ensure_bootstrap()

    def list_raw_documents(self) -> list[RawDocument]:
        documents: list[RawDocument] = []
        for path in sorted(self.raw_dir.glob("*/*/source.md")):
            documents.append(self.read_raw_document(path))
        return documents

    def read_raw_document(self, path: Path) -> RawDocument:
        frontmatter, body = parse_frontmatter_document(path.read_text(encoding="utf-8"))
        return RawDocument(
            frontmatter=RawDocumentFrontmatter.model_validate(frontmatter),
            body=body,
            path=str(path.relative_to(self.root)),
        )

    def read_raw_document_relative(self, relative_path: str) -> RawDocument | None:
        path = self.root / relative_path
        if not path.exists():
            return None
        return self.read_raw_document(path)

    def find_raw_document(
        self, *, source_id: str | None, external_key: str | None
    ) -> RawDocument | None:
        if not source_id or not external_key:
            return None
        for document in self.list_raw_documents():
            if (
                document.frontmatter.source_id == source_id
                and document.frontmatter.external_key == external_key
            ):
                return document
        return None

    def write_raw_document(
        self,
        *,
        kind: str,
        doc_id: str,
        frontmatter: RawDocumentFrontmatter,
        body: str,
    ) -> Path:
        folder = self.raw_dir / kind / doc_id
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / "source.md"
        payload = render_frontmatter_document(frontmatter.model_dump(mode="json"), body)
        self.write_text(path, payload)
        self.state.upsert_raw_document(
            RawDocument(
                frontmatter=frontmatter,
                body=body,
                path=str(path.relative_to(self.root)),
            )
        )
        return path

    def read_wiki_page(self, path: Path) -> WikiPage:
        frontmatter, body = parse_frontmatter_document(path.read_text(encoding="utf-8"))
        return WikiPage(
            frontmatter=WikiPageFrontmatter.model_validate(frontmatter),
            body=body,
            path=str(path.relative_to(self.root)),
        )

    def write_wiki_page(
        self,
        *,
        namespace: str,
        slug: str,
        frontmatter: WikiPageFrontmatter,
        body: str,
    ) -> Path:
        folder = self.wiki_dir / namespace
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{slug}.md"
        payload = render_frontmatter_document(frontmatter.model_dump(mode="json"), body)
        self.write_text(path, payload)
        return path

    def load_items_index(self) -> ItemsIndex:
        return self.state.load_items_index()

    def save_items_index(self, index: ItemsIndex) -> None:
        self.state.save_items_index(index)

    def sync_raw_documents(self, documents: list[RawDocument]) -> None:
        self.state.sync_raw_documents(documents)

    def query_items(
        self,
        *,
        query: str | None = None,
        status_filter: str | None = None,
        content_type: str | None = None,
        source_id: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        sort: str = "importance",
    ) -> list[VaultItemRecord]:
        return self.state.query_items(
            query=query,
            status_filter=status_filter,
            content_type=content_type,
            source_id=source_id,
            date_from=date_from,
            date_to=date_to,
            sort=sort,
        )

    def get_item(self, item_id: str) -> VaultItemRecord | None:
        return self.state.get_item(item_id)

    def load_pages_index(self) -> PagesIndex:
        return self.state.load_pages_index()

    def save_pages_index(self, index: PagesIndex) -> None:
        self.state.save_pages_index(index)

    def load_graph_index(self) -> GraphIndex:
        return self.state.load_graph_index()

    def save_graph_index(self, index: GraphIndex) -> None:
        self.state.save_graph_index(index)

    def load_insights_index(self) -> InsightsIndex:
        return self.state.load_insights_index()

    def save_insights_index(self, index: InsightsIndex) -> None:
        self.state.save_insights_index(index)

    def load_published_index(self) -> PublishedIndex:
        return self.state.load_published_index()

    def save_published_index(self, index: PublishedIndex) -> None:
        self.state.save_published_index(index)

    def load_sources_config(self) -> VaultSourcesConfig:
        return self.state.load_sources_config()

    def save_sources_config(self, config: VaultSourcesConfig) -> None:
        self.state.save_sources_config(config)

    def brief_dir_for_date(self, brief_date: datetime | str | Any) -> Path:
        slug = brief_date.isoformat() if hasattr(brief_date, "isoformat") else str(brief_date)
        return self.briefs_dir / slug

    def load_pairing_codes(self) -> PairingCodesState:
        return self.state.load_pairing_codes()

    def save_pairing_codes(self, state: PairingCodesState) -> None:
        self.state.save_pairing_codes(state)

    def load_paired_devices(self) -> PairedDevicesState:
        return self.state.load_paired_devices()

    def save_paired_devices(self, state: PairedDevicesState) -> None:
        self.state.save_paired_devices(state)

    def load_ai_budget(self) -> LocalBudgetState:
        return self.state.load_ai_budget()

    def save_ai_budget(self, state: LocalBudgetState) -> None:
        self.state.save_ai_budget(state)

    def load_starred_items(self) -> StarredItemsState:
        return self.state.load_starred_items()

    def save_starred_items(self, state: StarredItemsState) -> None:
        self.state.save_starred_items(state)

    def append_run_record(self, payload: dict[str, Any]) -> None:
        self.state.append_run_record(payload)

    def write_run_records(self, payloads: list[dict[str, Any]]) -> None:
        self.state.write_run_records(payloads)

    def upsert_run_record(self, payload: dict[str, Any]) -> None:
        self.state.upsert_run_record(payload)

    def load_run_records(self) -> list[dict[str, Any]]:
        return self.state.load_run_records()

    def ai_trace_bundle_dir(self, *, recorded_at: datetime, trace_id: str) -> Path:
        normalized = recorded_at if recorded_at.tzinfo else recorded_at.replace(tzinfo=UTC)
        return self.ai_traces_dir / normalized.astimezone(UTC).date().isoformat() / trace_id

    def write_ai_trace_bundle(
        self,
        *,
        artifact: AITraceArtifact,
        prompt_markdown: str,
    ) -> tuple[Path, Path]:
        self.prune_ai_traces(retention_days=self.settings.ai_trace_retention_days)
        bundle_dir = self.ai_trace_bundle_dir(
            recorded_at=artifact.recorded_at, trace_id=artifact.id
        )
        prompt_path = bundle_dir / "prompt.md"
        trace_path = bundle_dir / "trace.json"
        updated_artifact = artifact.model_copy(update={"prompt_path": str(prompt_path)})
        self.write_text(prompt_path, prompt_markdown)
        self.write_json(trace_path, updated_artifact)
        return prompt_path, trace_path

    def write_ai_run_manifest(self, manifest: AIRunManifest) -> Path:
        path = self.ai_run_manifests_dir / f"{manifest.run_id}.json"
        self.write_json(path, manifest)
        self.state.write_ai_run_manifest(manifest, str(path))
        return path

    def prune_ai_traces(self, *, retention_days: int) -> None:
        if retention_days <= 0 or not self.ai_traces_dir.exists():
            return
        cutoff = self.utcnow().date() - timedelta(days=retention_days)
        for path in self.ai_traces_dir.iterdir():
            if path.name == "runs" or not path.is_dir():
                continue
            try:
                path_date = datetime.fromisoformat(path.name).date()
            except ValueError:
                continue
            if path_date < cutoff:
                shutil.rmtree(path, ignore_errors=True)

    def acquire_lease(
        self,
        *,
        name: str,
        owner: str = "mac",
        ttl_seconds: int = 600,
    ) -> LeaseHandle:
        return self.state.acquire_lease(name=name, owner=owner, ttl_seconds=ttl_seconds)

    def renew_lease(self, handle: LeaseHandle, *, ttl_seconds: int = 600) -> None:
        self.state.renew_lease(handle, ttl_seconds=ttl_seconds)

    def release_lease(self, handle: LeaseHandle) -> None:
        self.state.release_lease(handle)

    def clear_lease(self, *, name: str) -> None:
        self.state.clear_lease(name)

    def request_operation_stop(
        self,
        *,
        run_id: str,
        source_id: str | None = None,
        requested_by: str = "local-control",
    ) -> None:
        self.state.request_operation_stop(
            run_id=run_id,
            source_id=source_id,
            requested_by=requested_by,
        )

    def is_operation_stop_requested(self, run_id: str) -> bool:
        return self.state.is_operation_stop_requested(run_id)

    def clear_operation_stop_request(self, run_id: str) -> None:
        self.state.clear_operation_stop_request(run_id)

    def write_json(self, path: Path, payload: BaseModel | dict[str, Any]) -> None:
        content = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
        self.write_bytes(path, self._json_bytes(content, indent=True))

    def write_text(self, path: Path, value: str) -> None:
        self.write_bytes(path, value.encode("utf-8"))

    def write_bytes(self, path: Path, value: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
        try:
            tmp_path.write_bytes(value)
            os.replace(tmp_path, path)
        finally:
            tmp_path.unlink(missing_ok=True)

    def _load_json_model(
        self,
        path: Path,
        model: type[TModel],
        *,
        default: TModel | None,
    ) -> TModel | None:
        if not path.exists():
            return default
        payload = orjson.loads(path.read_bytes())
        return model.model_validate(payload)

    @staticmethod
    def _json_bytes(payload: dict[str, Any], *, indent: bool) -> bytes:
        options = orjson.OPT_SORT_KEYS
        if indent:
            options |= orjson.OPT_INDENT_2
        return orjson.dumps(payload, option=options)

    @staticmethod
    def utcnow() -> datetime:
        return datetime.now(UTC)
