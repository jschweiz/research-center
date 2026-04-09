from __future__ import annotations

import re
from typing import Any

from app.db.models import RunStatus
from app.schemas.ops import IngestionRunHistoryRead
from app.schemas.sources import (
    SourceCreate,
    SourceLatestExtractionRunRead,
    SourceProbeRead,
    SourceRead,
    SourceUpdate,
)
from app.services.vault_runtime import slugify, utcnow
from app.services.vault_sources import VaultSourceIngestionService
from app.vault.models import VaultSourceDefinition, VaultSourcesConfig
from app.vault.store import VaultStore

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class VaultSourceProbeError(RuntimeError):
    pass


class VaultSourceRegistryService:
    def __init__(self) -> None:
        self.store = VaultStore()
        self.store.ensure_layout()
        self.ingestion = VaultSourceIngestionService()

    def list_sources(self) -> list[SourceRead]:
        config = self.ingestion.ensure_default_sources_config()
        return [self._to_source_read(source) for source in config.sources]

    def get_source(self, source_id: str) -> SourceRead | None:
        source = self.ingestion.get_source(source_id)
        if source is None:
            return None
        return self._to_source_read(source)

    def create_source(self, payload: SourceCreate) -> SourceRead:
        config = self.ingestion.ensure_default_sources_config()
        now = utcnow()
        source = VaultSourceDefinition(
            id=self._allocate_source_id(payload.name, config),
            type=payload.type,
            name=payload.name.strip(),
            enabled=payload.active,
            raw_kind=payload.raw_kind.strip(),
            custom_pipeline_id=None,
            classification_mode=payload.classification_mode,
            decomposition_mode=payload.decomposition_mode,
            description=payload.description.strip() if payload.description else None,
            tags=self._normalize_strings(payload.tags),
            url=payload.url.strip() if payload.url else None,
            max_items=payload.max_items,
            created_at=now,
            updated_at=now,
            config_json=self._normalize_config(
                source_type=payload.type,
                query=payload.query,
                config_json=payload.config_json,
            ),
        )
        config.sources.append(source)
        self.store.save_sources_config(config)
        return self._to_source_read(source)

    def update_source(self, source_id: str, payload: SourceUpdate) -> SourceRead | None:
        config = self.ingestion.ensure_default_sources_config()
        source = next((item for item in config.sources if item.id == source_id), None)
        if source is None:
            return None

        current_query = self._query_from_definition(source)
        next_query = payload.query if payload.query is not None else current_query
        next_config = self._normalize_config(
            source_type=source.type,
            query=next_query,
            config_json=payload.config_json if payload.config_json is not None else source.config_json,
        )
        updated = source.model_copy(
            update={
                "name": payload.name.strip() if payload.name is not None else source.name,
                "enabled": payload.active if payload.active is not None else source.enabled,
                "raw_kind": payload.raw_kind.strip() if payload.raw_kind is not None else source.raw_kind,
                "classification_mode": payload.classification_mode if payload.classification_mode is not None else source.classification_mode,
                "decomposition_mode": payload.decomposition_mode if payload.decomposition_mode is not None else source.decomposition_mode,
                "description": payload.description.strip() if payload.description else None
                if payload.description is not None
                else source.description,
                "tags": self._normalize_strings(payload.tags) if payload.tags is not None else source.tags,
                "url": payload.url.strip() if payload.url else None if payload.url is not None else source.url,
                "max_items": payload.max_items if payload.max_items is not None else source.max_items,
                "updated_at": utcnow(),
                "config_json": next_config,
            }
        )

        config.sources = [updated if item.id == source_id else item for item in config.sources]
        self.store.save_sources_config(config)
        return self._to_source_read(updated)

    def delete_source(self, source_id: str) -> bool:
        config = self.ingestion.ensure_default_sources_config()
        next_sources = [source for source in config.sources if source.id != source_id]
        if len(next_sources) == len(config.sources):
            return False
        self.store.save_sources_config(VaultSourcesConfig(sources=next_sources))
        return True

    def latest_log(self, source_id: str):
        return self.ingestion.latest_run_for_source(source_id)

    def probe_source(self, source_id: str) -> SourceProbeRead:
        source = self.ingestion.get_source(source_id)
        if source is None:
            raise VaultSourceProbeError("Source not found.")

        if source.type == "website":
            entries = self.ingestion._discover_website_entries(source)
            if source.custom_pipeline_id == "alphaxiv-paper":
                detail = (
                    "Preview found "
                    f"{len(entries)} alphaXiv paper{'s' if len(entries) != 1 else ''} using the profile alphaXiv feed settings."
                )
            else:
                discovery_mode = (
                    str((source.config_json or {}).get("discovery_mode") or "rss_feed").strip()
                    or "rss_feed"
                )
                detail = (
                    f"Preview found {len(entries)} website entries using {discovery_mode.replace('_', ' ')} discovery."
                )
            return SourceProbeRead(
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                total_found=len(entries),
                sample_titles=[entry.title for entry in entries[:5]],
                detail=detail,
                checked_at=utcnow(),
            )

        if source.type == "gmail_newsletter":
            connector = self.ingestion._build_gmail_connector()
            if connector is None:
                raise VaultSourceProbeError("Gmail ingest credentials are not configured.")
            config_json = source.config_json if isinstance(source.config_json, dict) else {}
            senders = self._normalize_strings(config_json.get("senders") or [])
            labels = self._normalize_strings(config_json.get("labels") or [])
            raw_query = str(config_json.get("raw_query") or "").strip() or None
            messages = connector.list_newsletters(
                senders=senders or None,
                labels=labels or None,
                raw_query=raw_query,
                max_results=min(source.max_items, 5),
            )
            detail_bits: list[str] = [f"Preview found {len(messages)} Gmail message{'s' if len(messages) != 1 else ''}"]
            if senders:
                detail_bits.append(f"for {', '.join(senders)}")
            elif raw_query:
                detail_bits.append(f'for query "{raw_query}"')
            return SourceProbeRead(
                source_id=source.id,
                source_name=source.name,
                source_type=source.type,
                total_found=len(messages),
                sample_titles=[message.subject for message in messages[:5]],
                detail=" ".join(detail_bits) + ".",
                checked_at=utcnow(),
            )

        raise VaultSourceProbeError(f"Unsupported source type: {source.type}")

    def _to_source_read(self, source: VaultSourceDefinition) -> SourceRead:
        latest_run = self.ingestion.latest_run_for_source(source.id)
        created_at = source.created_at or utcnow()
        updated_at = source.updated_at or created_at
        last_synced_at = latest_run.finished_at if latest_run and latest_run.status == RunStatus.SUCCEEDED else None

        latest_summary = None
        if latest_run is not None:
            latest_summary = SourceLatestExtractionRunRead(
                id=latest_run.id,
                status=latest_run.status,
                operation_kind=latest_run.operation_kind,
                summary=latest_run.summary,
                started_at=latest_run.started_at,
                finished_at=latest_run.finished_at,
                emitted_kinds=self._emitted_kinds_from_run(latest_run),
            )

        return SourceRead(
            id=source.id,
            type=source.type,  # type: ignore[arg-type]
            name=source.name,
            raw_kind=source.raw_kind,
            classification_mode=source.classification_mode,  # type: ignore[arg-type]
            decomposition_mode=source.decomposition_mode,  # type: ignore[arg-type]
            url=source.url,
            query=self._query_from_definition(source),
            description=source.description,
            active=source.enabled,
            max_items=source.max_items,
            tags=source.tags,
            config_json=source.config_json,
            last_synced_at=last_synced_at,
            created_at=created_at,
            updated_at=updated_at,
            has_custom_pipeline=bool(source.custom_pipeline_id),
            custom_pipeline_id=source.custom_pipeline_id,
            latest_extraction_run=latest_summary,
        )

    @staticmethod
    def _emitted_kinds_from_run(run: IngestionRunHistoryRead) -> list[str]:
        kinds: list[str] = []
        for step in run.steps:
            for kind, count in step.counts_by_kind.items():
                if count > 0 and kind not in kinds:
                    kinds.append(kind)
        if kinds:
            return kinds
        for info in run.basic_info:
            if info.label == "Kinds":
                return [part.strip() for part in info.value.split(",") if part.strip() and part.strip() != "none"]
        return []

    def _allocate_source_id(self, name: str, config: VaultSourcesConfig) -> str:
        base = slugify(name, fallback="source")
        existing = {source.id for source in config.sources}
        if base not in existing:
            return base
        index = 2
        while f"{base}-{index}" in existing:
            index += 1
        return f"{base}-{index}"

    def _normalize_config(
        self,
        *,
        source_type: str,
        query: str | None,
        config_json: dict[str, Any],
    ) -> dict[str, Any]:
        normalized = dict(config_json or {})
        if source_type == "website":
            discovery_mode = str(normalized.get("discovery_mode") or "").strip() or "rss_feed"
            normalized["discovery_mode"] = discovery_mode
            website_url = str(normalized.get("website_url") or "").strip()
            if website_url:
                normalized["website_url"] = website_url
            else:
                normalized.pop("website_url", None)
            return normalized

        normalized.pop("senders", None)
        normalized.pop("raw_query", None)
        parsed = self._parse_query_input(query)
        if parsed["senders"]:
            normalized["senders"] = parsed["senders"]
        if parsed["raw_query"]:
            normalized["raw_query"] = parsed["raw_query"]
        labels = self._normalize_strings(normalized.get("labels") or [])
        if labels:
            normalized["labels"] = labels
        else:
            normalized.pop("labels", None)
        return normalized

    def _query_from_definition(self, source: VaultSourceDefinition) -> str | None:
        if source.type != "gmail_newsletter":
            return None
        config_json = source.config_json if isinstance(source.config_json, dict) else {}
        raw_query = str(config_json.get("raw_query") or "").strip()
        if raw_query:
            return raw_query
        senders = self._normalize_strings(config_json.get("senders") or [])
        if senders:
            return ", ".join(senders)
        labels = self._normalize_strings(config_json.get("labels") or [])
        if labels:
            return " ".join(f"label:{label}" for label in labels)
        return None

    @staticmethod
    def _parse_query_input(query: str | None) -> dict[str, Any]:
        trimmed = str(query or "").strip()
        if not trimmed:
            return {"senders": [], "raw_query": None}
        parts = [part.strip() for part in re.split(r"[\n,]+", trimmed) if part.strip()]
        if parts and all(EMAIL_RE.match(part) for part in parts):
            return {"senders": parts, "raw_query": None}
        if EMAIL_RE.match(trimmed):
            return {"senders": [trimmed], "raw_query": None}
        return {"senders": [], "raw_query": trimmed}

    @staticmethod
    def _normalize_strings(values: list[str] | tuple[str, ...] | Any) -> list[str]:
        if not isinstance(values, (list, tuple)):
            return []
        seen: set[str] = set()
        normalized: list[str] = []
        for raw in values:
            value = str(raw or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        return normalized
