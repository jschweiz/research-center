from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from sqlalchemy import and_, case, delete, func, or_, select, text
from sqlalchemy.exc import IntegrityError

from app.core.config import get_settings
from app.db.models import (
    ContentType,
    RunStatus,
    VaultAIBudgetDay,
    VaultAIBudgetReservation,
    VaultAITrace,
    VaultItemProjection,
    VaultLease,
    VaultPairedDevice,
    VaultPairingCode,
    VaultProjectionState,
    VaultPublishedEdition,
    VaultReadItem,
    VaultRawDocument,
    VaultRun,
    VaultRunEvent,
    VaultRunStep,
    VaultSource,
    VaultStarredItem,
    VaultStopRequest,
    VaultTopic,
    VaultTopicEdge,
    VaultWikiPage,
)
from app.db.session import get_engine, get_session_factory
from app.schemas.ops import IngestionRunHistoryRead
from app.schemas.published import PublishedEditionSummaryRead
from app.vault.models import (
    AIRunManifest,
    AITraceReference,
    GraphEdge,
    GraphIndex,
    GraphNode,
    InsightsIndex,
    ItemsIndex,
    LocalBudgetDayState,
    LocalBudgetReservationState,
    LocalBudgetState,
    OperationStopRequestState,
    PagesIndex,
    PairedDevicesState,
    PairedDeviceState,
    PairingCodesState,
    PairingCodeState,
    PublishedIndex,
    ReadItemsState,
    RawDocument,
    SUB_DOCUMENT_TAG,
    StarredItemsState,
    VaultItemRecord,
    VaultSourceDefinition,
    VaultSourcesConfig,
)

if TYPE_CHECKING:
    from app.vault.store import VaultStore


_BOOTSTRAPPED_STATE_KEYS: set[str] = set()
_RECONCILED_RUN_KEYS: set[str] = set()
_PUBLISHED_BOOTSTRAP_KEYS: set[str] = set()
_ITEM_BOOTSTRAP_KEYS: set[str] = set()
_WIKI_BOOTSTRAP_KEYS: set[str] = set()


def _utcnow() -> datetime:
    return datetime.now(UTC)


class VaultStateRepository:
    def __init__(self, store: VaultStore) -> None:
        self.store = store
        self.settings = get_settings()

    def ensure_bootstrap(self) -> None:
        state_key = self._state_key()
        if state_key not in _BOOTSTRAPPED_STATE_KEYS:
            self._bootstrap_legacy_runtime_state()
            _BOOTSTRAPPED_STATE_KEYS.add(state_key)
        if state_key not in _RECONCILED_RUN_KEYS:
            self.reconcile_orphaned_runs()
            _RECONCILED_RUN_KEYS.add(state_key)

    def load_sources_config(self) -> VaultSourcesConfig:
        self.ensure_bootstrap()
        with self._session() as db:
            rows = list(db.scalars(select(VaultSource).order_by(VaultSource.name.asc())).all())
        return VaultSourcesConfig(sources=[self._to_source_definition(row) for row in rows])

    def save_sources_config(self, config: VaultSourcesConfig) -> None:
        now = _utcnow()
        with self._session() as db:
            db.execute(delete(VaultSource))
            for source in config.sources:
                created_at = source.created_at or now
                updated_at = source.updated_at or created_at
                db.add(
                    VaultSource(
                        id=source.id,
                        type=source.type,
                        name=source.name,
                        enabled=source.enabled,
                        raw_kind=source.raw_kind,
                        custom_pipeline_id=source.custom_pipeline_id,
                        classification_mode=source.classification_mode,
                        decomposition_mode=source.decomposition_mode,
                        description=source.description,
                        tags_json=list(source.tags),
                        url=source.url,
                        max_items=source.max_items,
                        config_json=dict(source.config_json),
                        created_at=created_at,
                        updated_at=updated_at,
                    )
                )
            db.commit()

    def load_pairing_codes(self) -> PairingCodesState:
        self.ensure_bootstrap()
        with self._session() as db:
            rows = list(
                db.scalars(
                    select(VaultPairingCode).order_by(VaultPairingCode.created_at.asc())
                ).all()
            )
        return PairingCodesState(
            codes=[
                PairingCodeState(
                    id=row.id,
                    label=row.label,
                    local_url=row.local_url,
                    expires_at=row.expires_at,
                    redeemed_at=row.redeemed_at,
                    metadata_json=dict(row.metadata_json),
                    created_at=row.created_at,
                )
                for row in rows
            ]
        )

    def save_pairing_codes(self, state: PairingCodesState) -> None:
        with self._session() as db:
            db.execute(delete(VaultPairingCode))
            for code in state.codes:
                db.add(
                    VaultPairingCode(
                        id=code.id,
                        label=code.label,
                        local_url=code.local_url,
                        expires_at=code.expires_at,
                        redeemed_at=code.redeemed_at,
                        metadata_json=dict(code.metadata_json),
                        created_at=code.created_at,
                    )
                )
            db.commit()

    def load_paired_devices(self) -> PairedDevicesState:
        self.ensure_bootstrap()
        with self._session() as db:
            rows = list(
                db.scalars(
                    select(VaultPairedDevice).order_by(VaultPairedDevice.paired_at.asc())
                ).all()
            )
        return PairedDevicesState(
            devices=[
                PairedDeviceState(
                    id=row.id,
                    label=row.label,
                    token_hash=row.token_hash,
                    last_used_at=row.last_used_at,
                    last_seen_ip=row.last_seen_ip,
                    revoked_at=row.revoked_at,
                    metadata_json=dict(row.metadata_json),
                    paired_at=row.paired_at,
                    updated_at=row.updated_at,
                )
                for row in rows
            ]
        )

    def save_paired_devices(self, state: PairedDevicesState) -> None:
        with self._session() as db:
            db.execute(delete(VaultPairedDevice))
            for device in state.devices:
                db.add(
                    VaultPairedDevice(
                        id=device.id,
                        label=device.label,
                        token_hash=device.token_hash,
                        last_used_at=device.last_used_at,
                        last_seen_ip=device.last_seen_ip,
                        revoked_at=device.revoked_at,
                        metadata_json=dict(device.metadata_json),
                        paired_at=device.paired_at,
                        updated_at=device.updated_at,
                    )
                )
            db.commit()

    def load_ai_budget(self) -> LocalBudgetState:
        self.ensure_bootstrap()
        with self._session() as db:
            days = list(
                db.scalars(
                    select(VaultAIBudgetDay).order_by(VaultAIBudgetDay.budget_date.asc())
                ).all()
            )
            reservations = list(
                db.scalars(
                    select(VaultAIBudgetReservation).order_by(
                        VaultAIBudgetReservation.created_at.asc()
                    )
                ).all()
            )
        return LocalBudgetState(
            days=[
                LocalBudgetDayState(
                    budget_date=row.budget_date,
                    spent_usd=row.spent_usd,
                    reserved_usd=row.reserved_usd,
                    limit_usd=row.limit_usd,
                    updated_at=row.updated_at,
                )
                for row in days
            ],
            reservations=[
                LocalBudgetReservationState(
                    id=row.id,
                    budget_date=row.budget_date,
                    provider=row.provider,
                    operation=row.operation,
                    state=row.state,
                    estimated_cost_usd=row.estimated_cost_usd,
                    actual_cost_usd=row.actual_cost_usd,
                    metadata_json=dict(row.metadata_json),
                    created_at=row.created_at,
                    finalized_at=row.finalized_at,
                )
                for row in reservations
            ],
        )

    def save_ai_budget(self, state: LocalBudgetState) -> None:
        with self._session() as db:
            db.execute(delete(VaultAIBudgetReservation))
            db.execute(delete(VaultAIBudgetDay))
            for day in state.days:
                db.add(
                    VaultAIBudgetDay(
                        budget_date=day.budget_date,
                        spent_usd=day.spent_usd,
                        reserved_usd=day.reserved_usd,
                        limit_usd=day.limit_usd,
                        updated_at=day.updated_at,
                    )
                )
            for reservation in state.reservations:
                db.add(
                    VaultAIBudgetReservation(
                        id=reservation.id,
                        budget_date=reservation.budget_date,
                        provider=reservation.provider,
                        operation=reservation.operation,
                        state=reservation.state,
                        estimated_cost_usd=reservation.estimated_cost_usd,
                        actual_cost_usd=reservation.actual_cost_usd,
                        metadata_json=dict(reservation.metadata_json),
                        created_at=reservation.created_at,
                        finalized_at=reservation.finalized_at,
                    )
                )
            db.commit()

    def load_starred_items(self) -> StarredItemsState:
        self.ensure_bootstrap()
        with self._session() as db:
            item_ids = list(
                db.scalars(
                    select(VaultStarredItem.item_id).order_by(VaultStarredItem.starred_at.desc())
                ).all()
            )
        return StarredItemsState(item_ids=item_ids)

    def save_starred_items(self, state: StarredItemsState) -> None:
        with self._session() as db:
            db.execute(delete(VaultStarredItem))
            now = _utcnow()
            for offset, item_id in enumerate(state.item_ids):
                db.add(VaultStarredItem(item_id=item_id, starred_at=now))
                now = now.replace(microsecond=min(now.microsecond + offset + 1, 999999))
            db.commit()

    def load_read_items(self) -> ReadItemsState:
        self.ensure_bootstrap()
        with self._session() as db:
            item_ids = list(
                db.scalars(
                    select(VaultReadItem.item_id).order_by(VaultReadItem.read_at.desc())
                ).all()
            )
        return ReadItemsState(item_ids=item_ids)

    def save_read_items(self, state: ReadItemsState) -> None:
        with self._session() as db:
            db.execute(delete(VaultReadItem))
            now = _utcnow()
            for offset, item_id in enumerate(state.item_ids):
                db.add(VaultReadItem(item_id=item_id, read_at=now))
                now = now.replace(microsecond=min(now.microsecond + offset + 1, 999999))
            db.commit()

    def append_run_record(self, payload: dict[str, Any]) -> None:
        self.upsert_run_record(payload)

    def write_run_records(self, payloads: list[dict[str, Any]]) -> None:
        with self._session() as db:
            db.execute(delete(VaultRunEvent))
            db.execute(delete(VaultRunStep))
            db.execute(delete(VaultRun))
            db.commit()
        for payload in payloads:
            self.upsert_run_record(payload)

    def upsert_run_record(self, payload: dict[str, Any]) -> None:
        run = IngestionRunHistoryRead.model_validate(payload)
        run_payload = run.model_dump(mode="json")
        with self._session() as db:
            existing = db.get(VaultRun, run.id)
            if existing is None:
                existing = VaultRun(
                    id=run.id,
                    run_type=run.run_type,
                    status=run.status,
                    operation_kind=run.operation_kind,
                    trigger=run.trigger,
                    title=run.title,
                    summary=run.summary,
                    started_at=run.started_at,
                    finished_at=run.finished_at,
                    payload_json=run_payload,
                    prompt_path=run.prompt_path,
                    manifest_path=run.manifest_path,
                    changed_file_count=run.changed_file_count,
                )
                db.add(existing)
            else:
                existing.run_type = run.run_type
                existing.status = run.status
                existing.operation_kind = run.operation_kind
                existing.trigger = run.trigger
                existing.title = run.title
                existing.summary = run.summary
                existing.started_at = run.started_at
                existing.finished_at = run.finished_at
                existing.payload_json = run_payload
                existing.prompt_path = run.prompt_path
                existing.manifest_path = run.manifest_path
                existing.changed_file_count = run.changed_file_count
                existing.updated_at = _utcnow()
                db.execute(delete(VaultRunStep).where(VaultRunStep.run_id == run.id))
                db.execute(delete(VaultRunEvent).where(VaultRunEvent.run_id == run.id))

            for index, step in enumerate(run.steps):
                db.add(
                    VaultRunStep(
                        run_id=run.id,
                        step_index=index,
                        step_kind=step.step_kind,
                        status=step.status,
                        started_at=step.started_at,
                        finished_at=step.finished_at,
                        source_id=step.source_id,
                        doc_id=step.doc_id,
                        created_count=step.created_count,
                        updated_count=step.updated_count,
                        skipped_count=step.skipped_count,
                        counts_by_kind_json=dict(step.counts_by_kind),
                        payload_json=step.model_dump(mode="json"),
                    )
                )

            event_index = 0
            for log in run.logs:
                db.add(
                    VaultRunEvent(
                        run_id=run.id,
                        event_index=event_index,
                        logged_at=log.logged_at,
                        level=log.level,
                        message=log.message,
                        step_index=None,
                        payload_json=log.model_dump(mode="json"),
                    )
                )
                event_index += 1
            for step_index, step in enumerate(run.steps):
                for log in step.logs:
                    db.add(
                        VaultRunEvent(
                            run_id=run.id,
                            event_index=event_index,
                            logged_at=log.logged_at,
                            level=log.level,
                            message=log.message,
                            step_index=step_index,
                            payload_json=log.model_dump(mode="json"),
                        )
                    )
                    event_index += 1
            db.commit()

    def load_run_records(self) -> list[dict[str, Any]]:
        self.ensure_bootstrap()
        with self._session() as db:
            rows = list(
                db.scalars(
                    select(VaultRun).order_by(VaultRun.started_at.asc(), VaultRun.created_at.asc())
                ).all()
            )
        return [dict(row.payload_json) for row in rows]

    def record_ai_trace_reference(
        self, reference: AITraceReference, *, run_id: str | None = None
    ) -> None:
        with self._session() as db:
            existing = db.get(VaultAITrace, reference.trace_id)
            if existing is None:
                existing = VaultAITrace(
                    trace_id=reference.trace_id,
                    run_id=run_id,
                    provider=reference.provider,
                    model=reference.model,
                    operation=reference.operation,
                    status=reference.status,
                    recorded_at=reference.recorded_at,
                    duration_ms=reference.duration_ms,
                    prompt_sha256=reference.prompt_sha256,
                    prompt_path=reference.prompt_path,
                    trace_path=reference.trace_path,
                    prompt_tokens=reference.prompt_tokens,
                    completion_tokens=reference.completion_tokens,
                    total_tokens=reference.total_tokens,
                    cost_usd=reference.cost_usd,
                    context_json=dict(reference.context),
                    error=reference.error,
                )
                db.add(existing)
            else:
                existing.run_id = run_id or existing.run_id
                existing.provider = reference.provider
                existing.model = reference.model
                existing.operation = reference.operation
                existing.status = reference.status
                existing.recorded_at = reference.recorded_at
                existing.duration_ms = reference.duration_ms
                existing.prompt_sha256 = reference.prompt_sha256
                existing.prompt_path = reference.prompt_path
                existing.trace_path = reference.trace_path
                existing.prompt_tokens = reference.prompt_tokens
                existing.completion_tokens = reference.completion_tokens
                existing.total_tokens = reference.total_tokens
                existing.cost_usd = reference.cost_usd
                existing.context_json = dict(reference.context)
                existing.error = reference.error
            db.commit()

    def write_ai_run_manifest(self, manifest: AIRunManifest, path: str) -> None:
        for trace in manifest.traces:
            self.record_ai_trace_reference(trace, run_id=manifest.run_id)
        self._save_projection_state(
            "ai-manifest:" + manifest.run_id,
            generated_at=manifest.generated_at,
            payload=manifest.model_dump(mode="json") | {"path": path},
        )

    def sync_raw_documents(self, documents: list[RawDocument]) -> None:
        with self._session() as db:
            db.execute(delete(VaultRawDocument))
            for document in documents:
                db.add(VaultRawDocument(**self._raw_document_kwargs(document)))
            db.commit()

    def upsert_raw_document(self, document: RawDocument) -> None:
        payload = self._raw_document_kwargs(document)
        with self._session() as db:
            existing = db.get(VaultRawDocument, document.path)
            if existing is None:
                db.add(VaultRawDocument(**payload))
            else:
                for field_name, value in payload.items():
                    setattr(existing, field_name, value)
            db.commit()

    def load_items_index(self) -> ItemsIndex:
        self.ensure_bootstrap()
        payload = self._projection_payload("items")
        if payload is None:
            self._bootstrap_item_projections_from_files()
            payload = self._projection_payload("items")
        if payload is not None:
            return ItemsIndex.model_validate(payload)
        return ItemsIndex(generated_at=_utcnow(), items=[])

    def save_items_index(self, index: ItemsIndex) -> None:
        with self._session() as db:
            db.execute(delete(VaultItemProjection))
            for item in index.items:
                db.add(
                    VaultItemProjection(
                        item_id=item.id,
                        raw_doc_path=item.raw_doc_path,
                        kind=item.kind,
                        title=item.title,
                        source_id=item.source_id,
                        source_name=item.source_name,
                        organization_name=item.organization_name,
                        published_at=item.published_at,
                        ingested_at=item.ingested_at,
                        fetched_at=item.fetched_at,
                        canonical_url=item.canonical_url,
                        content_type=item.content_type,
                        extraction_confidence=item.extraction_confidence,
                        cleaned_text=item.cleaned_text,
                        short_summary=item.short_summary,
                        tags_text=" ".join(item.tags),
                        status=item.status,
                        doc_role=item.doc_role,
                        parent_id=item.parent_id,
                        index_visibility=item.index_visibility,
                        content_hash=item.content_hash,
                        identity_hash=item.identity_hash,
                        bucket=item.score.bucket,
                        total_score=item.score.total_score,
                        trend_score=item.trend_score,
                        novelty_score=item.novelty_score,
                        lightweight_enrichment_status=item.lightweight_enrichment_status,
                        lightweight_enriched_at=item.lightweight_enriched_at,
                        payload_json=item.model_dump(mode="json"),
                        updated_at=item.updated_at,
                    )
                )
            db.commit()
        self._rebuild_item_fts(index.items)
        self._save_projection_state(
            "items", generated_at=index.generated_at, payload=index.model_dump(mode="json")
        )

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
        self.load_items_index()
        matched_ids: set[str] | None = None
        if query:
            matched_ids = self._search_item_ids(query)
            if not matched_ids:
                return []
        with self._session() as db:
            stmt = select(VaultItemProjection)
            if matched_ids is not None:
                stmt = stmt.where(VaultItemProjection.item_id.in_(matched_ids))
            if status_filter:
                stmt = stmt.where(VaultItemProjection.status == status_filter)
            else:
                stmt = stmt.where(VaultItemProjection.status != "archived")
            if content_type:
                try:
                    stmt = stmt.where(VaultItemProjection.content_type == ContentType(content_type))
                except ValueError:
                    return []
            if source_id:
                source_name = None
                source = db.get(VaultSource, source_id)
                if source is not None:
                    source_name = source.name
                stmt = stmt.where(
                    or_(
                        VaultItemProjection.source_id == source_id,
                        VaultItemProjection.source_name == source_id,
                        VaultItemProjection.source_name == source_name,
                        VaultItemProjection.kind == source_id,
                    )
                )
            rows = list(db.scalars(stmt).all())
        items = [VaultItemRecord.model_validate(row.payload_json) for row in rows]
        if date_from or date_to:
            items = [
                item
                for item in items
                if (
                    (item_date := self._item_reference_date(item)) is not None
                    and (date_from is None or item_date >= date_from)
                    and (date_to is None or item_date <= date_to)
                )
            ]
        if sort == "newest":
            items.sort(key=lambda item: item.published_at or item.ingested_at, reverse=True)
        elif sort == "oldest":
            items.sort(key=lambda item: item.published_at or item.ingested_at)
        else:
            items.sort(
                key=lambda item: (
                    item.score.total_score,
                    item.trend_score,
                    item.published_at or item.fetched_at or item.ingested_at,
                    item.title.lower(),
                ),
                reverse=True,
            )
        return items

    def query_items_page(
        self,
        *,
        query: str | None = None,
        status_filter: str | None = None,
        content_type: str | None = None,
        source_id: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        sort: str = "importance",
        include_hidden_primary_newsletters: bool = False,
        include_sub_documents: bool = True,
        offset: int = 0,
        limit: int | None = None,
    ) -> tuple[list[VaultItemRecord], int]:
        self.load_items_index()
        matched_ids: set[str] | None = None
        if query:
            matched_ids = self._search_item_ids(query)
            if not matched_ids:
                return [], 0

        reference_timestamp = self._item_reference_timestamp_expression()
        with self._session() as db:
            stmt = select(VaultItemProjection)
            if matched_ids is not None:
                stmt = stmt.where(VaultItemProjection.item_id.in_(matched_ids))
            if status_filter:
                stmt = stmt.where(VaultItemProjection.status == status_filter)
            else:
                stmt = stmt.where(VaultItemProjection.status != "archived")
            if content_type:
                try:
                    stmt = stmt.where(VaultItemProjection.content_type == ContentType(content_type))
                except ValueError:
                    return [], 0
            if source_id:
                source_name = None
                source = db.get(VaultSource, source_id)
                if source is not None:
                    source_name = source.name
                stmt = stmt.where(
                    or_(
                        VaultItemProjection.source_id == source_id,
                        VaultItemProjection.source_name == source_id,
                        VaultItemProjection.source_name == source_name,
                        VaultItemProjection.kind == source_id,
                    )
                )
            if date_from is not None:
                stmt = stmt.where(reference_timestamp >= self._local_day_start_utc(date_from))
            if date_to is not None:
                stmt = stmt.where(
                    reference_timestamp < self._local_day_start_utc(date_to + timedelta(days=1))
                )

            visibility_filter = VaultItemProjection.index_visibility != "hidden"
            if include_hidden_primary_newsletters:
                visibility_filter = or_(
                    visibility_filter,
                    and_(
                        VaultItemProjection.content_type == ContentType.NEWSLETTER,
                        VaultItemProjection.doc_role == "primary",
                    ),
                )
            stmt = stmt.where(visibility_filter)
            if not include_sub_documents:
                stmt = stmt.where(VaultItemProjection.doc_role != "derived")
                stmt = stmt.where(
                    func.lower(VaultItemProjection.tags_text).not_like(f"%{SUB_DOCUMENT_TAG}%")
                )

            total = int(db.scalar(select(func.count()).select_from(stmt.subquery())) or 0)

            if sort == "newest":
                stmt = stmt.order_by(
                    self._sort_timestamp_expression().desc(),
                    VaultItemProjection.title.asc(),
                )
            elif sort == "oldest":
                stmt = stmt.order_by(
                    self._sort_timestamp_expression().asc(),
                    VaultItemProjection.title.asc(),
                )
            else:
                stmt = stmt.order_by(
                    VaultItemProjection.total_score.desc(),
                    VaultItemProjection.trend_score.desc(),
                    self._content_type_bonus_expression().desc(),
                    reference_timestamp.desc(),
                    VaultItemProjection.title.asc(),
                )

            if offset > 0:
                stmt = stmt.offset(offset)
            if limit is not None:
                stmt = stmt.limit(limit)

            rows = list(db.scalars(stmt).all())
        return [VaultItemRecord.model_validate(row.payload_json) for row in rows], total

    @staticmethod
    def _item_reference_timestamp_expression():
        return func.coalesce(
            VaultItemProjection.published_at,
            VaultItemProjection.fetched_at,
            VaultItemProjection.ingested_at,
        )

    @staticmethod
    def _sort_timestamp_expression():
        return func.coalesce(
            VaultItemProjection.published_at,
            VaultItemProjection.ingested_at,
        )

    @staticmethod
    def _content_type_bonus_expression():
        return case(
            (VaultItemProjection.content_type == ContentType.PAPER, 1.4),
            (VaultItemProjection.content_type == ContentType.POST, 1.2),
            (VaultItemProjection.content_type == ContentType.ARTICLE, 1.0),
            (VaultItemProjection.content_type == ContentType.NEWS, 0.95),
            (VaultItemProjection.content_type == ContentType.NEWSLETTER, 0.8),
            (VaultItemProjection.content_type == ContentType.SIGNAL, 0.7),
            (VaultItemProjection.content_type == ContentType.THREAD, 0.65),
            else_=1.0,
        )

    def _local_day_start_utc(self, value: date) -> datetime:
        try:
            timezone = ZoneInfo(self.settings.timezone)
        except Exception:
            timezone = UTC
        return datetime.combine(value, time.min, tzinfo=timezone).astimezone(UTC)

    def _item_reference_date(self, item: VaultItemRecord) -> date | None:
        reference = item.published_at or item.fetched_at or item.ingested_at
        if reference is None:
            return None
        normalized = reference if reference.tzinfo else reference.replace(tzinfo=UTC)
        try:
            timezone = ZoneInfo(self.settings.timezone)
        except Exception:
            return normalized.date()
        return normalized.astimezone(timezone).date()

    def get_item(self, item_id: str) -> VaultItemRecord | None:
        self.load_items_index()
        with self._session() as db:
            row = db.get(VaultItemProjection, item_id)
        if row is None:
            return None
        return VaultItemRecord.model_validate(row.payload_json)

    def load_insights_index(self) -> InsightsIndex:
        self.load_items_index()
        payload = self._projection_payload("insights")
        if payload is not None:
            return InsightsIndex.model_validate(payload)
        return InsightsIndex(generated_at=_utcnow())

    def save_insights_index(self, index: InsightsIndex) -> None:
        with self._session() as db:
            db.execute(delete(VaultTopicEdge))
            db.execute(delete(VaultTopic))
            for topic in index.topics:
                db.add(
                    VaultTopic(
                        topic_id=topic.id,
                        label=topic.label,
                        slug=topic.slug,
                        page_path=topic.page_path,
                        source_diversity=topic.source_diversity,
                        total_item_count=topic.total_item_count,
                        recent_item_count_7d=topic.recent_item_count_7d,
                        recent_item_count_30d=topic.recent_item_count_30d,
                        first_seen_at=topic.first_seen_at,
                        last_seen_at=topic.last_seen_at,
                        trend_score=topic.trend_score,
                        novelty_score=topic.novelty_score,
                        payload_json=topic.model_dump(mode="json"),
                    )
                )
            for edge in index.connections:
                db.add(
                    VaultTopicEdge(
                        source_topic_id=edge.source_topic_id,
                        target_topic_id=edge.target_topic_id,
                        weight=edge.weight,
                    )
                )
            db.commit()
        self._save_projection_state(
            "insights", generated_at=index.generated_at, payload=index.model_dump(mode="json")
        )

    def load_pages_index(self) -> PagesIndex:
        self.ensure_bootstrap()
        payload = self._projection_payload("pages")
        if payload is None:
            self._bootstrap_wiki_projections_from_files()
            payload = self._projection_payload("pages")
        if payload is not None:
            return PagesIndex.model_validate(payload)
        return PagesIndex(generated_at=_utcnow())

    def save_pages_index(self, index: PagesIndex) -> None:
        with self._session() as db:
            db.execute(delete(VaultWikiPage))
            for page in index.pages:
                db.add(
                    VaultWikiPage(
                        page_id=page.id,
                        path=page.path,
                        page_type=page.page_type,
                        title=page.title,
                        namespace=page.namespace,
                        slug=page.slug,
                        updated_at=page.updated_at,
                        managed=page.managed,
                        payload_json=page.model_dump(mode="json"),
                    )
                )
            db.commit()
        self._save_projection_state(
            "pages", generated_at=index.generated_at, payload=index.model_dump(mode="json")
        )

    def load_graph_index(self) -> GraphIndex:
        self.load_pages_index()
        payload = self._projection_payload("graph")
        if payload is not None:
            return GraphIndex.model_validate(payload)

        pages = self.load_pages_index()
        entry_lookup = {entry.id: entry for entry in pages.pages}
        edges = sorted(
            {
                (backlink, entry.id, "wiki_link")
                for entry in pages.pages
                for backlink in entry.backlinks
                if backlink in entry_lookup
            }
        )
        graph = GraphIndex(
            generated_at=self._projection_generated_at("pages") or _utcnow(),
            nodes=[
                GraphNode(
                    id=entry.id, label=entry.title, node_type=entry.page_type, path=entry.path
                )
                for entry in pages.pages
            ],
            edges=[
                GraphEdge(source=source, target=target, edge_type=edge_type)
                for source, target, edge_type in edges
            ],
        )
        self.save_graph_index(graph)
        return graph

    def save_graph_index(self, index: GraphIndex) -> None:
        self._save_projection_state(
            "graph", generated_at=index.generated_at, payload=index.model_dump(mode="json")
        )

    def load_published_index(self) -> PublishedIndex:
        self.ensure_bootstrap()
        payload = self._projection_payload("published")
        if payload is None:
            self._bootstrap_published_projections_from_files()
            payload = self._projection_payload("published")
        if payload is not None:
            return PublishedIndex.model_validate(payload)
        return PublishedIndex(generated_at=_utcnow())

    def save_published_index(self, index: PublishedIndex) -> None:
        editions: list[PublishedEditionSummaryRead] = []
        if index.latest is not None:
            editions.append(index.latest)
        for edition in index.editions:
            if all(existing.edition_id != edition.edition_id for existing in editions):
                editions.append(edition)
        with self._session() as db:
            db.execute(delete(VaultPublishedEdition))
            for edition in editions:
                db.add(self._published_edition_row(edition))
            db.commit()
        self._save_projection_state(
            "published",
            generated_at=index.generated_at,
            payload=index.model_copy(update={"editions": editions}).model_dump(mode="json"),
        )

    def acquire_lease(
        self,
        *,
        name: str,
        owner: str = "mac",
        ttl_seconds: int = 600,
    ):
        from app.vault.store import LeaseBusyError, LeaseHandle

        self.ensure_bootstrap()
        now = _utcnow()
        while True:
            token = self._lease_token()
            with self._session() as db:
                current = db.get(VaultLease, name)
                if current is not None:
                    expires_at = (
                        current.expires_at
                        if current.expires_at.tzinfo
                        else current.expires_at.replace(tzinfo=UTC)
                    )
                    if expires_at <= now:
                        db.delete(current)
                        db.commit()
                    else:
                        raise LeaseBusyError(f"Lease '{name}' is already held by {current.owner}.")
                row = VaultLease(
                    name=name,
                    owner=owner,
                    token=token,
                    acquired_at=now,
                    expires_at=now.replace(microsecond=0) + self._seconds_delta(ttl_seconds),
                )
                db.add(row)
                try:
                    db.commit()
                except IntegrityError:
                    db.rollback()
                    continue
                return LeaseHandle(
                    name=name, token=token, path=self.store.leases_dir / f"{name}.json"
                )

    def renew_lease(self, handle, *, ttl_seconds: int = 600) -> None:
        from app.vault.store import LeaseBusyError

        now = _utcnow()
        with self._session() as db:
            row = db.get(VaultLease, handle.name)
            if row is None or row.token != handle.token:
                raise LeaseBusyError(f"Lease '{handle.name}' is no longer held by the current run.")
            row.expires_at = now.replace(microsecond=0) + self._seconds_delta(ttl_seconds)
            db.commit()

    def release_lease(self, handle) -> None:
        with self._session() as db:
            row = db.get(VaultLease, handle.name)
            if row is None or row.token != handle.token:
                return
            db.delete(row)
            db.commit()

    def clear_lease(self, name: str) -> None:
        with self._session() as db:
            row = db.get(VaultLease, name)
            if row is None:
                return
            db.delete(row)
            db.commit()

    def request_operation_stop(
        self,
        *,
        run_id: str,
        source_id: str | None = None,
        requested_by: str = "local-control",
    ) -> None:
        payload = OperationStopRequestState(
            run_id=run_id,
            source_id=source_id,
            requested_by=requested_by,
            requested_at=_utcnow(),
        )
        with self._session() as db:
            existing = db.get(VaultStopRequest, run_id)
            if existing is None:
                db.add(
                    VaultStopRequest(
                        run_id=payload.run_id,
                        source_id=payload.source_id,
                        requested_by=payload.requested_by,
                        requested_at=payload.requested_at,
                    )
                )
            else:
                existing.source_id = payload.source_id
                existing.requested_by = payload.requested_by
                existing.requested_at = payload.requested_at
            db.commit()

    def is_operation_stop_requested(self, run_id: str) -> bool:
        self.ensure_bootstrap()
        with self._session() as db:
            return db.get(VaultStopRequest, run_id) is not None

    def clear_operation_stop_request(self, run_id: str) -> None:
        with self._session() as db:
            row = db.get(VaultStopRequest, run_id)
            if row is None:
                return
            db.delete(row)
            db.commit()

    def reconcile_orphaned_runs(self) -> None:
        with self._session() as db:
            rows = list(
                db.scalars(
                    select(VaultRun).where(
                        VaultRun.status.in_((RunStatus.PENDING, RunStatus.RUNNING))
                    )
                ).all()
            )
            changed = False
            for row in rows:
                payload = IngestionRunHistoryRead.model_validate(row.payload_json)
                if payload.status not in {RunStatus.PENDING, RunStatus.RUNNING}:
                    continue
                interrupted_at = _utcnow()
                errors = list(payload.errors)
                reason = (
                    "Run was interrupted when the app restarted before the process could finish."
                )
                if reason not in errors:
                    errors.append(reason)
                payload = payload.model_copy(
                    update={
                        "status": RunStatus.INTERRUPTED,
                        "finished_at": interrupted_at,
                        "summary": reason,
                        "errors": errors,
                        "duration_seconds": payload.duration_seconds
                        if payload.duration_seconds is not None
                        else round((interrupted_at - payload.started_at).total_seconds(), 2),
                    }
                )
                row.status = payload.status
                row.finished_at = payload.finished_at
                row.summary = payload.summary
                row.payload_json = payload.model_dump(mode="json")
                row.updated_at = interrupted_at
                changed = True
            if changed:
                db.commit()

    def _bootstrap_legacy_runtime_state(self) -> None:
        self._bootstrap_sources_from_file()
        self._bootstrap_runs_from_file()
        self._bootstrap_pairing_from_files()
        self._bootstrap_starred_items_from_file()
        self._bootstrap_budget_from_file()

    def _bootstrap_sources_from_file(self) -> None:
        with self._session() as db:
            if db.scalar(select(VaultSource.id).limit(1)) is not None:
                return
        if not self.store.sources_config_path.exists():
            return
        legacy = self.store._load_json_model(
            self.store.sources_config_path, VaultSourcesConfig, default=VaultSourcesConfig()
        )
        if legacy is None or not legacy.sources:
            return
        self.save_sources_config(legacy)

    def _bootstrap_runs_from_file(self) -> None:
        with self._session() as db:
            if db.scalar(select(VaultRun.id).limit(1)) is not None:
                return
        if not self.store.run_log_path.exists():
            return
        payloads = []
        for raw in self.store.run_log_path.read_text(encoding="utf-8").splitlines():
            if not raw.strip():
                continue
            payload = IngestionRunHistoryRead.model_validate_json(raw)
            if payload.status in {RunStatus.PENDING, RunStatus.RUNNING}:
                interrupted_at = _utcnow()
                reason = "Run was interrupted during migration from legacy run-log storage."
                payload = payload.model_copy(
                    update={
                        "status": RunStatus.INTERRUPTED,
                        "finished_at": interrupted_at,
                        "summary": reason,
                        "errors": [*payload.errors, reason],
                        "duration_seconds": payload.duration_seconds
                        if payload.duration_seconds is not None
                        else round((interrupted_at - payload.started_at).total_seconds(), 2),
                    }
                )
            payloads.append(payload.model_dump(mode="json"))
        self.write_run_records(payloads)

    def _bootstrap_pairing_from_files(self) -> None:
        with self._session() as db:
            has_pairing = db.scalar(select(VaultPairingCode.id).limit(1)) is not None
            has_devices = db.scalar(select(VaultPairedDevice.id).limit(1)) is not None
        if not has_pairing and self.store.pairing_codes_path.exists():
            payload = self.store._load_json_model(
                self.store.pairing_codes_path, PairingCodesState, default=PairingCodesState()
            )
            if payload is not None:
                self.save_pairing_codes(payload)
        if not has_devices and self.store.paired_devices_path.exists():
            payload = self.store._load_json_model(
                self.store.paired_devices_path,
                PairedDevicesState,
                default=PairedDevicesState(),
            )
            if payload is not None:
                self.save_paired_devices(payload)

    def _bootstrap_starred_items_from_file(self) -> None:
        with self._session() as db:
            if db.scalar(select(VaultStarredItem.item_id).limit(1)) is not None:
                return
        if not self.store.starred_items_path.exists():
            return
        payload = self.store._load_json_model(
            self.store.starred_items_path, StarredItemsState, default=StarredItemsState()
        )
        if payload is not None:
            self.save_starred_items(payload)

    def _bootstrap_budget_from_file(self) -> None:
        with self._session() as db:
            has_days = db.scalar(select(VaultAIBudgetDay.budget_date).limit(1)) is not None
            has_reservations = db.scalar(select(VaultAIBudgetReservation.id).limit(1)) is not None
        if has_days or has_reservations or not self.store.ai_budget_path.exists():
            return
        payload = self.store._load_json_model(
            self.store.ai_budget_path, LocalBudgetState, default=LocalBudgetState()
        )
        if payload is not None:
            self.save_ai_budget(payload)

    def _bootstrap_item_projections_from_files(self) -> None:
        state_key = self._state_key()
        if state_key in _ITEM_BOOTSTRAP_KEYS:
            return
        _ITEM_BOOTSTRAP_KEYS.add(state_key)
        if not list(self.store.raw_dir.glob("*/*/source.md")):
            return
        from app.services.vault_ingestion import VaultIndexService

        indexer = VaultIndexService(store=self.store, ensure_layout=False)
        documents = self.store.list_raw_documents()
        self.sync_raw_documents(documents)
        items = indexer._expected_index_items(
            documents=documents, persist_normalized_frontmatter=False
        )
        items, insights = indexer.insights.enrich_items(items)
        items = indexer._score_items(items)
        self.save_items_index(ItemsIndex(generated_at=_utcnow(), items=items))
        self.save_insights_index(insights)

    def _bootstrap_wiki_projections_from_files(self) -> None:
        state_key = self._state_key()
        if state_key in _WIKI_BOOTSTRAP_KEYS:
            return
        _WIKI_BOOTSTRAP_KEYS.add(state_key)
        if not self.store.wiki_dir.exists() or not list(self.store.wiki_dir.rglob("*.md")):
            return
        from app.services.vault_wiki_index import VaultWikiIndexService

        service = VaultWikiIndexService(store=self.store, ensure_layout=False)
        pages, graph = service.scan()
        self.save_pages_index(pages)
        self.save_graph_index(graph)

    def _bootstrap_published_projections_from_files(self) -> None:
        state_key = self._state_key()
        if state_key in _PUBLISHED_BOOTSTRAP_KEYS:
            return
        _PUBLISHED_BOOTSTRAP_KEYS.add(state_key)

        manifests = list(self.store.viewer_dir.glob("history/*/manifest.json"))
        latest_manifest = self.store.viewer_dir / "latest" / "manifest.json"
        if latest_manifest.exists():
            manifests.append(latest_manifest)
        editions: list[PublishedEditionSummaryRead] = []
        for path in manifests:
            try:
                manifest_payload = path.read_text(encoding="utf-8")
                edition = PublishedEditionSummaryRead.model_validate_json(
                    self._extract_edition_json(manifest_payload)
                )
            except Exception:
                continue
            if all(existing.edition_id != edition.edition_id for existing in editions):
                editions.append(edition)
        if not editions:
            return
        editions.sort(key=lambda entry: entry.published_at, reverse=True)
        self.save_published_index(
            PublishedIndex(generated_at=_utcnow(), latest=editions[0], editions=editions)
        )

    def _projection_payload(self, name: str) -> dict[str, Any] | None:
        with self._session() as db:
            row = db.get(VaultProjectionState, name)
            if row is None:
                return None
            return dict(row.payload_json)

    def _projection_generated_at(self, name: str) -> datetime | None:
        with self._session() as db:
            row = db.get(VaultProjectionState, name)
            return row.generated_at if row is not None else None

    def _save_projection_state(
        self, name: str, *, generated_at: datetime, payload: dict[str, Any]
    ) -> None:
        with self._session() as db:
            row = db.get(VaultProjectionState, name)
            if row is None:
                row = VaultProjectionState(
                    name=name, generated_at=generated_at, payload_json=payload
                )
                db.add(row)
            else:
                row.generated_at = generated_at
                row.payload_json = payload
            db.commit()

    @staticmethod
    def _ensure_item_fts_table(connection) -> None:
        try:
            connection.execute(
                text(
                    """
                    CREATE VIRTUAL TABLE IF NOT EXISTS vault_item_fts
                    USING fts5(
                        item_id UNINDEXED,
                        title,
                        short_summary,
                        cleaned_text,
                        source_name,
                        tags
                    )
                    """
                )
            )
        except Exception:
            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS vault_item_fts (
                        item_id TEXT PRIMARY KEY,
                        title TEXT NOT NULL DEFAULT '',
                        short_summary TEXT NOT NULL DEFAULT '',
                        cleaned_text TEXT NOT NULL DEFAULT '',
                        source_name TEXT NOT NULL DEFAULT '',
                        tags TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
            )

    def _rebuild_item_fts(self, items: list[VaultItemRecord]) -> None:
        engine = get_engine()
        if engine.dialect.name != "sqlite":
            return
        with engine.begin() as connection:
            self._ensure_item_fts_table(connection)
            connection.execute(text("DELETE FROM vault_item_fts"))
            for item in items:
                connection.execute(
                    text(
                        """
                        INSERT INTO vault_item_fts (
                            item_id,
                            title,
                            short_summary,
                            cleaned_text,
                            source_name,
                            tags
                        ) VALUES (
                            :item_id,
                            :title,
                            :short_summary,
                            :cleaned_text,
                            :source_name,
                            :tags
                        )
                        """
                    ),
                    {
                        "item_id": item.id,
                        "title": item.title,
                        "short_summary": item.short_summary or "",
                        "cleaned_text": item.cleaned_text or "",
                        "source_name": item.source_name,
                        "tags": " ".join(item.tags),
                    },
                )

    def _search_item_ids(self, query: str) -> set[str]:
        engine = get_engine()
        if engine.dialect.name != "sqlite":
            return set()
        sanitized = " ".join(part for part in query.replace(":", " ").split() if part)
        if not sanitized:
            return set()
        with engine.connect() as connection:
            try:
                rows = connection.execute(
                    text("SELECT item_id FROM vault_item_fts WHERE vault_item_fts MATCH :query"),
                    {"query": sanitized},
                )
            except Exception:
                rows = connection.execute(
                    text(
                        """
                        SELECT item_id
                        FROM vault_item_fts
                        WHERE title LIKE :pattern
                           OR short_summary LIKE :pattern
                           OR cleaned_text LIKE :pattern
                           OR source_name LIKE :pattern
                           OR tags LIKE :pattern
                        """
                    ),
                    {"pattern": f"%{query}%"},
                )
            return {str(row[0]) for row in rows}

    def _published_edition_row(self, edition: PublishedEditionSummaryRead) -> VaultPublishedEdition:
        return VaultPublishedEdition(
            edition_id=edition.edition_id,
            record_name=edition.record_name,
            period_type=edition.period_type,
            brief_date=edition.brief_date,
            week_start=edition.week_start,
            week_end=edition.week_end,
            title=edition.title,
            generated_at=edition.generated_at,
            published_at=edition.published_at,
            has_audio=edition.has_audio,
            schema_version=edition.schema_version,
            payload_json=edition.model_dump(mode="json"),
        )

    def _to_source_definition(self, row: VaultSource) -> VaultSourceDefinition:
        return VaultSourceDefinition(
            id=row.id,
            type=row.type,
            name=row.name,
            enabled=row.enabled,
            raw_kind=row.raw_kind,
            custom_pipeline_id=row.custom_pipeline_id,
            classification_mode=row.classification_mode,
            decomposition_mode=row.decomposition_mode,
            description=row.description,
            tags=list(row.tags_json),
            url=row.url,
            max_items=row.max_items,
            created_at=row.created_at,
            updated_at=row.updated_at,
            config_json=dict(row.config_json),
        )

    @staticmethod
    def _raw_document_kwargs(document: RawDocument) -> dict[str, Any]:
        frontmatter = document.frontmatter
        return {
            "raw_doc_path": document.path,
            "doc_id": frontmatter.id,
            "kind": frontmatter.kind,
            "title": frontmatter.title,
            "source_id": frontmatter.source_id,
            "source_name": frontmatter.source_name,
            "canonical_url": frontmatter.canonical_url,
            "published_at": frontmatter.published_at,
            "ingested_at": frontmatter.ingested_at,
            "fetched_at": frontmatter.fetched_at,
            "content_hash": frontmatter.content_hash,
            "identity_hash": frontmatter.identity_hash,
            "tags_json": list(frontmatter.tags),
            "asset_paths_json": list(frontmatter.asset_paths),
            "status": frontmatter.status,
            "doc_role": frontmatter.doc_role,
            "parent_id": frontmatter.parent_id,
            "index_visibility": frontmatter.index_visibility,
            "short_summary": frontmatter.short_summary,
            "lightweight_enrichment_status": frontmatter.lightweight_enrichment_status,
            "lightweight_enriched_at": frontmatter.lightweight_enriched_at,
            "lightweight_enrichment_model": frontmatter.lightweight_enrichment_model,
            "lightweight_scoring_model": frontmatter.lightweight_scoring_model,
            "body_text": document.body,
            "frontmatter_json": frontmatter.model_dump(mode="json"),
            "payload_json": document.model_dump(mode="json"),
            "updated_at": frontmatter.lightweight_enriched_at
            or frontmatter.fetched_at
            or frontmatter.ingested_at,
        }

    @staticmethod
    def _extract_edition_json(manifest_text: str) -> str:
        import json

        payload = json.loads(manifest_text)
        return json.dumps(payload.get("edition") or {}, ensure_ascii=True)

    @staticmethod
    def _lease_token() -> str:
        import secrets

        return secrets.token_hex(16)

    @staticmethod
    def _seconds_delta(seconds: int):
        from datetime import timedelta

        return timedelta(seconds=seconds)

    @staticmethod
    def _session():
        return get_session_factory()()

    def _state_key(self) -> str:
        return f"{self.settings.database_url}|{self.store.root}|{self.store.local_state_root}"
