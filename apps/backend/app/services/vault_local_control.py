from __future__ import annotations

import hashlib
import io
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode
from uuid import uuid4

from app.core.config import get_settings
from app.core.security import get_session_manager
from app.schemas.local_control import (
    CodexStatusRead,
    LocalControlInsightsRead,
    LocalControlInsightTopicRead,
    LocalControlStatusRead,
    OllamaStatusRead,
    PairRedeemResponse,
    VaultGitStatusRead,
)
from app.services.vault_advanced_enrichment import VaultAdvancedEnrichmentService
from app.services.vault_briefs import VaultBriefService
from app.services.vault_git_sync import VaultGitSyncService
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_lightweight_enrichment import VaultLightweightEnrichmentService
from app.services.vault_publishing import VaultPublisherService
from app.vault.models import PairedDeviceState, PairingCodeState
from app.vault.store import VaultStore

try:
    import qrcode
    from qrcode.image.svg import SvgPathImage
except ImportError:  # pragma: no cover
    qrcode = None
    SvgPathImage = None

PAIRING_TOKEN_SALT = "research-center-local-pairing"


class LocalControlError(RuntimeError):
    pass


@dataclass(frozen=True)
class PairingCodeResult:
    device_label: str
    pairing_token: str
    pairing_url: str
    qr_svg: str | None
    expires_at: datetime
    hosted_return_url: str | None


class VaultLocalControlService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.store = VaultStore()
        self.store.ensure_layout()
        self.session_manager = get_session_manager()
        self.publisher = VaultPublisherService()
        self.briefs = VaultBriefService()
        self.ingestion = VaultIngestionService()
        self.lightweight = VaultLightweightEnrichmentService()
        self.advanced = VaultAdvancedEnrichmentService()
        self.sync = VaultGitSyncService()

    def create_pairing_code(self, *, label: str) -> PairingCodeResult:
        now = datetime.now(UTC)
        expires_at = now + timedelta(minutes=self.settings.local_pairing_token_ttl_minutes)
        state = self.store.load_pairing_codes()
        pairing_code = PairingCodeState(
            id=str(uuid4()),
            label=label.strip() or "iPad",
            local_url=self.settings.local_server_base_url.rstrip("/"),
            expires_at=expires_at,
            redeemed_at=None,
            metadata_json={},
            created_at=now,
        )
        state.codes = [
            code
            for code in state.codes
            if (
                (code.expires_at if code.expires_at.tzinfo else code.expires_at.replace(tzinfo=UTC))
                > now
            )
            and code.redeemed_at is None
        ]
        state.codes.append(pairing_code)
        self.store.save_pairing_codes(state)

        token = self.session_manager.issue_scoped_token(
            {
                "pairing_code_id": pairing_code.id,
                "issued_at": now.isoformat(),
                "expires_at": expires_at.isoformat(),
                "label": pairing_code.label,
                "local_url": pairing_code.local_url,
            },
            salt=PAIRING_TOKEN_SALT,
        )
        pairing_url = f"{pairing_code.local_url}/pair?{urlencode({'pairing_token': token})}"
        return PairingCodeResult(
            device_label=pairing_code.label,
            pairing_token=token,
            pairing_url=pairing_url,
            qr_svg=self._build_qr_svg(pairing_url),
            expires_at=expires_at,
            hosted_return_url=self._read_string(self.settings.hosted_viewer_url),
        )

    def redeem_pairing_token(
        self,
        *,
        pairing_token: str,
        device_label: str | None,
        client_ip: str | None,
    ) -> PairRedeemResponse:
        payload = self.session_manager.load_scoped_token(
            pairing_token,
            salt=PAIRING_TOKEN_SALT,
            max_age_seconds=self.settings.local_pairing_token_ttl_minutes * 60,
        )
        if payload is None:
            raise LocalControlError("Pairing token is invalid or expired.")

        pairing_code_id = str(payload.get("pairing_code_id") or "")
        pairing_codes = self.store.load_pairing_codes()
        pairing_code = next((code for code in pairing_codes.codes if code.id == pairing_code_id), None)
        if pairing_code is None:
            raise LocalControlError("Pairing token does not match an active pairing request.")
        expires_at = pairing_code.expires_at if pairing_code.expires_at.tzinfo else pairing_code.expires_at.replace(tzinfo=UTC)
        if pairing_code.redeemed_at is not None:
            raise LocalControlError("This pairing token has already been redeemed.")
        if expires_at <= datetime.now(UTC):
            raise LocalControlError("This pairing token has expired.")

        access_token = secrets.token_urlsafe(48)
        device = PairedDeviceState(
            id=str(uuid4()),
            label=(device_label or pairing_code.label).strip() or pairing_code.label,
            token_hash=self._token_hash(access_token),
            last_used_at=datetime.now(UTC),
            last_seen_ip=client_ip,
            revoked_at=None,
            metadata_json={"pairing_code_id": pairing_code.id},
            paired_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        devices = self.store.load_paired_devices()
        devices.devices.append(device)
        self.store.save_paired_devices(devices)

        pairing_code.redeemed_at = datetime.now(UTC)
        self.store.save_pairing_codes(pairing_codes)

        return PairRedeemResponse(
            device_label=device.label,
            paired_local_url=pairing_code.local_url,
            access_token=access_token,
            hosted_return_url=self._read_string(self.settings.hosted_viewer_url),
        )

    def authenticate_access_token(
        self,
        *,
        access_token: str,
        client_ip: str | None,
    ) -> PairedDeviceState:
        if not access_token:
            raise LocalControlError("Local control token is required.")
        devices = self.store.load_paired_devices()
        device = next(
            (
                current
                for current in devices.devices
                if current.token_hash == self._token_hash(access_token) and current.revoked_at is None
            ),
            None,
        )
        if device is None:
            raise LocalControlError("Local control token is invalid.")
        now = datetime.now(UTC)
        paired_at = device.paired_at if device.paired_at.tzinfo else device.paired_at.replace(tzinfo=UTC)
        max_age = timedelta(days=self.settings.local_control_token_max_age_days)
        if paired_at + max_age <= now:
            device.revoked_at = now
            device.updated_at = now
            self.store.save_paired_devices(devices)
            raise LocalControlError("Local control token expired. Pair this device again.")
        device.last_used_at = now
        device.last_seen_ip = client_ip
        device.updated_at = now
        self.store.save_paired_devices(devices)
        return device

    def build_status(self, device: PairedDeviceState) -> LocalControlStatusRead:
        latest_publication = self.publisher.get_latest_published_summary()
        latest_brief_date = latest_publication.brief_date if latest_publication else self.briefs.current_edition_date()
        latest_brief_dir = (
            self.store.brief_dir_for_date(latest_brief_date)
            if latest_brief_date is not None
            else None
        )
        raw_documents = self.store.list_raw_documents()
        _items, insights_index = self.advanced.insights.ensure_index(persist=False)
        return LocalControlStatusRead(
            device_label=device.label,
            paired_local_url=self.settings.local_server_base_url.rstrip("/"),
            vault_root_dir=str(self.store.root),
            viewer_bundle_dir=str(self.store.viewer_dir),
            current_brief_date=self.briefs.current_edition_date(),
            latest_publication=latest_publication,
            latest_brief_dir=str(latest_brief_dir) if latest_brief_dir is not None else None,
            raw_document_count=len(raw_documents),
            lightweight_pending_count=self.lightweight.count_pending_documents(documents=raw_documents),
            lightweight_metadata_pending_count=self.lightweight.count_metadata_pending_documents(
                documents=raw_documents
            ),
            lightweight_scoring_pending_count=self.lightweight.count_scoring_pending_documents(
                documents=raw_documents
            ),
            items_index=self.ingestion.items_index_status(documents=raw_documents),
            wiki_page_count=len(list(self.store.wiki_dir.rglob("*.md"))),
            topic_count=len(insights_index.topics),
            rising_topic_count=len(insights_index.rising_topic_ids),
            vault_sync=VaultGitStatusRead.model_validate(self.sync.status().__dict__),
            ollama=OllamaStatusRead.model_validate(self.lightweight.ollama_status()),
            codex=CodexStatusRead.model_validate(self.advanced.codex_status()),
        )

    def build_insights(self, *, limit: int = 12) -> LocalControlInsightsRead:
        payload = self.advanced.insight_radar(limit=limit)
        return LocalControlInsightsRead(
            map_page=payload.get("map_page"),
            trends_page=payload.get("trends_page"),
            topics=[
                LocalControlInsightTopicRead.model_validate(topic)
                for topic in payload.get("topics", [])
                if isinstance(topic, dict)
            ],
            rising_topics=[
                LocalControlInsightTopicRead.model_validate(topic)
                for topic in payload.get("rising_topics", [])
                if isinstance(topic, dict)
            ],
        )

    def list_recent_operations(self, *, limit: int = 40):
        return self.ingestion.list_recent_runs(limit=limit)

    def _build_qr_svg(self, value: str) -> str | None:
        if qrcode is None or SvgPathImage is None:
            return None
        image = qrcode.make(value, image_factory=SvgPathImage)
        buffer = io.BytesIO()
        image.save(buffer)
        return buffer.getvalue().decode("utf-8")

    @staticmethod
    def _token_hash(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _read_string(value: Any) -> str | None:
        return str(value).strip() if isinstance(value, str) and value.strip() else None
