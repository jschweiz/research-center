from __future__ import annotations

import imaplib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.encryption import get_fernet
from app.db.models import ConnectionProvider, ConnectionSecret, ConnectionStatus
from app.integrations.gmail_imap import GmailImapConnector
from app.integrations.gmail_oauth import GmailOAuthClient
from app.integrations.zotero import ZoteroClient
from app.schemas.connections import ConnectionPayload


@dataclass
class ZoteroVerificationResult:
    status: ConnectionStatus
    metadata_json: dict
    resolved_payload: dict[str, str]


@dataclass
class GmailVerificationResult:
    status: ConnectionStatus
    metadata_json: dict
    resolved_payload: dict[str, str]


class ConnectionService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.fernet = get_fernet()
        self.gmail_oauth = GmailOAuthClient()

    def get_connection(self, provider: ConnectionProvider) -> ConnectionSecret | None:
        return self.db.scalar(select(ConnectionSecret).where(ConnectionSecret.provider == provider))

    def get_zotero_connection(self, refresh_if_needed: bool = False) -> ConnectionSecret | None:
        connection = self.get_connection(ConnectionProvider.ZOTERO)
        if not connection:
            return None
        if refresh_if_needed and self._zotero_metadata_needs_refresh(connection):
            payload = self.get_payload(ConnectionProvider.ZOTERO)
            verification = self._verify_zotero_payload(
                payload=payload or {},
                base_metadata=connection.metadata_json | {"auth_mode": "api_key"},
                preserve_verified_identity=True,
            )
            return self.store_connection(
                provider=ConnectionProvider.ZOTERO,
                label=connection.label,
                payload=verification.resolved_payload,
                metadata_json=verification.metadata_json,
                status=verification.status,
                last_synced_at=connection.last_synced_at,
            )
        return connection

    def get_payload(self, provider: ConnectionProvider) -> dict | None:
        connection = self.get_connection(provider)
        if not connection:
            return None
        decrypted = self.fernet.decrypt(connection.encrypted_payload.encode("utf-8")).decode("utf-8")
        return json.loads(decrypted)

    def upsert_zotero_connection(self, payload: ConnectionPayload) -> ConnectionSecret:
        connection = self.get_connection(ConnectionProvider.ZOTERO)
        existing_payload = self.get_payload(ConnectionProvider.ZOTERO) if connection else {}
        raw_payload = payload.payload or {}
        requested_payload = self._normalize_zotero_payload(raw_payload)
        next_payload = {
            "api_key": requested_payload["api_key"] or str(existing_payload.get("api_key") or "").strip(),
            "library_id": requested_payload["library_id"]
            if "library_id" in raw_payload
            else str(existing_payload.get("library_id") or "").strip(),
            "library_type": requested_payload["library_type"]
            if "library_type" in raw_payload
            else str(existing_payload.get("library_type") or "users").strip() or "users",
        }
        verification = self._verify_zotero_payload(
            payload=next_payload,
            base_metadata=((connection.metadata_json if connection else {}) | payload.metadata_json | {"auth_mode": "api_key"}),
            preserve_verified_identity=False,
        )
        return self.store_connection(
            provider=ConnectionProvider.ZOTERO,
            label=payload.label,
            payload=verification.resolved_payload,
            metadata_json=verification.metadata_json,
            status=verification.status,
            last_synced_at=connection.last_synced_at if connection else None,
        )

    def upsert_connection(
        self, provider: ConnectionProvider, payload: ConnectionPayload
    ) -> ConnectionSecret:
        if provider == ConnectionProvider.GMAIL:
            return self.upsert_gmail_connection(payload)
        connection = self.get_connection(provider)
        merged_metadata = (connection.metadata_json if connection else {}) | payload.metadata_json
        next_payload = payload.payload if payload.payload else (self.get_payload(provider) if connection else {})
        if payload.payload:
            next_status = ConnectionStatus.CONNECTED
        elif connection:
            next_status = connection.status
        else:
            next_status = ConnectionStatus.DISCONNECTED
        return self.store_connection(
            provider=provider,
            label=payload.label,
            payload=next_payload,
            metadata_json=merged_metadata,
            status=next_status,
            last_synced_at=connection.last_synced_at if connection else None,
        )

    def upsert_gmail_connection(self, payload: ConnectionPayload) -> ConnectionSecret:
        connection = self.get_connection(ConnectionProvider.GMAIL)
        existing_payload = self.get_payload(ConnectionProvider.GMAIL) if connection else {}
        base_metadata = (connection.metadata_json if connection else {}) | payload.metadata_json
        requested_payload = payload.payload or {}
        requested_auth_mode = str(requested_payload.get("auth_mode") or existing_payload.get("auth_mode") or "").strip().lower()

        if requested_auth_mode == "app_password" or requested_payload.get("email") is not None or requested_payload.get("app_password") is not None:
            normalized_payload = self._normalize_gmail_imap_payload(
                requested_payload=requested_payload,
                existing_payload=existing_payload or {},
            )
            verification = self._verify_gmail_imap_payload(
                payload=normalized_payload,
                base_metadata=base_metadata,
            )
            return self.store_connection(
                provider=ConnectionProvider.GMAIL,
                label=payload.label,
                payload=verification.resolved_payload,
                metadata_json=verification.metadata_json,
                status=verification.status,
                last_synced_at=connection.last_synced_at if connection else None,
            )

        next_payload = requested_payload if requested_payload else (existing_payload if connection else {})
        if next_payload:
            next_status = ConnectionStatus.CONNECTED
            merged_metadata = {
                key: value for key, value in (base_metadata | {"last_error": None}).items() if value is not None
            }
        elif connection:
            next_status = connection.status
            merged_metadata = base_metadata
        else:
            next_status = ConnectionStatus.DISCONNECTED
            merged_metadata = base_metadata
        return self.store_connection(
            provider=ConnectionProvider.GMAIL,
            label=payload.label,
            payload=next_payload,
            metadata_json=merged_metadata,
            status=next_status,
            last_synced_at=connection.last_synced_at if connection else None,
        )

    def store_connection(
        self,
        *,
        provider: ConnectionProvider,
        label: str,
        payload: dict,
        metadata_json: dict,
        status: ConnectionStatus,
        last_synced_at: datetime | None = None,
    ) -> ConnectionSecret:
        connection = self.get_connection(provider)
        encrypted = self.fernet.encrypt(json.dumps(payload).encode("utf-8")).decode("utf-8")
        if not connection:
            connection = ConnectionSecret(
                provider=provider,
                label=label,
                encrypted_payload=encrypted,
                metadata_json=metadata_json,
                status=status,
                last_synced_at=last_synced_at,
            )
        else:
            connection.label = label
            connection.encrypted_payload = encrypted
            connection.metadata_json = metadata_json
            connection.status = status
            connection.last_synced_at = last_synced_at
        self.db.add(connection)
        self.db.commit()
        self.db.refresh(connection)
        return connection

    def get_valid_gmail_payload(self) -> dict | None:
        connection = self.get_connection(ConnectionProvider.GMAIL)
        if not connection:
            return None
        payload = self.get_payload(ConnectionProvider.GMAIL)
        if not payload:
            return None
        auth_mode = str(payload.get("auth_mode") or "").strip().lower()
        if auth_mode == "app_password" or payload.get("app_password"):
            email_address = str(payload.get("email") or "").strip()
            app_password = str(payload.get("app_password") or "").strip()
            if email_address and app_password:
                return payload | {"auth_mode": "app_password"}
            connection.status = ConnectionStatus.ERROR
            connection.metadata_json = connection.metadata_json | {"last_error": "Missing Gmail IMAP credentials."}
            self.db.add(connection)
            self.db.commit()
            return None
        if not self._is_expired(payload.get("expires_at")):
            return payload | {"auth_mode": "oauth"}
        refresh_token = payload.get("refresh_token")
        if not refresh_token:
            connection.status = ConnectionStatus.ERROR
            connection.metadata_json = connection.metadata_json | {"last_error": "Missing refresh token."}
            self.db.add(connection)
            self.db.commit()
            return None
        try:
            refreshed = self.gmail_oauth.refresh_access_token(refresh_token)
        except Exception as exc:
            connection.status = ConnectionStatus.ERROR
            connection.metadata_json = connection.metadata_json | {"last_error": str(exc)}
            self.db.add(connection)
            self.db.commit()
            return None
        merged_payload = payload | refreshed
        merged_payload["refresh_token"] = refreshed.get("refresh_token") or refresh_token
        refreshed_connection = self.store_connection(
            provider=ConnectionProvider.GMAIL,
            label=connection.label,
            payload=merged_payload,
            metadata_json={
                key: value for key, value in (connection.metadata_json | {"last_error": None}).items() if value is not None
            },
            status=ConnectionStatus.CONNECTED,
            last_synced_at=connection.last_synced_at,
        )
        refreshed_payload = self.get_payload(refreshed_connection.provider)
        return refreshed_payload | {"auth_mode": "oauth"} if refreshed_payload else None

    def _is_expired(self, expires_at: str | None) -> bool:
        if not expires_at:
            return False
        try:
            parsed = datetime.fromisoformat(expires_at)
        except ValueError:
            return False
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed <= datetime.now(UTC) + timedelta(minutes=2)

    def _normalize_zotero_payload(self, payload: dict) -> dict[str, str]:
        return {
            "api_key": str(payload.get("api_key") or "").strip(),
            "library_id": str(payload.get("library_id") or "").strip(),
            "library_type": str(payload.get("library_type") or "users").strip() or "users",
        }

    def _normalize_gmail_imap_payload(self, *, requested_payload: dict, existing_payload: dict) -> dict[str, str]:
        email_address = str(requested_payload.get("email") or existing_payload.get("email") or "").strip()
        app_password = str(requested_payload.get("app_password") or existing_payload.get("app_password") or "").strip()
        return {
            "auth_mode": "app_password",
            "email": email_address,
            "app_password": app_password,
        }

    def _is_zotero_personal_library_alias(self, library_id: str) -> bool:
        normalized = "".join(character for character in library_id.lower() if character.isalnum())
        return normalized in {"", "mylibrary"}

    def _verify_zotero_payload(
        self,
        *,
        payload: dict,
        base_metadata: dict,
        preserve_verified_identity: bool,
    ) -> ZoteroVerificationResult:
        normalized_payload = self._normalize_zotero_payload(payload)
        metadata = base_metadata | {
            "library_id": normalized_payload["library_id"] or base_metadata.get("library_id"),
            "library_type": normalized_payload["library_type"],
        }

        if not normalized_payload["api_key"]:
            return ZoteroVerificationResult(
                status=ConnectionStatus.ERROR,
                metadata_json=self._zotero_error_metadata(
                    metadata=metadata,
                    detail="Zotero API key is required.",
                    preserve_verified_identity=preserve_verified_identity,
                ),
                resolved_payload=normalized_payload,
            )

        if normalized_payload["library_type"] != "users" and not normalized_payload["library_id"]:
            return ZoteroVerificationResult(
                status=ConnectionStatus.ERROR,
                metadata_json=self._zotero_error_metadata(
                    metadata=metadata,
                    detail="A Zotero library ID is required for this library type.",
                    preserve_verified_identity=preserve_verified_identity,
                ),
                resolved_payload=normalized_payload,
            )

        try:
            key_info = ZoteroClient(
                api_key=normalized_payload["api_key"],
                library_id=normalized_payload["library_id"],
                library_type=normalized_payload["library_type"],
            ).get_current_key_info()
        except httpx.HTTPStatusError as exc:
            detail = f"Zotero rejected the API key ({exc.response.status_code})."
            return ZoteroVerificationResult(
                status=ConnectionStatus.ERROR,
                metadata_json=self._zotero_error_metadata(
                    metadata=metadata,
                    detail=detail,
                    preserve_verified_identity=preserve_verified_identity,
                ),
                resolved_payload=normalized_payload,
            )
        except httpx.HTTPError:
            return ZoteroVerificationResult(
                status=ConnectionStatus.ERROR,
                metadata_json=self._zotero_error_metadata(
                    metadata=metadata,
                    detail="Could not reach Zotero to verify the connection.",
                    preserve_verified_identity=preserve_verified_identity,
                ),
                resolved_payload=normalized_payload,
            )

        user_id = str(key_info.user_id) if key_info.user_id is not None else None
        access = key_info.access or {}
        scope = access.get("user", {}) if normalized_payload["library_type"] == "users" else access.get("groups", {}).get("all", {})
        can_access_library = bool(scope.get("library"))
        can_write = bool(scope.get("write"))
        last_error: str | None = None
        effective_library_id = normalized_payload["library_id"]

        if normalized_payload["library_type"] == "users":
            if not user_id:
                last_error = "Zotero did not return a user ID for this API key."
            elif self._is_zotero_personal_library_alias(normalized_payload["library_id"]):
                effective_library_id = user_id
            elif normalized_payload["library_id"] != user_id:
                last_error = (
                    f"API key belongs to user {user_id}, but the configured library ID is "
                    f"{normalized_payload['library_id']}."
                )
        if not last_error and not can_access_library:
            last_error = "API key does not include library access."
        if not last_error and not can_write:
            last_error = "API key does not include write access, so library saves stay disabled."

        resolved_payload = normalized_payload | {"library_id": effective_library_id}
        return ZoteroVerificationResult(
            status=ConnectionStatus.CONNECTED if not last_error else ConnectionStatus.ERROR,
            metadata_json=(metadata | {"library_id": effective_library_id})
            | {
                "connected_username": key_info.username,
                "connected_user_id": user_id,
                "can_access_library": can_access_library,
                "can_write": can_write,
                "verified_at": datetime.now(UTC).isoformat(),
                "last_error": last_error,
            },
            resolved_payload=resolved_payload,
        )

    def _zotero_error_metadata(
        self,
        *,
        metadata: dict,
        detail: str,
        preserve_verified_identity: bool,
    ) -> dict:
        if preserve_verified_identity:
            return metadata | {"can_access_library": False, "can_write": False, "last_error": detail}
        return metadata | {
            "connected_username": None,
            "connected_user_id": None,
            "can_access_library": False,
            "can_write": False,
            "verified_at": None,
            "last_error": detail,
        }

    def _verify_gmail_imap_payload(
        self,
        *,
        payload: dict[str, str],
        base_metadata: dict,
    ) -> GmailVerificationResult:
        email_address = payload["email"]
        app_password = payload["app_password"]
        metadata = base_metadata | {
            "auth_mode": "app_password",
            "connected_email": email_address or base_metadata.get("connected_email"),
        }

        if not email_address:
            return GmailVerificationResult(
                status=ConnectionStatus.ERROR,
                metadata_json={key: value for key, value in (metadata | {"last_error": "Gmail address is required."}).items() if value is not None},
                resolved_payload=payload,
            )
        if not app_password:
            return GmailVerificationResult(
                status=ConnectionStatus.ERROR,
                metadata_json={key: value for key, value in (metadata | {"last_error": "Gmail app password is required."}).items() if value is not None},
                resolved_payload=payload,
            )

        try:
            GmailImapConnector(email_address=email_address, app_password=app_password).test_connection()
        except (imaplib.IMAP4.error, OSError, RuntimeError) as exc:
            detail = str(exc).strip() or "Could not authenticate to Gmail IMAP."
            return GmailVerificationResult(
                status=ConnectionStatus.ERROR,
                metadata_json={key: value for key, value in (metadata | {"last_error": detail}).items() if value is not None},
                resolved_payload=payload,
            )

        return GmailVerificationResult(
            status=ConnectionStatus.CONNECTED,
            metadata_json={key: value for key, value in (metadata | {"last_error": None}).items() if value is not None},
            resolved_payload=payload,
        )

    def _zotero_metadata_needs_refresh(self, connection: ConnectionSecret) -> bool:
        metadata = connection.metadata_json or {}
        if connection.status != ConnectionStatus.CONNECTED:
            return not metadata.get("library_id")
        return any(
            metadata.get(field) in (None, "")
            for field in ("connected_username", "connected_user_id", "verified_at", "library_id")
        ) or "can_write" not in metadata
