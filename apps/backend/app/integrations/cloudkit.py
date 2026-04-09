from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from app.core.config import Settings, get_settings

CLOUDKIT_BASE_URL = "https://api.apple-cloudkit.com"


class CloudKitError(RuntimeError):
    pass


@dataclass(frozen=True)
class CloudKitAssetReference:
    file_checksum: str
    receipt: str
    reference_checksum: str
    size: int
    wrapping_key: str

    def as_field_value(self) -> dict[str, Any]:
        return {
            "fileChecksum": self.file_checksum,
            "receipt": self.receipt,
            "referenceChecksum": self.reference_checksum,
            "size": self.size,
            "wrappingKey": self.wrapping_key,
        }


class CloudKitClient:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.http_client = http_client or httpx.Client(timeout=60.0)
        self._owns_client = http_client is None

    def close(self) -> None:
        if self._owns_client:
            self.http_client.close()

    def __enter__(self) -> CloudKitClient:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @property
    def record_type(self) -> str:
        return self.settings.cloudkit_record_type

    def require_write_configuration(self) -> None:
        if not self.settings.cloudkit_write_configured:
            raise CloudKitError("CloudKit write configuration is incomplete.")

    def _database_path(self, subpath: str) -> str:
        container = self.settings.cloudkit_container_identifier
        if not container:
            raise CloudKitError("CloudKit container identifier is not configured.")
        normalized = subpath.lstrip("/")
        return (
            f"/database/1/{container}/{self.settings.cloudkit_environment}/"
            f"{self.settings.cloudkit_database}/{normalized}"
        )

    def _database_url(self, subpath: str) -> str:
        return f"{CLOUDKIT_BASE_URL}{self._database_path(subpath)}"

    def _load_private_key(self):
        private_key = self.settings.cloudkit_server_to_server_private_key_pem
        if not private_key:
            raise CloudKitError("CloudKit server-to-server private key is not configured.")
        normalized = private_key.replace("\\n", "\n").encode("utf-8")
        return serialization.load_pem_private_key(normalized, password=None)

    def _signed_headers(self, *, subpath: str, body: bytes) -> dict[str, str]:
        self.require_write_configuration()
        iso8601 = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        message = f"{iso8601}:{body.decode('utf-8')}:{self._database_path(subpath)}".encode()
        signature = self._load_private_key().sign(message, ec.ECDSA(hashes.SHA256()))
        return {
            "Content-Type": "application/json; charset=utf-8",
            "X-Apple-CloudKit-Request-KeyID": self.settings.cloudkit_server_to_server_key_id or "",
            "X-Apple-CloudKit-Request-ISO8601Date": iso8601,
            "X-Apple-CloudKit-Request-SignatureV1": base64.b64encode(signature).decode("ascii"),
        }

    def _post_signed_json(self, subpath: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        response = self.http_client.post(
            self._database_url(subpath),
            content=body,
            headers=self._signed_headers(subpath=subpath, body=body),
        )
        response.raise_for_status()
        data = response.json()
        self._raise_for_cloudkit_error(data)
        return data

    def lookup_records(self, record_names: list[str]) -> list[dict[str, Any]]:
        if not record_names:
            return []
        payload = {"records": [{"recordName": record_name} for record_name in record_names]}
        data = self._post_signed_json("records/lookup", payload)
        return list(data.get("records") or [])

    def upload_asset(
        self,
        *,
        field_name: str,
        data: bytes,
        record_type: str | None = None,
        filename: str | None = None,
    ) -> CloudKitAssetReference:
        record_type = record_type or self.record_type
        token_payload = {
            "tokens": [
                {
                    "recordType": record_type,
                    "fieldName": field_name,
                    "desiredSize": len(data),
                    "fileChecksum": self._sha256(data),
                }
            ]
        }
        token_response = self._post_signed_json("assets/upload", token_payload)
        token = self._extract_upload_token(token_response)
        upload_url = self._first_non_empty(
            token.get("url"),
            token.get("uploadURL"),
            self._nested_value(token, "singleFile", "url"),
        )
        if not upload_url:
            raise CloudKitError("CloudKit asset upload URL is missing from the upload token.")

        upload_headers = {
            str(key): str(value)
            for key, value in (
                token.get("headers")
                or self._nested_value(token, "singleFile", "headers")
                or {}
            ).items()
        }
        if filename:
            upload_headers.setdefault("Content-Disposition", f'attachment; filename="{filename}"')

        method = str(token.get("method") or "PUT").upper()
        upload_response = self.http_client.request(
            method,
            upload_url,
            content=data,
            headers=upload_headers,
        )
        upload_response.raise_for_status()
        upload_metadata = self._maybe_json(upload_response) or {}
        asset_reference_payload = self._extract_asset_reference_payload(upload_metadata) or self._extract_asset_reference_payload(token)
        if not asset_reference_payload:
            raise CloudKitError("CloudKit asset upload response did not include an asset receipt.")
        return CloudKitAssetReference(
            file_checksum=str(asset_reference_payload["fileChecksum"]),
            receipt=str(asset_reference_payload["receipt"]),
            reference_checksum=str(asset_reference_payload["referenceChecksum"]),
            size=int(asset_reference_payload["size"]),
            wrapping_key=str(asset_reference_payload["wrappingKey"]),
        )

    def create_or_update_record(
        self,
        *,
        record_name: str,
        fields: dict[str, dict[str, Any]],
        record_type: str | None = None,
    ) -> dict[str, Any]:
        record_type = record_type or self.record_type
        existing = self.lookup_records([record_name])
        operation_type = "forceUpdate" if existing else "create"
        payload = {
            "operations": [
                {
                    "operationType": operation_type,
                    "record": {
                        "recordName": record_name,
                        "recordType": record_type,
                        "fields": fields,
                    },
                }
            ],
            "atomic": True,
        }
        data = self._post_signed_json("records/modify", payload)
        records = list(data.get("records") or [])
        if not records:
            raise CloudKitError("CloudKit modify response did not include a saved record.")
        return records[0]

    def build_public_read_url(self, subpath: str) -> str:
        api_token = self.settings.cloudkit_api_token
        if not api_token:
            raise CloudKitError("CloudKit API token is not configured.")
        return f"{self._database_url(subpath)}?ckAPIToken={api_token}"

    @staticmethod
    def wrap_fields(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {key: {"value": value} for key, value in payload.items()}

    def _extract_upload_token(self, payload: dict[str, Any]) -> dict[str, Any]:
        token_candidates = payload.get("tokens") or payload.get("results") or []
        if not token_candidates:
            raise CloudKitError("CloudKit asset upload did not return any upload tokens.")
        token = token_candidates[0]
        if not isinstance(token, dict):
            raise CloudKitError("CloudKit asset upload returned an invalid upload token payload.")
        return token

    def _extract_asset_reference_payload(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        if not payload:
            return None
        candidates = [
            payload,
            self._nested_value(payload, "singleFile"),
            self._nested_value(payload, "asset"),
            self._nested_value(payload, "uploadedFile"),
        ]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            required = ("fileChecksum", "receipt", "referenceChecksum", "size", "wrappingKey")
            if all(key in candidate for key in required):
                return candidate
        return None

    def _raise_for_cloudkit_error(self, payload: dict[str, Any]) -> None:
        if not payload:
            return
        server_error = payload.get("serverErrorCode")
        reason = payload.get("reason")
        if server_error:
            message = f"CloudKit error {server_error}"
            if reason:
                message = f"{message}: {reason}"
            raise CloudKitError(message)
        for record in payload.get("records") or []:
            if not isinstance(record, dict):
                continue
            record_error = record.get("serverErrorCode")
            if record_error:
                message = f"CloudKit record error {record_error}"
                if record.get("reason"):
                    message = f"{message}: {record['reason']}"
                raise CloudKitError(message)

    def _nested_value(self, payload: dict[str, Any], *keys: str) -> dict[str, Any] | None:
        current: Any = payload
        for key in keys:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current if isinstance(current, dict) else None

    def _maybe_json(self, response: httpx.Response) -> dict[str, Any] | None:
        content_type = response.headers.get("content-type", "")
        if "json" not in content_type.lower():
            return None
        try:
            payload = response.json()
        except ValueError:
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _sha256(data: bytes) -> str:
        digest = hashes.Hash(hashes.SHA256())
        digest.update(data)
        return base64.b64encode(digest.finalize()).decode("ascii")

    @staticmethod
    def _first_non_empty(*values: Any) -> str | None:
        for value in values:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None
