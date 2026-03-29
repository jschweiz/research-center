from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import httpx


@dataclass
class ZoteroExportResult:
    success: bool
    confidence_score: float
    detail: str
    response_payload: dict[str, Any]


@dataclass
class ZoteroKeyInfo:
    user_id: int | None
    username: str | None
    access: dict[str, Any]


class ZoteroClient:
    def __init__(self, api_key: str, library_id: str, library_type: str = "users") -> None:
        self.api_key = api_key
        self.library_id = library_id
        self.library_type = library_type
        self.base_url = f"https://api.zotero.org/{library_type}/{library_id}"

    def _headers(self) -> dict[str, str]:
        return {
            "Zotero-API-Key": self.api_key,
            "Zotero-API-Version": "3",
            "Content-Type": "application/json",
        }

    def _write_headers(self) -> dict[str, str]:
        return self._headers() | {"Zotero-Write-Token": uuid4().hex}

    def _collection_segments(self, collection_name: str) -> list[str]:
        return [segment.strip() for segment in collection_name.split("/") if segment.strip()]

    def _collection_list_url(self, parent_collection: str | None = None) -> str:
        if parent_collection:
            return f"{self.base_url}/collections/{parent_collection}/collections"
        return f"{self.base_url}/collections/top"

    def _list_collections(self, parent_collection: str | None = None) -> list[dict[str, Any]]:
        collections: list[dict[str, Any]] = []
        start = 0
        url = self._collection_list_url(parent_collection)
        while True:
            response = httpx.get(
                url,
                headers=self._headers(),
                params={"format": "json", "limit": 100, "start": start},
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list) or not payload:
                break
            collections.extend(entry for entry in payload if isinstance(entry, dict))
            if len(payload) < 100:
                break
            start += len(payload)
        return collections

    def _collection_key_from_payload(self, payload: Any) -> str | None:
        if isinstance(payload, str) and payload:
            return payload
        if not isinstance(payload, dict):
            return None
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("key"), str) and data.get("key"):
            return data["key"]
        if isinstance(payload.get("key"), str) and payload.get("key"):
            return payload["key"]
        return None

    def _success_entry(self, payload: dict[str, Any]) -> Any:
        successful = payload.get("successful")
        if isinstance(successful, dict) and successful:
            return next(iter(successful.values()))
        success = payload.get("success")
        if isinstance(success, dict) and success:
            return next(iter(success.values()))
        return None

    def _write_error_detail(self, payload: dict[str, Any]) -> str | None:
        failed = payload.get("failed")
        if not isinstance(failed, dict) or not failed:
            return None
        first_failure = next(iter(failed.values()))
        if isinstance(first_failure, dict):
            detail = first_failure.get("message") or first_failure.get("error")
            if isinstance(detail, str) and detail.strip():
                return detail.strip()
        elif isinstance(first_failure, str) and first_failure.strip():
            return first_failure.strip()
        return None

    def _json_body(self, response: httpx.Response) -> dict[str, Any] | list[Any] | None:
        if not response.text:
            return None
        try:
            payload = response.json()
        except ValueError:
            return None
        if isinstance(payload, (dict, list)):
            return payload
        return None

    def _find_collection_key(
        self,
        *,
        name: str,
        parent_collection: str | None = None,
    ) -> str | None:
        for entry in self._list_collections(parent_collection):
            data = entry.get("data") if isinstance(entry.get("data"), dict) else entry
            if not isinstance(data, dict):
                continue
            if data.get("name") != name:
                continue
            key = self._collection_key_from_payload(entry) or self._collection_key_from_payload(data)
            if key:
                return key
        return None

    def _create_collection(
        self,
        *,
        name: str,
        parent_collection: str | None = None,
    ) -> str:
        collection_payload: dict[str, Any] = {"name": name}
        if parent_collection:
            collection_payload["parentCollection"] = parent_collection
        response = httpx.post(
            f"{self.base_url}/collections",
            headers=self._write_headers(),
            json=[collection_payload],
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json() if response.text else {}
        if not isinstance(payload, dict):
            raise RuntimeError("Zotero collection creation returned an unexpected response.")
        collection_key = self._collection_key_from_payload(self._success_entry(payload))
        if collection_key:
            return collection_key
        detail = self._write_error_detail(payload) or "Zotero did not return a collection key."
        raise RuntimeError(f"Zotero collection creation failed: {detail}")

    def ensure_collection(self, collection_name: str) -> str:
        segments = self._collection_segments(collection_name)
        if not segments:
            raise ValueError("Zotero collection name is invalid.")
        parent_collection: str | None = None
        for segment in segments:
            collection_key = self._find_collection_key(name=segment, parent_collection=parent_collection)
            if not collection_key:
                collection_key = self._create_collection(name=segment, parent_collection=parent_collection)
            parent_collection = collection_key
        if not parent_collection:
            raise RuntimeError("Zotero collection resolution failed.")
        return parent_collection

    def get_current_key_info(self) -> ZoteroKeyInfo:
        response = httpx.get(
            "https://api.zotero.org/keys/current",
            headers=self._headers(),
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        return ZoteroKeyInfo(
            user_id=payload.get("userID"),
            username=payload.get("username"),
            access=payload.get("access") or {},
        )

    def save_item(
        self,
        *,
        item: dict[str, Any],
        insight: dict[str, Any],
        tags: list[str],
        note_prefix: str | None = None,
        collection_name: str | None = None,
    ) -> ZoteroExportResult:
        confidence = 0.92 if item.get("title") and item.get("canonical_url") else 0.45
        zotero_item_type = "journalArticle" if item.get("content_type") == "paper" else "webpage"
        note = "\n\n".join(
            part
            for part in [
                note_prefix,
                insight.get("short_summary"),
                insight.get("why_it_matters"),
                "\n".join(f"- {question}" for question in insight.get("follow_up_questions", [])),
            ]
            if part
        )
        normalized_collection_name = str(collection_name or "").strip()
        collection_key: str | None = None

        if confidence < 0.65:
            payload = [
                {
                    "itemType": zotero_item_type,
                    "title": item.get("title"),
                    "url": item.get("canonical_url"),
                }
            ]
            return ZoteroExportResult(
                success=False,
                confidence_score=confidence,
                detail="Metadata confidence too low for automatic export.",
                response_payload={
                    "collection_name": normalized_collection_name or None,
                    "payload": payload,
                },
            )

        if normalized_collection_name:
            try:
                collection_key = self.ensure_collection(normalized_collection_name)
            except httpx.HTTPStatusError as exc:
                return ZoteroExportResult(
                    success=False,
                    confidence_score=confidence,
                    detail=f"Zotero collection setup failed with status {exc.response.status_code}.",
                    response_payload={
                        "collection_name": normalized_collection_name,
                        "body": exc.response.text,
                    },
                )
            except httpx.HTTPError:
                return ZoteroExportResult(
                    success=False,
                    confidence_score=confidence,
                    detail="Could not reach Zotero while preparing the destination collection.",
                    response_payload={"collection_name": normalized_collection_name},
                )
            except (RuntimeError, ValueError) as exc:
                return ZoteroExportResult(
                    success=False,
                    confidence_score=confidence,
                    detail=str(exc),
                    response_payload={"collection_name": normalized_collection_name},
                )

        zotero_item: dict[str, Any] = {
            "itemType": zotero_item_type,
            "title": item.get("title"),
            "url": item.get("canonical_url"),
            "abstractNote": insight.get("short_summary") or "",
            "creators": [
                {"creatorType": "author", "name": author} for author in item.get("authors", [])
            ],
            "tags": [{"tag": tag} for tag in tags],
            "date": item.get("published_at") or "",
            "extra": note or "",
        }
        if collection_key:
            zotero_item["collections"] = [collection_key]
        payload = [zotero_item]

        response = httpx.post(
            f"{self.base_url}/items",
            headers=self._write_headers(),
            json=payload,
            timeout=20,
        )
        if response.status_code >= 400:
            response_body = self._json_body(response)
            detail = self._write_error_detail(response_body) if isinstance(response_body, dict) else None
            return ZoteroExportResult(
                success=False,
                confidence_score=confidence,
                detail=detail or f"Zotero export failed with status {response.status_code}.",
                response_payload={
                    "body": response.text,
                    "collection_name": normalized_collection_name or None,
                },
            )

        response_payload = self._json_body(response) or {}
        if isinstance(response_payload, dict) and collection_key:
            response_payload = response_payload | {
                "collection_name": normalized_collection_name,
                "collection_key": collection_key,
            }
        return ZoteroExportResult(
            success=True,
            confidence_score=confidence,
            detail="Saved to Zotero.",
            response_payload=response_payload,
        )

    def sync_library_items(self, limit: int = 50) -> list[dict[str, Any]]:
        response = httpx.get(
            f"{self.base_url}/items",
            headers=self._headers(),
            params={"limit": limit, "format": "json"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        items: list[dict[str, Any]] = []
        for entry in payload:
            data = entry.get("data", {})
            items.append(
                {
                    "key": data.get("key"),
                    "title": data.get("title"),
                    "url": data.get("url"),
                    "authors": [creator.get("name") for creator in data.get("creators", []) if creator.get("name")],
                    "date": data.get("date"),
                    "tags": [tag.get("tag") for tag in data.get("tags", []) if tag.get("tag")],
                }
            )
        return items
