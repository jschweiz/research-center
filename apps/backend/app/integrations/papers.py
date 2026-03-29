from __future__ import annotations

import re
from typing import Any

import httpx
from bs4 import BeautifulSoup

DOI_PATTERN = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.IGNORECASE)


class PaperMetadataClient:
    def __init__(self, timeout_seconds: int = 20) -> None:
        self.timeout_seconds = timeout_seconds

    def extract_doi(self, text: str) -> str | None:
        match = DOI_PATTERN.search(text)
        return match.group(0) if match else None

    def enrich(self, *, title: str, text: str, url: str) -> dict[str, Any]:
        doi = self.extract_doi(text) or self.extract_doi(url)
        metadata: dict[str, Any] = {}
        if doi:
            metadata["doi"] = doi
            try:
                metadata.update(self._crossref(doi))
            except Exception:
                metadata["crossref_error"] = "unavailable"
            try:
                metadata.update(self._semantic_scholar(doi))
            except Exception:
                metadata["semantic_scholar_error"] = "unavailable"
        return metadata

    def _crossref(self, doi: str) -> dict[str, Any]:
        response = httpx.get(
            f"https://api.crossref.org/works/{doi}",
            headers={"User-Agent": "ResearchCenter/0.1"},
            timeout=self.timeout_seconds,
        )
        if response.status_code >= 400:
            return {}
        message = response.json().get("message", {})
        return {
            "crossref_title": (message.get("title") or [None])[0],
            "crossref_abstract": self._normalize_abstract(message.get("abstract")),
            "crossref_publisher": message.get("publisher"),
        }

    def _semantic_scholar(self, doi: str) -> dict[str, Any]:
        response = httpx.get(
            f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}",
            params={"fields": "title,abstract,authors.name,venue,year,url,citationCount"},
            timeout=self.timeout_seconds,
        )
        if response.status_code >= 400:
            return {}
        payload = response.json()
        return {
            "semantic_scholar_title": payload.get("title"),
            "semantic_scholar_abstract": self._normalize_abstract(payload.get("abstract")),
            "semantic_scholar_authors": [
                author.get("name") for author in payload.get("authors", []) if author.get("name")
            ],
            "semantic_scholar_citation_count": payload.get("citationCount"),
            "semantic_scholar_url": payload.get("url"),
            "semantic_scholar_venue": payload.get("venue"),
        }

    def _normalize_abstract(self, value: str | None) -> str | None:
        if not value:
            return None
        return " ".join(BeautifulSoup(value, "html.parser").get_text(" ", strip=True).split())
