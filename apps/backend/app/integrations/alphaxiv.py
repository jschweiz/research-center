from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse

from dateutil import parser as date_parser

from app.core.outbound import fetch_safe_response

ALPHAXIV_API_BASE = "https://api.alphaxiv.org/papers/v3"
ALPHAXIV_AUDIO_BASE = "https://paper-podcasts.alphaxiv.org"
ALPHAXIV_WEB_BASE = "https://www.alphaxiv.org/"
ALPHAXIV_FEED_URL = f"{ALPHAXIV_API_BASE}/feed"


@dataclass(frozen=True)
class AlphaXivFeedPaper:
    paper_id: str
    abs_url: str
    title: str
    published_at: datetime | None
    summary: str | None
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class AlphaXivFeedPage:
    page_num: int
    page_size: int
    papers: list[AlphaXivFeedPaper]
    raw_paper_count: int


@dataclass(frozen=True)
class AlphaXivPaperBundle:
    paper_id: str
    abs_url: str
    title: str
    abstract: str
    authors: list[str]
    topics: list[str]
    published_at: datetime | None
    first_published_at: datetime | None
    updated_at: datetime | None
    version_label: str | None
    version_order: int | None
    version_id: str | None
    canonical_id: str | None
    group_id: str | None
    source_url: str | None
    pdf_url: str | None
    image_url: str | None
    license: str | None
    citations_count: int | None
    citation_bibtex: str | None
    resources: list[Any]
    versions: list[Any]
    github_url: str | None
    github_stars: int | None
    metrics: dict[str, Any]
    summary: dict[str, Any]
    overview_status: dict[str, Any] | None
    overview: dict[str, Any] | None
    ai_detection: dict[str, Any] | None
    transcript: list[dict[str, Any]]
    podcast_url: str | None
    podcast_audio: bytes | None
    transcript_url: str | None
    paper_payload: dict[str, Any]
    preview_payload: dict[str, Any]
    legacy_payload: dict[str, Any]
    similar_papers: list[dict[str, Any]]

    @property
    def short_summary(self) -> str | None:
        for payload in (self.summary, self.overview.get("summary") if isinstance(self.overview, dict) else None):
            if not isinstance(payload, dict):
                continue
            value = str(payload.get("summary") or "").strip()
            if value:
                return value
        return None

    @property
    def overview_markdown(self) -> str | None:
        if not isinstance(self.overview, dict):
            return None
        value = str(self.overview.get("overview") or "").strip()
        return value or None

    @property
    def overview_languages(self) -> list[str]:
        translations = (
            self.overview_status.get("translations")
            if isinstance(self.overview_status, dict)
            else None
        )
        if not isinstance(translations, dict):
            return []
        return sorted(
            language
            for language, payload in translations.items()
            if isinstance(language, str)
            and isinstance(payload, dict)
            and str(payload.get("state") or "").strip().lower() == "done"
        )

    def metadata_payload(self) -> dict[str, Any]:
        ai_detection = self.ai_detection if isinstance(self.ai_detection, dict) else {}
        return {
            "paper_id": self.paper_id,
            "canonical_id": self.canonical_id,
            "group_id": self.group_id,
            "version_id": self.version_id,
            "title": self.title,
            "abstract": self.abstract,
            "short_summary": self.short_summary,
            "authors": self.authors,
            "topics": self.topics,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "first_published_at": self.first_published_at.isoformat() if self.first_published_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "version_label": self.version_label,
            "version_order": self.version_order,
            "license": self.license,
            "citations_count": self.citations_count,
            "source_url": self.source_url,
            "alphaxiv_url": self.abs_url,
            "pdf_url": self.pdf_url,
            "image_url": self.image_url,
            "github_url": self.github_url,
            "github_stars": self.github_stars,
            "resources": self.resources,
            "versions": self.versions,
            "metrics": self.metrics,
            "podcast_url": self.podcast_url,
            "transcript_url": self.transcript_url,
            "transcript_line_count": len(self.transcript),
            "overview_languages": self.overview_languages,
            "similar_papers_count": len(self.similar_papers),
            "similar_paper_titles": [
                str(paper.get("title") or "").strip()
                for paper in self.similar_papers
                if isinstance(paper, dict) and str(paper.get("title") or "").strip()
            ],
            "ai_detection": {
                "state": ai_detection.get("state"),
                "prediction_short": ai_detection.get("predictionShort"),
                "headline": ai_detection.get("headline"),
                "fraction_ai": ai_detection.get("fractionAi"),
                "fraction_ai_assisted": ai_detection.get("fractionAiAssisted"),
                "fraction_human": ai_detection.get("fractionHuman"),
            }
            if ai_detection
            else None,
        }


class AlphaXivClient:
    def __init__(self, timeout_seconds: int = 20) -> None:
        self.timeout_seconds = timeout_seconds

    def fetch_feed_page(
        self,
        *,
        page_num: int,
        page_size: int,
        sort: str,
        interval: str,
        topics: list[str] | None = None,
        organizations: list[str] | None = None,
        source: str | None = None,
    ) -> AlphaXivFeedPage:
        params: dict[str, str | int] = {
            "pageNum": max(page_num, 1),
            "pageSize": max(page_size, 1),
            "sort": sort,
            "interval": interval,
        }
        normalized_topics = [str(topic).strip() for topic in (topics or []) if str(topic).strip()]
        normalized_organizations = [
            str(organization).strip()
            for organization in (organizations or [])
            if str(organization).strip()
        ]
        if normalized_topics:
            params["topics"] = json.dumps(normalized_topics)
        if normalized_organizations:
            params["organizations"] = json.dumps(normalized_organizations)
        if source:
            params["source"] = source

        payload = self._fetch_required_json(f"{ALPHAXIV_FEED_URL}?{urlencode(params)}")
        papers_payload = payload.get("papers")
        if not isinstance(papers_payload, list):
            raise ValueError("Expected alphaXiv feed response to include a papers list.")

        papers: list[AlphaXivFeedPaper] = []
        for entry in papers_payload:
            if not isinstance(entry, dict):
                continue
            parsed_entry = self._parse_feed_paper(entry)
            if parsed_entry is not None:
                papers.append(parsed_entry)

        return AlphaXivFeedPage(
            page_num=max(page_num, 1),
            page_size=max(page_size, 1),
            papers=papers,
            raw_paper_count=len(papers_payload),
        )

    def fetch_paper(self, url: str) -> AlphaXivPaperBundle:
        paper_id = self.paper_id_from_url(url)
        paper_payload = self._fetch_required_json(f"{ALPHAXIV_API_BASE}/{paper_id}")
        preview_payload = self._fetch_required_json(f"{ALPHAXIV_API_BASE}/{paper_id}/preview")
        legacy_payload = self._fetch_required_json(f"{ALPHAXIV_API_BASE}/legacy/{paper_id}")
        similar_papers = self._coerce_dict_list(
            self._fetch_optional_json(f"{ALPHAXIV_API_BASE}/{paper_id}/similar-papers")
        )

        version_id = self._string_or_none(preview_payload.get("version_id")) or self._string_or_none(
            paper_payload.get("versionId")
        )
        overview_status = (
            self._fetch_optional_json(f"{ALPHAXIV_API_BASE}/{version_id}/overview/status")
            if version_id
            else None
        )
        overview = (
            self._fetch_optional_json(f"{ALPHAXIV_API_BASE}/{version_id}/overview/en")
            if version_id and self._overview_ready(overview_status, language="en")
            else None
        )
        ai_detection = (
            self._fetch_optional_json(f"{ALPHAXIV_API_BASE}/{version_id}/ai-detection")
            if version_id
            else None
        )

        legacy_paper = legacy_payload.get("paper") if isinstance(legacy_payload, dict) else {}
        legacy_group = legacy_paper.get("paper_group") if isinstance(legacy_paper, dict) else {}
        legacy_version = legacy_paper.get("paper_version") if isinstance(legacy_paper, dict) else {}
        legacy_pdf = legacy_paper.get("pdf_info") if isinstance(legacy_paper, dict) else {}
        group_id = self._string_or_none(legacy_group.get("id")) or self._string_or_none(paper_payload.get("groupId"))

        transcript_url = f"{ALPHAXIV_AUDIO_BASE}/{group_id}/transcript.json" if group_id else None
        transcript = self._coerce_dict_list(
            self._fetch_optional_json(transcript_url) if transcript_url else None
        )
        podcast_url = f"{ALPHAXIV_AUDIO_BASE}/{group_id}/podcast.mp3" if group_id else None
        podcast_audio = self._fetch_optional_bytes(podcast_url) if podcast_url else None

        authors = self._extract_author_names(preview_payload, legacy_paper)
        topics = self._extract_topics(preview_payload, legacy_group)
        summary = preview_payload.get("paper_summary") if isinstance(preview_payload.get("paper_summary"), dict) else {}
        metrics = (
            preview_payload.get("metrics")
            if isinstance(preview_payload.get("metrics"), dict)
            else legacy_group.get("metrics")
            if isinstance(legacy_group, dict)
            else {}
        )

        return AlphaXivPaperBundle(
            paper_id=paper_id,
            abs_url=self._normalize_abs_url(url, paper_id=paper_id),
            title=self._string_or_none(paper_payload.get("title"))
            or self._string_or_none(preview_payload.get("title"))
            or self._string_or_none(legacy_version.get("title"))
            or paper_id,
            abstract=self._string_or_none(paper_payload.get("abstract"))
            or self._string_or_none(preview_payload.get("abstract"))
            or "",
            authors=authors,
            topics=topics,
            published_at=self._parse_datetime(
                paper_payload.get("publicationDate") or preview_payload.get("publication_date")
            ),
            first_published_at=self._parse_datetime(
                paper_payload.get("firstPublicationDate") or preview_payload.get("first_publication_date")
            ),
            updated_at=self._parse_datetime(preview_payload.get("updated_at")),
            version_label=self._string_or_none(paper_payload.get("versionLabel")),
            version_order=self._int_or_none(paper_payload.get("versionOrder")),
            version_id=version_id,
            canonical_id=self._string_or_none(preview_payload.get("canonical_id")),
            group_id=group_id,
            source_url=(
                self._string_or_none(paper_payload.get("sourceUrl"))
                or (
                    self._string_or_none(legacy_group.get("source", {}).get("url"))
                    if isinstance(legacy_group, dict)
                    else None
                )
            ),
            pdf_url=(
                self._string_or_none(legacy_pdf.get("fetcher_url"))
                if isinstance(legacy_pdf, dict)
                else None
            ),
            image_url=self._normalize_image_url(
                self._string_or_none(preview_payload.get("image_url"))
                or self._string_or_none(legacy_version.get("imageURL"))
            ),
            license=self._string_or_none(paper_payload.get("license"))
            or self._string_or_none(legacy_version.get("license")),
            citations_count=self._int_or_none(paper_payload.get("citationsCount")),
            citation_bibtex=self._string_or_none(paper_payload.get("citationBibtex")),
            resources=list(paper_payload.get("resources") or []) if isinstance(paper_payload, dict) else [],
            versions=list(paper_payload.get("versions") or []) if isinstance(paper_payload, dict) else [],
            github_url=self._string_or_none(preview_payload.get("github_url")),
            github_stars=self._int_or_none(preview_payload.get("github_stars")),
            metrics=metrics if isinstance(metrics, dict) else {},
            summary=summary,
            overview_status=overview_status if isinstance(overview_status, dict) else None,
            overview=overview if isinstance(overview, dict) else None,
            ai_detection=ai_detection if isinstance(ai_detection, dict) else None,
            transcript=transcript,
            podcast_url=podcast_url,
            podcast_audio=podcast_audio,
            transcript_url=transcript_url,
            paper_payload=paper_payload if isinstance(paper_payload, dict) else {},
            preview_payload=preview_payload if isinstance(preview_payload, dict) else {},
            legacy_payload=legacy_payload if isinstance(legacy_payload, dict) else {},
            similar_papers=similar_papers,
        )

    def paper_id_from_url(self, url: str) -> str:
        candidate = str(url or "").strip()
        parsed = urlparse(candidate)
        path = parsed.path.strip("/")
        for prefix in ("abs/", "overview/", "resources/"):
            if path.startswith(prefix):
                identifier = path[len(prefix) :].strip("/")
                if identifier:
                    return self._strip_version_suffix(identifier)
        if candidate and "/" not in candidate and "." in candidate:
            return self._strip_version_suffix(candidate)
        raise ValueError(f"Could not determine alphaXiv paper id from URL: {url}")

    def _fetch_required_json(self, url: str) -> dict[str, Any]:
        response = fetch_safe_response(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Expected JSON object from alphaXiv endpoint: {url}")
        return payload

    def _fetch_optional_json(self, url: str) -> dict[str, Any] | list[Any] | None:
        response = fetch_safe_response(url, timeout=self.timeout_seconds)
        if response.status_code >= 400:
            return None
        payload = response.json()
        if isinstance(payload, (dict, list)):
            return payload
        return None

    def _fetch_optional_bytes(self, url: str) -> bytes | None:
        response = fetch_safe_response(url, timeout=self.timeout_seconds)
        if response.status_code >= 400:
            return None
        return response.content or None

    def _normalize_abs_url(self, url: str, *, paper_id: str) -> str:
        return urljoin(ALPHAXIV_WEB_BASE, f"abs/{paper_id}")

    def _normalize_image_url(self, value: str | None) -> str | None:
        if not value:
            return None
        return urljoin(ALPHAXIV_WEB_BASE, value)

    def _parse_feed_paper(self, payload: dict[str, Any]) -> AlphaXivFeedPaper | None:
        paper_id = self._resolve_feed_paper_id(payload)
        if not paper_id:
            return None

        title = (
            self._string_or_none(payload.get("title"))
            or self._string_or_none(payload.get("paperTitle"))
            or paper_id
        )
        summary_payload = (
            payload.get("paper_summary")
            if isinstance(payload.get("paper_summary"), dict)
            else payload.get("paperSummary")
            if isinstance(payload.get("paperSummary"), dict)
            else None
        )
        summary = (
            self._string_or_none(summary_payload.get("summary"))
            if isinstance(summary_payload, dict)
            else self._string_or_none(payload.get("summary"))
        )
        published_at = self._parse_datetime(
            payload.get("publicationDate")
            or payload.get("publication_date")
            or payload.get("firstPublicationDate")
            or payload.get("first_publication_date")
            or payload.get("publishedAt")
            or payload.get("published_at")
        )

        return AlphaXivFeedPaper(
            paper_id=paper_id,
            abs_url=self._normalize_abs_url("", paper_id=paper_id),
            title=title,
            published_at=published_at,
            summary=summary,
            raw_payload=dict(payload),
        )

    def _resolve_feed_paper_id(self, payload: dict[str, Any]) -> str | None:
        candidate_payloads = [payload]
        for key in ("paper", "preview"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                candidate_payloads.append(nested)

        for candidate_payload in candidate_payloads:
            for key in (
                "universalId",
                "universal_id",
                "paperId",
                "paper_id",
                "canonicalId",
                "canonical_id",
            ):
                value = self._string_or_none(candidate_payload.get(key))
                if value:
                    return self._strip_version_suffix(value)

        for candidate_payload in candidate_payloads:
            for key in (
                "sourceUrl",
                "source_url",
                "alphaxiv_url",
                "alphaxivUrl",
                "abs_url",
                "absUrl",
                "url",
            ):
                value = self._string_or_none(candidate_payload.get(key))
                if not value:
                    continue
                try:
                    return self.paper_id_from_url(value)
                except ValueError:
                    continue

        return None

    def _extract_author_names(self, preview_payload: dict[str, Any], legacy_paper: dict[str, Any]) -> list[str]:
        authors = [
            str(author or "").strip()
            for author in (preview_payload.get("authors") or [])
            if str(author or "").strip()
        ]
        if authors:
            return authors
        legacy_authors = legacy_paper.get("authors") if isinstance(legacy_paper, dict) else []
        return [
            str(author.get("full_name") or "").strip()
            for author in legacy_authors or []
            if isinstance(author, dict) and str(author.get("full_name") or "").strip()
        ]

    def _extract_topics(self, preview_payload: dict[str, Any], legacy_group: dict[str, Any]) -> list[str]:
        topics = [
            str(topic or "").strip()
            for topic in (preview_payload.get("topics") or [])
            if str(topic or "").strip()
        ]
        if topics:
            return topics
        return [
            str(topic or "").strip()
            for topic in (legacy_group.get("topics") or [])
            if str(topic or "").strip()
        ]

    def _overview_ready(self, overview_status: dict[str, Any] | list[Any] | None, *, language: str) -> bool:
        if not isinstance(overview_status, dict):
            return False
        translations = overview_status.get("translations")
        if not isinstance(translations, dict):
            return False
        translation = translations.get(language)
        return isinstance(translation, dict) and str(translation.get("state") or "").strip().lower() == "done"

    def _strip_version_suffix(self, value: str) -> str:
        candidate = str(value or "").strip().strip("/")
        if len(candidate) >= 3 and candidate[-1].isdigit():
            prefix, version = candidate.rsplit("v", 1) if "v" in candidate else (candidate, "")
            if prefix and version.isdigit():
                return prefix
        return candidate

    def _parse_datetime(self, value: Any) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if abs(timestamp) > 100_000_000_000:
                timestamp /= 1000.0
            return datetime.fromtimestamp(timestamp, tz=UTC)
        try:
            parsed = date_parser.parse(str(value))
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)

    def _coerce_dict_list(self, payload: Any) -> list[dict[str, Any]]:
        if not isinstance(payload, list):
            return []
        return [entry for entry in payload if isinstance(entry, dict)]

    def _string_or_none(self, value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None

    def _int_or_none(self, value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
