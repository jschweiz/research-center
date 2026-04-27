from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urljoin, urlparse

import trafilatura
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from app.core.outbound import fetch_safe_response


@dataclass
class ExtractedContent:
    title: str
    cleaned_text: str
    outbound_links: list[str]
    published_at: datetime | None
    mime_type: str | None
    extraction_confidence: float
    raw_payload: dict


class ContentExtractor:
    def __init__(self, timeout_seconds: int = 20) -> None:
        self.timeout_seconds = timeout_seconds

    @staticmethod
    def _content_fingerprint(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())

    def _looks_title_only(self, *, title: str, text: str) -> bool:
        normalized_title = self._content_fingerprint(title)
        normalized_text = self._content_fingerprint(text)
        return bool(normalized_title) and normalized_text == normalized_title

    def _extract_primary_content_text(self, soup: BeautifulSoup, *, title: str) -> str | None:
        for selector in (
            "article",
            '[role="main"]',
            "main",
            ".post-content",
            ".article-content",
            ".entry-content",
            ".content",
        ):
            for element in soup.select(selector):
                candidate_html = str(element)
                candidate = trafilatura.extract(
                    candidate_html,
                    include_links=True,
                    include_comments=False,
                    output_format="txt",
                    favor_precision=True,
                )
                cleaned = (candidate or "").strip()
                if self._looks_title_only(title=title, text=cleaned):
                    cleaned = ""
                if not cleaned:
                    cleaned = element.get_text("\n", strip=True)
                cleaned = cleaned.strip()
                if cleaned and not self._looks_title_only(title=title, text=cleaned):
                    return cleaned
        return None

    @staticmethod
    def _meta_description(soup: BeautifulSoup) -> str | None:
        for selector in (
            'meta[name="description"]',
            'meta[property="og:description"]',
            'meta[name="twitter:description"]',
        ):
            element = soup.select_one(selector)
            content = str(element.get("content") or "").strip() if element else ""
            if content:
                return content
        return None

    def normalize_title(self, title: str, *, url: str | None = None) -> str:
        cleaned = re.sub(r"\s+", " ", str(title or "")).strip()
        if not cleaned:
            return ""
        hostname = (urlparse(str(url or "")).hostname or "").lower()
        if hostname.endswith("anthropic.com"):
            cleaned = re.sub(
                r"\s+(?:\\|\||-|–|—|·|/)\s+Anthropic$",
                "",
                cleaned,
                flags=re.IGNORECASE,
            ).strip()
        return cleaned

    def extract_published_at_from_html(self, html: str) -> datetime | None:
        payload = str(html or "").strip()
        if not payload:
            return None
        soup = BeautifulSoup(payload, "html.parser")
        return self._parse_published_at(soup)

    def _parse_datetime_candidate(self, value: str) -> datetime | None:
        candidate = str(value or "").strip()
        if not candidate:
            return None
        try:
            parsed = date_parser.parse(candidate)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)

    def _best_published_at_candidate(self, candidates: list[str]) -> datetime | None:
        fallback: datetime | None = None
        for candidate in candidates:
            parsed = self._parse_datetime_candidate(candidate)
            if parsed is None:
                continue
            if parsed.time() != datetime.min.time():
                return parsed
            fallback = fallback or parsed
        return fallback

    def _json_ld_published_candidates(self, soup: BeautifulSoup) -> list[str]:
        candidates: list[str] = []

        def collect(node: object) -> None:
            if isinstance(node, list):
                for item in node:
                    collect(item)
                return
            if not isinstance(node, dict):
                return
            for key in ("datePublished", "dateCreated", "uploadDate", "publishedAt", "published_at"):
                value = node.get(key)
                if isinstance(value, str) and value.strip():
                    candidates.append(value.strip())
            for value in node.values():
                collect(value)

        for script in soup.select('script[type="application/ld+json"]'):
            raw_text = script.string or script.get_text(" ", strip=True)
            if not raw_text:
                continue
            try:
                payload = json.loads(raw_text)
            except json.JSONDecodeError:
                continue
            collect(payload)
        return candidates

    def _headline_adjacent_published_candidates(self, soup: BeautifulSoup) -> list[str]:
        headline = soup.find("h1")
        if headline is None or headline.parent is None:
            return []

        candidates: list[str] = []
        for element in headline.parent.find_all(["time", "div", "span", "p"], recursive=False):
            if element is headline or element.find("h1") is not None:
                continue
            datetime_value = element.get("datetime")
            if isinstance(datetime_value, str) and datetime_value.strip():
                candidates.append(datetime_value.strip())
            text = re.sub(r"\s+", " ", element.get_text(" ", strip=True))
            if 5 <= len(text) <= 40:
                candidates.append(text)
        return candidates

    def _parse_published_at(self, soup: BeautifulSoup) -> datetime | None:
        candidates: list[str] = []
        for selector in (
            ('meta[property="article:published_time"]', "content"),
            ('meta[property="og:article:published_time"]', "content"),
            ('meta[property="og:published_time"]', "content"),
            ('meta[name="citation_publication_date"]', "content"),
            ('meta[name="citation_online_date"]', "content"),
            ('meta[name="citation_date"]', "content"),
            ('meta[name="dc.date"]', "content"),
            ('meta[name="dc.date.issued"]', "content"),
            ('meta[name="publication_date"]', "content"),
            ('meta[name="publish-date"]', "content"),
            ('meta[name="publish_date"]', "content"),
            ('meta[name="pubdate"]', "content"),
            ('meta[name="datePublished"]', "content"),
            ('meta[property="datePublished"]', "content"),
            ('meta[itemprop="datePublished"]', "content"),
            ('meta[name="date"]', "content"),
            ("time[datetime]", "datetime"),
        ):
            element = soup.select_one(selector[0])
            value = element.get(selector[1], "").strip() if element else ""
            if value:
                candidates.append(value)

        candidates.extend(self._json_ld_published_candidates(soup))
        candidates.extend(self._headline_adjacent_published_candidates(soup))

        fallback = self._best_published_at_candidate(candidates)

        text = soup.get_text(" ", strip=True)
        arxiv_match = re.search(r"\[v\d+\]\s+\w{3},\s+(\d{1,2}\s+\w{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+UTC)", text)
        if arxiv_match:
            parsed = self._parse_datetime_candidate(arxiv_match.group(1))
            if parsed is not None:
                return parsed
        return fallback

    def _extract_from_html(
        self,
        html: str,
        *,
        url: str,
        fetched_url: str | None = None,
        mime_type: str | None = None,
    ) -> ExtractedContent:
        payload = str(html or "")
        base_url = str(fetched_url or url)
        soup = BeautifulSoup(payload, "html.parser")
        raw_title = (soup.title.get_text(strip=True) if soup.title else "") or url
        title = self.normalize_title(raw_title, url=url) or url
        published_at = self.extract_published_at_from_html(payload)

        extracted = trafilatura.extract(
            payload,
            include_links=True,
            include_comments=False,
            output_format="txt",
            favor_precision=True,
        )
        links = [
            urljoin(base_url, anchor["href"])
            for anchor in soup.find_all("a", href=True)
            if anchor["href"]
        ]

        cleaned = (extracted or "").strip()
        confidence = 0.85 if cleaned else 0.4
        if not cleaned:
            cleaned = soup.get_text("\n", strip=True)

        if self._looks_title_only(title=title, text=cleaned):
            fallback = self._extract_primary_content_text(soup, title=title)
            if fallback:
                cleaned = fallback
                confidence = max(confidence, 0.7)
            else:
                description = self._meta_description(soup)
                if description and not self._looks_title_only(title=title, text=description):
                    cleaned = description
                    confidence = max(confidence, 0.5)
        return ExtractedContent(
            title=title,
            cleaned_text=cleaned[:40000],
            outbound_links=list(dict.fromkeys(links))[:50],
            published_at=published_at,
            mime_type=mime_type or "text/html",
            extraction_confidence=confidence,
            raw_payload={"html": payload[:150000], "fetched_url": base_url},
        )

    def _extract_from_url_once(
        self,
        url: str,
        *,
        allow_insecure_tls: bool = False,
    ) -> ExtractedContent:
        fetch_kwargs: dict[str, object] = {"timeout": self.timeout_seconds}
        if allow_insecure_tls:
            fetch_kwargs["allow_insecure_tls"] = True
        response = fetch_safe_response(url, **fetch_kwargs)
        response.raise_for_status()
        return self._extract_from_html(
            response.text,
            url=url,
            fetched_url=str(response.url),
            mime_type=response.headers.get("content-type"),
        )

    def extract_from_url(self, url: str, *, allow_insecure_tls: bool = False) -> ExtractedContent:
        last_error: Exception | None = None
        for attempt_index in range(2):
            try:
                return self._extract_from_url_once(
                    url,
                    allow_insecure_tls=allow_insecure_tls or attempt_index > 0,
                )
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Extraction failed for {url}")

    def extract_from_html(self, html: str, *, url: str) -> ExtractedContent:
        return self._extract_from_html(html, url=url, fetched_url=url, mime_type="text/html")
