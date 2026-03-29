from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urljoin

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

    def _parse_published_at(self, soup: BeautifulSoup) -> datetime | None:
        candidates: list[str] = []
        for selector in (
            ('meta[property="article:published_time"]', "content"),
            ('meta[property="og:published_time"]', "content"),
            ('meta[name="citation_publication_date"]', "content"),
            ('meta[name="citation_online_date"]', "content"),
            ('meta[name="citation_date"]', "content"),
            ('meta[name="dc.date"]', "content"),
            ('meta[name="dc.date.issued"]', "content"),
            ('meta[name="date"]', "content"),
            ("time[datetime]", "datetime"),
        ):
            element = soup.select_one(selector[0])
            value = element.get(selector[1], "").strip() if element else ""
            if value:
                candidates.append(value)

        fallback: datetime | None = None
        for candidate in candidates:
            try:
                parsed = date_parser.parse(candidate)
            except (TypeError, ValueError, OverflowError):
                continue
            normalized = parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
            if normalized.time() != datetime.min.time():
                return normalized
            fallback = fallback or normalized

        text = soup.get_text(" ", strip=True)
        arxiv_match = re.search(r"\[v\d+\]\s+\w{3},\s+(\d{1,2}\s+\w{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+UTC)", text)
        if arxiv_match:
            try:
                parsed = date_parser.parse(arxiv_match.group(1))
            except (TypeError, ValueError, OverflowError):
                return fallback
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        return fallback

    def extract_from_url(self, url: str) -> ExtractedContent:
        response = fetch_safe_response(url, timeout=self.timeout_seconds)
        response.raise_for_status()

        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        title = (soup.title.get_text(strip=True) if soup.title else "") or url
        published_at = self._parse_published_at(soup)

        extracted = trafilatura.extract(
            html,
            include_links=True,
            include_comments=False,
            output_format="txt",
            favor_precision=True,
        )
        links = [
            urljoin(str(response.url), anchor["href"])
            for anchor in soup.find_all("a", href=True)
            if anchor["href"]
        ]

        cleaned = extracted or soup.get_text("\n", strip=True)
        confidence = 0.85 if extracted else 0.4
        return ExtractedContent(
            title=title,
            cleaned_text=cleaned[:40000],
            outbound_links=list(dict.fromkeys(links))[:50],
            published_at=published_at,
            mime_type=response.headers.get("content-type"),
            extraction_confidence=confidence,
            raw_payload={"html": html[:150000], "fetched_url": str(response.url)},
        )
