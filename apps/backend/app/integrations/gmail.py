from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from bs4 import BeautifulSoup

TEXT_LINK_RE = re.compile(r"https?://\S+")


@dataclass
class NewsletterMessage:
    message_id: str
    thread_id: str
    subject: str
    sender: str
    published_at: datetime
    text_body: str
    html_body: str
    outbound_links: list[str]
    permalink: str


class GmailConnector:
    api_base = "https://gmail.googleapis.com/gmail/v1/users/me"

    def __init__(self, access_token: str) -> None:
        self.access_token = access_token

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.access_token}"}

    def list_newsletters(
        self,
        senders: list[str] | None = None,
        labels: list[str] | None = None,
        raw_query: str | None = None,
        max_results: int = 20,
        newer_than_days: int | None = 7,
    ) -> list[NewsletterMessage]:
        query_parts: list[str] = []
        if newer_than_days is not None and newer_than_days > 0:
            query_parts.append(f"newer_than:{newer_than_days}d")
        if raw_query:
            query_parts.append(f"({raw_query})")
        if senders:
            query_parts.append("(" + " OR ".join(f"from:{sender}" for sender in senders) + ")")
        if labels:
            query_parts.extend(f"label:{label}" for label in labels)
        query = " ".join(query_parts)

        with httpx.Client(timeout=20, follow_redirects=True) as client:
            listing = client.get(
                f"{self.api_base}/messages",
                params={"maxResults": max_results, "q": query},
                headers=self._headers(),
            )
            listing.raise_for_status()
            payload = listing.json()
            messages = payload.get("messages", [])

            results: list[NewsletterMessage] = []
            for message in messages:
                detail = client.get(
                    f"{self.api_base}/messages/{message['id']}",
                    params={"format": "full"},
                    headers=self._headers(),
                )
                detail.raise_for_status()
                parsed = self._parse_message(detail.json())
                if parsed:
                    results.append(parsed)
            return results

    def _parse_message(self, message: dict[str, Any]) -> NewsletterMessage | None:
        payload = message.get("payload", {})
        headers = {entry["name"].lower(): entry["value"] for entry in payload.get("headers", [])}
        subject = headers.get("subject", "Untitled newsletter")
        sender = headers.get("from", "Unknown sender")
        date_header = headers.get("date")
        published_at = (
            parsedate_to_datetime(date_header).astimezone(UTC)
            if date_header
            else datetime.now(UTC)
        )

        html_body = self._extract_body(payload, mime_type="text/html")
        text_body = self._extract_body(payload, mime_type="text/plain")
        if not text_body and html_body:
            soup = BeautifulSoup(html_body, "html.parser")
            text_body = soup.get_text("\n", strip=True)
        outbound_links = extract_email_links(html_body=html_body, text_body=text_body)

        return NewsletterMessage(
            message_id=message["id"],
            thread_id=message.get("threadId", message["id"]),
            subject=subject,
            sender=sender,
            published_at=published_at,
            text_body=text_body[:40000],
            html_body=html_body[:150000],
            outbound_links=list(dict.fromkeys(outbound_links))[:50],
            permalink=f"https://mail.google.com/mail/u/0/#inbox/{message['id']}",
        )

    def _extract_body(self, payload: dict[str, Any], mime_type: str) -> str:
        if payload.get("mimeType") == mime_type and payload.get("body", {}).get("data"):
            return self._decode(payload["body"]["data"])
        for part in payload.get("parts", []) or []:
            if part.get("mimeType") == mime_type and part.get("body", {}).get("data"):
                return self._decode(part["body"]["data"])
            nested = self._extract_body(part, mime_type)
            if nested:
                return nested
        return ""

    def _decode(self, body: str) -> str:
        return base64.urlsafe_b64decode(body.encode("utf-8")).decode("utf-8", errors="ignore")


def extract_email_links(*, html_body: str, text_body: str) -> list[str]:
    links: list[str] = []
    if html_body:
        soup = BeautifulSoup(html_body, "html.parser")
        links.extend(anchor["href"] for anchor in soup.find_all("a", href=True))
    if text_body:
        links.extend(TEXT_LINK_RE.findall(text_body))
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_link in links:
        link = str(raw_link or "").strip().rstrip(').,;:!?"\'')
        if not link or not link.startswith(("http://", "https://")) or link in seen:
            continue
        seen.add(link)
        normalized.append(link)
    return normalized[:50]
