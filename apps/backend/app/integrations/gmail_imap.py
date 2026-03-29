from __future__ import annotations

import imaplib
from contextlib import suppress
from datetime import UTC, datetime
from email import message_from_bytes, policy
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime
from urllib.parse import quote

from bs4 import BeautifulSoup

from app.integrations.gmail import NewsletterMessage, extract_email_links


class GmailImapConnector:
    host = "imap.gmail.com"
    port = 993

    def __init__(self, *, email_address: str, app_password: str) -> None:
        self.email_address = email_address
        self.app_password = app_password

    def test_connection(self) -> None:
        client = self._connect()
        try:
            self._select_mailbox(client)
        finally:
            self._logout(client)

    def list_newsletters(
        self,
        senders: list[str] | None = None,
        labels: list[str] | None = None,
        raw_query: str | None = None,
        max_results: int = 20,
        newer_than_days: int | None = 7,
    ) -> list[NewsletterMessage]:
        client = self._connect()
        try:
            self._select_mailbox(client)
            query = self._build_query(
                senders=senders,
                labels=labels,
                raw_query=raw_query,
                newer_than_days=newer_than_days,
            )
            status, data = client.uid("SEARCH", None, "X-GM-RAW", query)
            if status != "OK":
                return []

            message_uids = [uid for uid in (data[0] or b"").split() if uid][-max_results:]
            results: list[NewsletterMessage] = []
            for uid in reversed(message_uids):
                status, fetched = client.uid("FETCH", uid, "(RFC822)")
                if status != "OK":
                    continue
                parsed = self._parse_fetch(uid.decode("utf-8", errors="ignore"), fetched or [])
                if parsed:
                    results.append(parsed)
            return results
        finally:
            self._logout(client)

    def _connect(self) -> imaplib.IMAP4_SSL:
        client = imaplib.IMAP4_SSL(self.host, self.port)
        client.login(self.email_address, self.app_password)
        return client

    def _select_mailbox(self, client: imaplib.IMAP4_SSL) -> None:
        status, mailboxes = client.list()
        if status == "OK":
            for mailbox in mailboxes or []:
                parsed = self._parse_list_mailbox(mailbox)
                if not parsed:
                    continue
                flags, name = parsed
                if "\\All" not in flags:
                    continue
                status, _ = client.select(self._quote_mailbox(name), readonly=True)
                if status == "OK":
                    return
        status, _ = client.select("INBOX", readonly=True)
        if status != "OK":
            raise RuntimeError("Could not select a mailbox for Gmail IMAP access.")

    def _build_query(
        self,
        *,
        senders: list[str] | None,
        labels: list[str] | None,
        raw_query: str | None,
        newer_than_days: int | None,
    ) -> str:
        query_parts: list[str] = []
        if newer_than_days is not None and newer_than_days > 0:
            query_parts.append(f"newer_than:{newer_than_days}d")
        if raw_query:
            query_parts.append(f"({raw_query})")
        if senders:
            query_parts.append("(" + " OR ".join(f"from:{sender}" for sender in senders) + ")")
        if labels:
            query_parts.extend(f"label:{label}" for label in labels)
        return " ".join(query_parts)

    def _parse_fetch(self, uid: str, fetched: list[tuple[bytes, bytes] | bytes]) -> NewsletterMessage | None:
        raw_email: bytes | None = None
        for part in fetched:
            if isinstance(part, tuple) and len(part) >= 2:
                raw_email = part[1]
                break
        if not raw_email:
            return None

        message = message_from_bytes(raw_email, policy=policy.default)
        subject = self._decode_header_value(message.get("Subject")) or "Untitled newsletter"
        sender = self._decode_header_value(message.get("From")) or "Unknown sender"
        date_header = message.get("Date")
        published_at = (
            parsedate_to_datetime(date_header).astimezone(UTC)
            if date_header
            else datetime.now(UTC)
        )
        text_body, html_body = self._extract_bodies(message)
        outbound_links = extract_email_links(html_body=html_body, text_body=text_body)
        if not text_body and html_body:
            text_body = BeautifulSoup(html_body, "html.parser").get_text("\n", strip=True)

        message_id = (self._decode_header_value(message.get("Message-ID")) or uid).strip()
        return NewsletterMessage(
            message_id=message_id,
            thread_id=message_id,
            subject=subject,
            sender=sender,
            published_at=published_at,
            text_body=text_body[:40000],
            html_body=html_body[:150000],
            outbound_links=outbound_links[:50],
            permalink=self._build_permalink(message_id=message_id, sender=sender, subject=subject),
        )

    def _extract_bodies(self, message: Message) -> tuple[str, str]:
        text_chunks: list[str] = []
        html_chunks: list[str] = []
        for part in message.walk():
            if part.is_multipart():
                continue
            disposition = str(part.get("Content-Disposition") or "").lower()
            if "attachment" in disposition:
                continue
            content_type = part.get_content_type()
            try:
                content = part.get_content()
            except Exception:
                payload = part.get_payload(decode=True) or b""
                charset = part.get_content_charset() or "utf-8"
                content = payload.decode(charset, errors="ignore")
            if not isinstance(content, str):
                continue
            if content_type == "text/plain":
                text_chunks.append(content)
            elif content_type == "text/html":
                html_chunks.append(content)
        return ("\n".join(text_chunks).strip(), "\n".join(html_chunks).strip())

    def _build_permalink(self, *, message_id: str, sender: str, subject: str) -> str:
        normalized_message_id = message_id.strip()
        if normalized_message_id:
            return f"https://mail.google.com/mail/u/0/#search/{quote(f'rfc822msgid:{normalized_message_id}')}"
        search_terms = " ".join(part for part in [f"from:{sender}" if sender else "", subject] if part).strip()
        return f"https://mail.google.com/mail/u/0/#search/{quote(search_terms or self.email_address)}"

    def _parse_list_mailbox(self, mailbox: bytes) -> tuple[str, str] | None:
        decoded = mailbox.decode("utf-8", errors="ignore")
        marker = ' "/" '
        delimiter_index = decoded.find(marker)
        if delimiter_index == -1:
            marker = ' "." '
            delimiter_index = decoded.find(marker)
        if delimiter_index == -1:
            return None
        flags = decoded.split(")", 1)[0].lstrip("(")
        name = decoded[delimiter_index + len(marker):].strip()
        if name.startswith('"') and name.endswith('"'):
            name = name[1:-1]
        return (flags, name)

    def _quote_mailbox(self, mailbox: str) -> str:
        escaped = mailbox.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'

    def _decode_header_value(self, value: str | None) -> str:
        if not value:
            return ""
        decoded_chunks: list[str] = []
        for chunk, encoding in decode_header(value):
            if isinstance(chunk, bytes):
                decoded_chunks.append(chunk.decode(encoding or "utf-8", errors="ignore"))
            else:
                decoded_chunks.append(chunk)
        return "".join(decoded_chunks).strip()

    def _logout(self, client: imaplib.IMAP4_SSL) -> None:
        with suppress(Exception):
            client.logout()
