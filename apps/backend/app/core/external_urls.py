from __future__ import annotations

from urllib.parse import urlparse

FREEDIUM_MIRROR_PREFIX = "https://freedium-mirror.cfd/"


def resolve_external_url(url: str | None) -> str | None:
    if url is None:
        return None

    trimmed = url.strip()
    if not trimmed or trimmed.startswith(FREEDIUM_MIRROR_PREFIX):
        return trimmed

    parsed = urlparse(trimmed)
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"}:
        return trimmed

    if hostname == "medium.com" or hostname.endswith(".medium.com"):
        return f"{FREEDIUM_MIRROR_PREFIX}{trimmed}"

    return trimmed
