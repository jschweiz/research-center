from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

LEADING_PAPER_ID_RE = re.compile(r"^\s*(?:\[(?=[^\]\s]{1,48}\])[-A-Za-z0-9._:/]+\]\s*)+")
ARXIV_ID_RE = re.compile(r"^(?:arxiv:)?\d{4}\.\d{4,5}(?:v\d+)?$", re.IGNORECASE)
SIGNAL_TOKEN_RE = re.compile(r"[A-Za-z0-9]+(?:[-/][A-Za-z0-9]+)*")
SUMMARY_SENTENCE_BREAK_RE = re.compile(r"(?<=[.!?])\s+")
SUMMARY_URL_RE = re.compile(r"https?://\S+")
WHY_IT_MATTERS_BOILERPLATE_RE = re.compile(
    r"^\s*this surfaced because it intersects with .*?\b(?:and|but)\b\s*",
    re.IGNORECASE,
)
SIGNAL_STOPWORDS = {
    "a",
    "about",
    "an",
    "and",
    "are",
    "article",
    "articles",
    "as",
    "at",
    "be",
    "because",
    "been",
    "being",
    "but",
    "by",
    "current",
    "enough",
    "for",
    "from",
    "fresh",
    "hand-imported",
    "help",
    "helped",
    "helps",
    "highlight",
    "highlighted",
    "highlights",
    "improve",
    "improved",
    "improves",
    "in",
    "influence",
    "into",
    "intersects",
    "introduce",
    "introduced",
    "introduces",
    "is",
    "it",
    "item",
    "items",
    "its",
    "look",
    "looks",
    "manual",
    "material",
    "matters",
    "new",
    "next",
    "of",
    "on",
    "or",
    "paper",
    "papers",
    "present",
    "presented",
    "presents",
    "profile",
    "read",
    "report",
    "reported",
    "reports",
    "show",
    "showed",
    "shown",
    "shows",
    "signal",
    "signals",
    "source",
    "study",
    "studies",
    "summary",
    "surfaced",
    "that",
    "the",
    "their",
    "them",
    "there",
    "these",
    "this",
    "those",
    "to",
    "was",
    "were",
    "what",
    "why",
    "with",
}


def normalize_whitespace(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_item_title(title: str, *, content_type: Any = None) -> str:
    normalized = normalize_whitespace(title)
    if _content_type_name(content_type) != "paper":
        return normalized
    stripped = normalize_whitespace(LEADING_PAPER_ID_RE.sub("", normalized))
    return stripped or normalized


def compact_signal_note(
    note: str | None,
    *,
    title: str = "",
    summary: str = "",
    fallback_text: str = "",
    max_phrases: int = 3,
) -> str | None:
    phrases = extract_signal_phrases(
        [
            _strip_signal_boilerplate(note or ""),
            summary,
            title,
            fallback_text,
        ],
        max_phrases=max_phrases,
    )
    if phrases:
        return ", ".join(phrases)

    fallback = _strip_terminal_punctuation(normalize_whitespace(summary or title or note or fallback_text))
    return fallback[:120] or None


def fallback_short_summary(
    *,
    summary: str | None,
    text: str | None,
    title: str,
    max_chars: int = 240,
) -> str | None:
    normalized_summary = normalize_whitespace(summary)
    if normalized_summary:
        return _truncate_summary(normalized_summary, max_chars=max_chars)

    normalized_text = _summary_candidate_text(text)
    if normalized_text:
        sentence = SUMMARY_SENTENCE_BREAK_RE.split(normalized_text, maxsplit=1)[0]
        return _truncate_summary(sentence, max_chars=max_chars)

    normalized_title = normalize_whitespace(title)
    if normalized_title:
        return _truncate_summary(normalized_title, max_chars=max_chars)
    return None


def extract_signal_phrases(texts: Iterable[str], *, max_phrases: int = 3, max_words_per_phrase: int = 2) -> list[str]:
    phrases: list[str] = []
    seen: set[str] = set()

    def push(chunk: list[str]) -> None:
        if not chunk or len(phrases) >= max_phrases:
            return
        phrase = " ".join(chunk)
        normalized_phrase = phrase.lower()
        if normalized_phrase in seen:
            return
        seen.add(normalized_phrase)
        phrases.append(phrase)

    for text in texts:
        cleaned = normalize_whitespace(text)
        if not cleaned:
            continue

        for segment in re.split(r"[,:;.!?]\s*", cleaned):
            chunk: list[str] = []
            for token in SIGNAL_TOKEN_RE.findall(segment):
                if _skip_signal_token(token):
                    push(chunk)
                    chunk = []
                    continue
                chunk.append(token)
                if len(chunk) >= max_words_per_phrase:
                    push(chunk)
                    chunk = []

            push(chunk)
        if len(phrases) >= max_phrases:
            break

    return phrases[:max_phrases]


def _strip_signal_boilerplate(value: str) -> str:
    cleaned = normalize_whitespace(value)
    cleaned = WHY_IT_MATTERS_BOILERPLATE_RE.sub("", cleaned)
    cleaned = re.sub(r"^\s*why (?:it|this) matters\b[:\-]?\s*", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" .,:;-")


def _strip_terminal_punctuation(value: str) -> str:
    return value.rstrip(" .,:;!?")


def _summary_candidate_text(value: str | None) -> str:
    cleaned = SUMMARY_URL_RE.sub("", normalize_whitespace(value))
    cleaned = re.sub(r"\[[^\]]+\]\([^)]+\)", "", cleaned)
    return cleaned.strip(" -")


def _truncate_summary(value: str, *, max_chars: int) -> str:
    normalized = _strip_terminal_punctuation(normalize_whitespace(value))
    if len(normalized) <= max_chars:
        return normalized
    trimmed = normalized[: max_chars + 1].rsplit(" ", 1)[0]
    if not trimmed:
        trimmed = normalized[:max_chars]
    return trimmed.rstrip(" ,;:-")


def _skip_signal_token(token: str) -> bool:
    lowered = token.lower()
    if lowered in SIGNAL_STOPWORDS:
        return True
    if ARXIV_ID_RE.fullmatch(token):
        return True
    if not any(char.isalpha() for char in token):
        return True
    return bool(len(token) < 3 and not token.isupper())


def _content_type_name(content_type: Any) -> str | None:
    raw = getattr(content_type, "value", content_type)
    if isinstance(raw, str):
        return raw.lower()
    return None
