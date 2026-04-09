from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import yaml

FRONTMATTER_DELIMITER = "---"


def parse_frontmatter_document(text: str) -> tuple[dict[str, Any], str]:
    normalized = text.replace("\r\n", "\n")
    if not normalized.startswith(f"{FRONTMATTER_DELIMITER}\n"):
        return {}, normalized

    lines = normalized.split("\n")
    for index in range(1, len(lines)):
        if lines[index].strip() == FRONTMATTER_DELIMITER:
            header_lines = lines[1:index]
            body = "\n".join(lines[index + 1 :]).lstrip("\n")
            return _parse_frontmatter_lines(header_lines), body
    return {}, normalized


def render_frontmatter_document(frontmatter: Mapping[str, Any], body: str) -> str:
    header = _render_frontmatter_lines(frontmatter)
    normalized_body = body.replace("\r\n", "\n").rstrip()
    if normalized_body:
        return (
            f"{FRONTMATTER_DELIMITER}\n{header}{FRONTMATTER_DELIMITER}\n"
            f"{normalized_body}\n"
        )
    return f"{FRONTMATTER_DELIMITER}\n{header}{FRONTMATTER_DELIMITER}\n"


def _parse_frontmatter_lines(lines: list[str]) -> dict[str, Any]:
    header_text = "\n".join(lines).strip()
    if not header_text:
        return {}
    try:
        loaded = yaml.safe_load(header_text)
    except yaml.YAMLError:
        return _parse_frontmatter_lines_legacy(lines)
    if loaded is None:
        return {}
    if not isinstance(loaded, Mapping):
        return _parse_frontmatter_lines_legacy(lines)
    return dict(loaded)


def _parse_frontmatter_lines_legacy(lines: list[str]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line.strip():
            index += 1
            continue
        key, separator, remainder = line.partition(":")
        if not separator:
            index += 1
            continue
        normalized_key = key.strip()
        normalized_value = remainder.lstrip()
        if normalized_value:
            payload[normalized_key] = _parse_scalar(normalized_value)
            index += 1
            continue

        items: list[Any] = []
        index += 1
        while index < len(lines):
            candidate = lines[index]
            stripped = candidate.lstrip()
            if not stripped.startswith("- "):
                break
            items.append(_parse_scalar(stripped[2:]))
            index += 1
        payload[normalized_key] = items
    return payload


def _render_frontmatter_lines(frontmatter: Mapping[str, Any]) -> str:
    if not frontmatter:
        return ""
    rendered = yaml.safe_dump(
        dict(frontmatter),
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
        width=10_000,
    )
    return rendered if rendered.endswith("\n") else f"{rendered}\n"


def _parse_scalar(value: str) -> Any:
    candidate = value.strip()
    lowered = candidate.lower()
    if lowered in {"null", "none"}:
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if candidate.startswith('"') and candidate.endswith('"') and len(candidate) >= 2:
        return candidate[1:-1]
    if candidate.startswith("'") and candidate.endswith("'") and len(candidate) >= 2:
        return candidate[1:-1]
    try:
        if "." in candidate:
            return float(candidate)
        return int(candidate)
    except ValueError:
        return candidate

