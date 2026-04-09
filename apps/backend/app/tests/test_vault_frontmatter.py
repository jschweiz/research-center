from __future__ import annotations

from app.vault.frontmatter import parse_frontmatter_document, render_frontmatter_document


def test_parse_frontmatter_document_supports_legacy_unquoted_colon_values() -> None:
    payload, body = parse_frontmatter_document(
        "---\n"
        "title: Vibe physics: The AI grad student\n"
        "tags:\n"
        "  - verifier routing\n"
        "  - topic: agents\n"
        "---\n"
        "Body.\n"
    )

    assert payload["title"] == "Vibe physics: The AI grad student"
    assert payload["tags"] == ["verifier routing", "topic: agents"]
    assert body == "Body.\n"


def test_render_frontmatter_document_round_trips_nested_mappings() -> None:
    rendered = render_frontmatter_document(
        {
            "title": "Verifier routing: staff notes",
            "lightweight_score": {
                "relevance_score": 0.91,
                "topic_fit_score": 0.88,
                "bucket_hint": "must_read",
                "reason": "Strong fit for the current research profile.",
                "evidence_quotes": [
                    "verifier routing",
                    "research triage",
                ],
            },
        },
        "Body.\n",
    )

    payload, body = parse_frontmatter_document(rendered)

    assert payload["title"] == "Verifier routing: staff notes"
    assert payload["lightweight_score"]["relevance_score"] == 0.91
    assert payload["lightweight_score"]["topic_fit_score"] == 0.88
    assert payload["lightweight_score"]["bucket_hint"] == "must_read"
    assert payload["lightweight_score"]["evidence_quotes"] == [
        "verifier routing",
        "research triage",
    ]
    assert body == "Body.\n"
