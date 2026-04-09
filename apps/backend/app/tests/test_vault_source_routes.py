from __future__ import annotations

from datetime import UTC, datetime

import httpx
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.integrations.extractors import ExtractedContent


def _install_summary_stub(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.ollama_status",
        lambda self: {
            "available": True,
            "model": "test-ollama",
            "detail": None,
        },
    )
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.lightweight_enrich_raw_document",
        lambda self, item, text: {
            "authors": item.get("authors") or [],
            "tags": item.get("tags") or [],
            "short_summary": f"Summary for {item['title']}",
            "model": "test-ollama",
        },
    )
    monkeypatch.setattr(
        "app.integrations.llm.LLMClient.judge_lightweight_document",
        lambda self, item, text, *, profile, source_context=None: {
            "relevance_score": 0.78,
            "source_fit_score": 0.72,
            "topic_fit_score": 0.81,
            "author_fit_score": 0.5,
            "evidence_fit_score": 0.76,
            "confidence_score": 0.74,
            "bucket_hint": "must_read",
            "reason": f"Strong fit for {item['title']}.",
            "evidence_quotes": [f"Summary for {item['title']}"],
            "model": "test-ollama",
        },
    )


def test_list_sources_returns_vault_native_source_metadata(
    authenticated_client: TestClient,
) -> None:
    response = authenticated_client.get("/api/sources")

    assert response.status_code == 200
    payload = response.json()
    by_id = {entry["id"]: entry for entry in payload}

    assert set(by_id) >= {
        "openai-website",
        "anthropic-research",
        "mistral-research",
        "tldr-email",
        "medium-email",
    }
    assert by_id["openai-website"]["type"] == "website"
    assert by_id["openai-website"]["raw_kind"] == "blog-post"
    assert by_id["openai-website"]["classification_mode"] == "fixed"
    assert by_id["openai-website"]["decomposition_mode"] == "none"
    assert by_id["openai-website"]["has_custom_pipeline"] is True
    assert by_id["openai-website"]["custom_pipeline_id"] == "openai-website"
    assert by_id["openai-website"]["max_items"] == 20
    assert by_id["mistral-research"]["type"] == "website"
    assert by_id["mistral-research"]["url"] == "https://mistral.ai/news?category=research"
    assert by_id["tldr-email"]["type"] == "gmail_newsletter"
    assert by_id["tldr-email"]["raw_kind"] == "newsletter"
    assert by_id["tldr-email"]["classification_mode"] == "written_content_auto"
    assert by_id["tldr-email"]["decomposition_mode"] == "newsletter_entries"
    assert "tldrnewsletter.com" in (by_id["tldr-email"]["query"] or "")


def test_inject_source_and_latest_log_use_vault_source_pipeline(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    settings = get_settings()
    settings.vault_source_pipelines_enabled = True

    feed = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>OpenAI News</title>
    <item>
      <title>Test Launch</title>
      <description><![CDATA[<p>A compact summary from the RSS feed.</p>]]></description>
      <link>https://openai.com/index/test-launch</link>
      <pubDate>Tue, 07 Apr 2026 12:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

    def fake_fetch(
        url: str, *, timeout: float, headers=None, max_redirects: int = 5
    ) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        assert url == "https://openai.com/news/rss.xml"
        return httpx.Response(
            200,
            request=httpx.Request("GET", url),
            content=feed.encode("utf-8"),
            headers={"content-type": "application/rss+xml"},
        )

    def fake_extract(self, url: str) -> ExtractedContent:
        assert url == "https://openai.com/index/test-launch"
        return ExtractedContent(
            title="OpenAI Test Launch",
            cleaned_text="OpenAI shipped a new research release.",
            outbound_links=["https://example.com/context"],
            published_at=datetime(2026, 4, 7, 12, 0, tzinfo=UTC),
            mime_type="text/html",
            extraction_confidence=0.98,
            raw_payload={"html": "<html><body>OpenAI launch</body></html>"},
        )

    monkeypatch.setattr("app.services.vault_sources.fetch_safe_response", fake_fetch)
    monkeypatch.setattr(
        "app.integrations.extractors.ContentExtractor.extract_from_url",
        fake_extract,
    )
    _install_summary_stub(monkeypatch)

    response = authenticated_client.post("/api/sources/openai-website/inject")

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_name"] == "source_inject"
    assert payload["operation_run_id"]

    latest_log = authenticated_client.get("/api/sources/openai-website/latest-log")
    assert latest_log.status_code == 200
    run_payload = latest_log.json()["run"]
    assert run_payload["id"] == payload["operation_run_id"]
    assert run_payload["operation_kind"] == "raw_fetch"
    assert run_payload["status"] == "succeeded"
    assert any(
        info["label"] == "Source ID" and info["value"] == "openai-website"
        for info in run_payload["basic_info"]
    )
    assert any("Starting raw fetch" in log["message"] for log in run_payload["logs"])
    assert any(
        log["message"] == 'OpenAI Website: created blog-post "OpenAI Test Launch" (2026-04-07).'
        for log in run_payload["logs"]
    )
    assert any(step["step_kind"] == "raw_fetch" for step in run_payload["steps"])

    listing = authenticated_client.get("/api/sources")
    assert listing.status_code == 200
    source = next(entry for entry in listing.json() if entry["id"] == "openai-website")
    assert source["last_synced_at"] is not None
    assert source["latest_extraction_run"]["id"] == payload["operation_run_id"]
    assert source["latest_extraction_run"]["status"] == "succeeded"
    assert source["latest_extraction_run"]["emitted_kinds"] == ["blog-post"]

    items = authenticated_client.get("/api/items", params={"source_id": "openai-website"})
    assert items.status_code == 200
    item_payload = items.json()
    assert len(item_payload) == 1
    assert item_payload[0]["title"] == "OpenAI Test Launch"
    assert item_payload[0]["source_name"] == "OpenAI Website"


def test_inject_source_accepts_per_run_max_items_override_for_website_index_sources(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    settings = get_settings()
    settings.vault_source_pipelines_enabled = True

    index_html = "<html><body>{links}</body></html>".format(
        links="".join(
            f'<a href="/research/post-{index:02d}">Anthropic Post {index:02d}</a>'
            for index in range(1, 31)
        )
    )

    def fake_fetch(
        url: str, *, timeout: float, headers=None, max_redirects: int = 5
    ) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        assert url == "https://www.anthropic.com/research"
        return httpx.Response(
            200,
            request=httpx.Request("GET", url),
            content=index_html.encode("utf-8"),
            headers={"content-type": "text/html"},
        )

    def fake_extract(self, url: str) -> ExtractedContent:
        slug = url.rstrip("/").split("/")[-1]
        title = slug.replace("-", " ").title()
        return ExtractedContent(
            title=title,
            cleaned_text=f"{title} body.",
            outbound_links=[],
            published_at=datetime(2026, 4, 7, 12, 0, tzinfo=UTC),
            mime_type="text/html",
            extraction_confidence=0.98,
            raw_payload={"html": f"<html><body>{title}</body></html>"},
        )

    monkeypatch.setattr("app.services.vault_sources.fetch_safe_response", fake_fetch)
    monkeypatch.setattr(
        "app.integrations.extractors.ContentExtractor.extract_from_url",
        fake_extract,
    )
    _install_summary_stub(monkeypatch)

    response = authenticated_client.post(
        "/api/sources/anthropic-research/inject",
        json={"max_items": 25},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["detail"] == (
        "Source fetch, lightweight enrichment, and index rebuild completed for "
        "Anthropic Research with a cap of 25 documents."
    )
    assert payload["operation_run_id"]

    latest_log = authenticated_client.get("/api/sources/anthropic-research/latest-log")
    assert latest_log.status_code == 200
    run_payload = latest_log.json()["run"]
    assert run_payload["id"] == payload["operation_run_id"]
    assert any(
        info["label"] == "Max items" and info["value"] == "25" for info in run_payload["basic_info"]
    )
    assert any(
        info["label"] == "Configured max items" and info["value"] == "20"
        for info in run_payload["basic_info"]
    )

    items = authenticated_client.get("/api/items", params={"source_id": "anthropic-research"})
    assert items.status_code == 200
    item_payload = items.json()
    assert len(item_payload) == 25
    assert {entry["title"] for entry in item_payload} >= {"Post 01", "Post 25"}


def test_latest_source_log_returns_404_when_no_extraction_exists(
    authenticated_client: TestClient,
) -> None:
    response = authenticated_client.get("/api/sources/openai-website/latest-log")

    assert response.status_code == 404
    assert response.json()["detail"] == "No extraction log exists for this source yet."
