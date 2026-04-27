from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from app.db.base import Base
from app.db.models import (
    ConnectionProvider,
    ConnectionStatus,
    ContentType,
    IngestionRunType,
    RunStatus,
)
from app.db.session import get_engine, get_session_factory
from app.integrations.extractors import ExtractedContent
from app.integrations.gmail import NewsletterMessage
from app.schemas.ops import IngestionRunHistoryRead
from app.schemas.profile import AlphaXivSearchSettings
from app.services.connections import ConnectionService
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_runtime import RunRecorder
from app.services.vault_sources import (
    DEFAULT_SOURCES,
    SourceFetchCancelledError,
    VaultSourceIngestionService,
    WebsiteEntry,
)
from app.vault.models import (
    DefaultSourcesState,
    RawDocumentFrontmatter,
    VaultSourceDefinition,
    VaultSourcesConfig,
)
from app.vault.store import VaultStore


def _response(url: str, body: str, *, content_type: str) -> httpx.Response:
    return httpx.Response(
        200,
        request=httpx.Request("GET", url),
        content=body.encode("utf-8"),
        headers={"content-type": content_type},
    )


def _json_response(url: str, payload) -> httpx.Response:
    return httpx.Response(
        200,
        request=httpx.Request("GET", url),
        json=payload,
        headers={"content-type": "application/json"},
    )


def _bytes_response(url: str, payload: bytes, *, content_type: str) -> httpx.Response:
    return httpx.Response(
        200,
        request=httpx.Request("GET", url),
        content=payload,
        headers={"content-type": content_type},
    )


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


def test_vault_source_service_bootstraps_default_sources_config(client) -> None:
    service = VaultSourceIngestionService()
    config = service.store.load_sources_config()

    assert not service.store.sources_config_path.exists()
    assert sorted(source.id for source in config.sources) == sorted(
        [
            "openai-website",
            "anthropic-research",
            "mistral-research",
            "the-batch-research",
            "jack-clark-import-ai",
            "tldr-email",
            "medium-email",
            "alphasignal-email",
            "alphaxiv-paper",
        ]
    )

    by_id = {source.id: source for source in config.sources}
    assert by_id["openai-website"].type == "website"
    assert by_id["openai-website"].raw_kind == "blog-post"
    assert by_id["openai-website"].classification_mode == "fixed"
    assert by_id["openai-website"].decomposition_mode == "none"
    assert by_id["openai-website"].config_json["discovery_mode"] == "rss_feed"
    assert by_id["anthropic-research"].type == "website"
    assert by_id["anthropic-research"].raw_kind == "blog-post"
    assert by_id["anthropic-research"].config_json["discovery_mode"] == "website_index"
    assert by_id["mistral-research"].type == "website"
    assert by_id["mistral-research"].raw_kind == "blog-post"
    assert by_id["mistral-research"].config_json["discovery_mode"] == "website_index"
    assert by_id["mistral-research"].config_json["script_entry_parser"] == "mistral_news_posts"
    assert by_id["mistral-research"].config_json["website_url"] == (
        "https://mistral.ai/news?category=research"
    )
    assert by_id["the-batch-research"].type == "website"
    assert by_id["the-batch-research"].raw_kind == "blog-post"
    assert by_id["the-batch-research"].config_json["discovery_mode"] == "website_index"
    assert by_id["the-batch-research"].config_json["website_url"] == (
        "https://www.deeplearning.ai/the-batch/tag/research/"
    )
    assert by_id["jack-clark-import-ai"].type == "website"
    assert by_id["jack-clark-import-ai"].raw_kind == "newsletter"
    assert by_id["jack-clark-import-ai"].classification_mode == "written_content_auto"
    assert by_id["jack-clark-import-ai"].decomposition_mode == "newsletter_entries"
    assert by_id["jack-clark-import-ai"].url == "https://jack-clark.net/feed/"
    assert by_id["jack-clark-import-ai"].config_json["newsletter_parser"] == (
        "jack_clark_import_ai"
    )
    assert by_id["tldr-email"].type == "gmail_newsletter"
    assert by_id["tldr-email"].raw_kind == "newsletter"
    assert by_id["tldr-email"].classification_mode == "written_content_auto"
    assert by_id["tldr-email"].decomposition_mode == "newsletter_entries"
    assert by_id["tldr-email"].config_json["senders"] == [
        "dan@tldrnewsletter.com",
        "hi@tldrnewsletter.com",
        "newsletter@tldrnewsletter.com",
    ]
    assert by_id["medium-email"].type == "gmail_newsletter"
    assert by_id["medium-email"].raw_kind == "newsletter"
    assert by_id["medium-email"].config_json["senders"] == ["noreply@medium.com"]
    assert by_id["alphasignal-email"].type == "gmail_newsletter"
    assert by_id["alphasignal-email"].raw_kind == "newsletter"
    assert by_id["alphasignal-email"].classification_mode == "written_content_auto"
    assert by_id["alphasignal-email"].decomposition_mode == "newsletter_entries"
    assert by_id["alphasignal-email"].config_json["senders"] == ["news@alphasignal.ai"]
    assert by_id["alphaxiv-paper"].type == "website"
    assert by_id["alphaxiv-paper"].raw_kind == "paper"
    assert by_id["alphaxiv-paper"].enabled is True
    assert all(source.created_at is not None for source in config.sources)
    assert all(source.updated_at is not None for source in config.sources)


def test_vault_source_service_backfills_new_catalog_sources_without_overwriting_existing_ones(
    client,
) -> None:
    store = VaultStore()
    store.ensure_layout()
    customized_default = VaultSourceDefinition(
        id="openai-website",
        type="website",
        name="OpenAI Website",
        enabled=True,
        raw_kind="blog-post",
        custom_pipeline_id="openai-website",
        classification_mode="fixed",
        decomposition_mode="none",
        description="Customized openai source.",
        tags=["openai", "custom"],
        url="https://openai.com/news/rss.xml",
        max_items=42,
        created_at=datetime(2026, 4, 9, 8, 0, tzinfo=UTC),
        updated_at=datetime(2026, 4, 9, 8, 0, tzinfo=UTC),
        config_json={"discovery_mode": "rss_feed"},
    )
    custom_source = VaultSourceDefinition(
        id="custom-site",
        type="website",
        name="Custom Site",
        enabled=True,
        raw_kind="article",
        custom_pipeline_id=None,
        classification_mode="fixed",
        decomposition_mode="none",
        description="User-managed custom source.",
        tags=["custom"],
        url="https://example.com/feed.xml",
        max_items=9,
        created_at=datetime(2026, 4, 9, 8, 5, tzinfo=UTC),
        updated_at=datetime(2026, 4, 9, 8, 5, tzinfo=UTC),
        config_json={"discovery_mode": "rss_feed"},
    )
    store.save_sources_config(
        VaultSourcesConfig(sources=[customized_default, custom_source])
    )

    service = VaultSourceIngestionService()
    config = service.store.load_sources_config()
    by_id = {source.id: source for source in config.sources}

    assert by_id["openai-website"].max_items == 42
    assert by_id["openai-website"].tags == ["openai", "custom"]
    assert by_id["custom-site"].name == "Custom Site"
    assert set(by_id) == {
        "openai-website",
        "custom-site",
        "the-batch-research",
        "jack-clark-import-ai",
        "alphasignal-email",
    }
    assert service.store.load_default_sources_state().catalog_version == 4


def test_vault_source_service_repairs_version_two_catalog_state_for_missing_new_defaults(
    client,
) -> None:
    store = VaultStore()
    store.ensure_layout()
    customized_default = VaultSourceDefinition(
        id="openai-website",
        type="website",
        name="OpenAI Website",
        enabled=True,
        raw_kind="blog-post",
        custom_pipeline_id="openai-website",
        classification_mode="fixed",
        decomposition_mode="none",
        description="Customized openai source.",
        tags=["openai", "custom"],
        url="https://openai.com/news/rss.xml",
        max_items=42,
        created_at=datetime(2026, 4, 9, 8, 0, tzinfo=UTC),
        updated_at=datetime(2026, 4, 9, 8, 0, tzinfo=UTC),
        config_json={"discovery_mode": "rss_feed"},
    )
    custom_source = VaultSourceDefinition(
        id="custom-site",
        type="website",
        name="Custom Site",
        enabled=True,
        raw_kind="article",
        custom_pipeline_id=None,
        classification_mode="fixed",
        decomposition_mode="none",
        description="User-managed custom source.",
        tags=["custom"],
        url="https://example.com/feed.xml",
        max_items=9,
        created_at=datetime(2026, 4, 9, 8, 5, tzinfo=UTC),
        updated_at=datetime(2026, 4, 9, 8, 5, tzinfo=UTC),
        config_json={"discovery_mode": "rss_feed"},
    )
    store.save_sources_config(
        VaultSourcesConfig(sources=[customized_default, custom_source])
    )
    store.save_default_sources_state(DefaultSourcesState(catalog_version=2))

    service = VaultSourceIngestionService()
    config = service.store.load_sources_config()
    by_id = {source.id: source for source in config.sources}

    assert set(by_id) == {
        "openai-website",
        "custom-site",
        "the-batch-research",
        "alphasignal-email",
    }
    assert service.store.load_default_sources_state().catalog_version == 4


def test_openai_source_sync_writes_blog_post_raw_documents(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "openai-website")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    article_url = "https://openai.com/index/test-launch?utm_source=rss"
    feed = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>OpenAI News</title>
    <item>
      <title>Test Launch</title>
      <link>{article_url}</link>
      <pubDate>Tue, 07 Apr 2026 12:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

    def fake_fetch(url: str, *, timeout: float, headers=None, max_redirects: int = 5) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        assert url == "https://openai.com/news/rss.xml"
        return _response(url, feed, content_type="application/rss+xml")

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

    result = service.sync_enabled_sources(trigger="test_sync")
    documents = service.store.list_raw_documents()

    assert result.source_count == 1
    assert result.synced_document_count == 1
    assert result.failed_source_count == 0
    assert len(documents) == 1

    document = documents[0]
    source_dir = (service.store.root / document.path).parent

    assert document.path.startswith("raw/blog-post/")
    assert document.frontmatter.id.startswith("2026-04-07-openai-website-openai-test-launch-")
    assert document.frontmatter.kind == "blog-post"
    assert document.frontmatter.source_name == "OpenAI Website"
    assert document.frontmatter.source_url == "https://openai.com/index/test-launch"
    assert document.frontmatter.asset_paths == ["original.html"]
    assert document.body.strip() == "OpenAI shipped a new research release."
    assert (source_dir / "original.html").read_text(encoding="utf-8") == "<html><body>OpenAI launch</body></html>"

    _install_summary_stub(monkeypatch)
    index = VaultIngestionService().rebuild_items_index(trigger="test_sync")

    assert len(index.items) == 1
    assert index.items[0].kind == "blog-post"
    assert index.items[0].content_type == ContentType.POST


def test_source_sync_publishes_live_progress_counts_while_running(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "anthropic-research")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    index_html = "<html><body>{links}</body></html>".format(
        links="".join(
            f'<a href="/research/post-{index:02d}">Anthropic Post {index:02d}</a>'
            for index in range(1, 3)
        )
    )

    def fake_fetch(url: str, *, timeout: float, headers=None, max_redirects: int = 5) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        assert url == "https://www.anthropic.com/research"
        return _response(url, index_html, content_type="text/html")

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

    original_log = RunRecorder.log
    progress_snapshots: list[tuple[int | None, int | None, int, int, str]] = []

    def _recording_log(self, run, message, *, level="info"):
        def _value(label: str) -> int | None:
            raw_value = next((entry.value for entry in run.basic_info if entry.label == label), None)
            if raw_value is None:
                return None
            return int(raw_value)

        progress_snapshots.append(
            (
                _value("Inputs planned"),
                _value("Inputs processed"),
                run.created_count,
                run.updated_count,
                message,
            )
        )
        return original_log(self, run, message, level=level)

    monkeypatch.setattr(RunRecorder, "log", _recording_log)

    run = service.sync_source_by_id(
        "anthropic-research",
        trigger="manual_source_fetch",
        max_items=2,
    )

    document_snapshots = [
        snapshot
        for snapshot in progress_snapshots
        if snapshot[4].startswith('Anthropic Research: created blog-post "')
    ]

    assert run.status == RunStatus.SUCCEEDED
    assert [snapshot[0] for snapshot in document_snapshots] == [2, 2]
    assert [snapshot[1] for snapshot in document_snapshots] == [1, 2]
    assert [snapshot[2] for snapshot in document_snapshots] == [1, 2]
    assert [snapshot[3] for snapshot in document_snapshots] == [0, 0]


def test_source_sync_extracts_website_entries_in_parallel(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "anthropic-research")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    entries = [
        WebsiteEntry(
            link=f"https://www.anthropic.com/research/post-{index:02d}",
            title=f"Anthropic Post {index:02d}",
            published_at=datetime(2026, 4, index, 12, 0, tzinfo=UTC),
        )
        for index in range(1, 4)
    ]
    monkeypatch.setattr(
        service,
        "_discover_website_entries",
        lambda current_source, *, max_entries=20, alphaxiv_sort=None, run=None: entries[:max_entries],
    )

    state = {
        "active": 0,
        "max_active": 0,
        "started": 0,
    }
    lock = threading.Lock()
    all_started = threading.Event()

    def fake_extract(entry: WebsiteEntry) -> ExtractedContent:
        with lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            state["started"] += 1
            if state["started"] == len(entries):
                all_started.set()
        try:
            all_started.wait(timeout=1.0)
            return ExtractedContent(
                title=entry.title,
                cleaned_text=f"{entry.title} body.",
                outbound_links=[],
                published_at=entry.published_at,
                mime_type="text/html",
                extraction_confidence=0.98,
                raw_payload={"html": f"<html><body>{entry.title}</body></html>"},
            )
        finally:
            with lock:
                state["active"] -= 1

    monkeypatch.setattr(service, "_extract_website_entry", fake_extract)

    run = service.sync_source_by_id(
        "anthropic-research",
        trigger="manual_source_fetch",
        max_items=3,
    )

    assert run.status == RunStatus.SUCCEEDED
    assert state["max_active"] == 3


def test_sync_source_by_id_allows_different_sources_to_run_concurrently(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    first_source = next(source for source in DEFAULT_SOURCES.sources if source.id == "openai-website")
    second_source = next(source for source in DEFAULT_SOURCES.sources if source.id == "anthropic-research")
    service.store.save_sources_config(
        VaultSourcesConfig(
            sources=[
                first_source.model_copy(deep=True),
                second_source.model_copy(deep=True),
            ]
        )
    )

    state = {
        "active": 0,
        "max_active": 0,
        "started": 0,
    }
    lock = threading.Lock()
    both_started = threading.Event()
    results: dict[str, IngestionRunHistoryRead] = {}
    errors: list[Exception] = []

    def fake_sync_source_with_run(source, *, trigger: str, max_items=None, alphaxiv_sort=None):
        del max_items, alphaxiv_sort
        with lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            state["started"] += 1
            if state["started"] == 2:
                both_started.set()
        both_started.wait(timeout=1.0)
        try:
            run = service.runs.start(
                run_type=IngestionRunType.INGEST,
                operation_kind="raw_fetch",
                trigger=f"{trigger}:{source.id}",
                title=f"Fetch {source.name}",
                summary=f"Fetching raw documents for {source.name}.",
            )
            run.total_titles = 1
            run.created_count = 1
            return 1, service.runs.finish(
                run,
                status=RunStatus.SUCCEEDED,
                summary=f"Fetched 1 raw document for {source.name}.",
            )
        finally:
            with lock:
                state["active"] -= 1

    monkeypatch.setattr(service, "_sync_source_with_run", fake_sync_source_with_run)

    def run_fetch(source_id: str) -> None:
        try:
            results[source_id] = service.sync_source_by_id(
                source_id,
                trigger="manual_source_fetch",
            )
        except Exception as exc:  # pragma: no cover - assertion below reports details
            errors.append(exc)

    first_thread = threading.Thread(target=run_fetch, args=("openai-website",))
    second_thread = threading.Thread(target=run_fetch, args=("anthropic-research",))
    first_thread.start()
    second_thread.start()
    first_thread.join(timeout=2.0)
    second_thread.join(timeout=2.0)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert not errors
    assert set(results) == {"openai-website", "anthropic-research"}
    assert all(run.status == RunStatus.SUCCEEDED for run in results.values())
    assert state["max_active"] == 2


def test_sync_source_by_id_rejects_duplicate_active_source_fetch(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "openai-website")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    started = threading.Event()
    release = threading.Event()
    result_holder: dict[str, IngestionRunHistoryRead] = {}
    error_holder: list[Exception] = []

    def fake_sync_source_with_run(source, *, trigger: str, max_items=None, alphaxiv_sort=None):
        del max_items, alphaxiv_sort
        started.set()
        release.wait(timeout=1.0)
        run = service.runs.start(
            run_type=IngestionRunType.INGEST,
            operation_kind="raw_fetch",
            trigger=f"{trigger}:{source.id}",
            title=f"Fetch {source.name}",
            summary=f"Fetching raw documents for {source.name}.",
        )
        run.total_titles = 1
        run.created_count = 1
        return 1, service.runs.finish(
            run,
            status=RunStatus.SUCCEEDED,
            summary=f"Fetched 1 raw document for {source.name}.",
        )

    monkeypatch.setattr(service, "_sync_source_with_run", fake_sync_source_with_run)

    def run_fetch() -> None:
        try:
            result_holder["run"] = service.sync_source_by_id(
                "openai-website",
                trigger="manual_source_fetch",
            )
        except Exception as exc:  # pragma: no cover - assertion below reports details
            error_holder.append(exc)

    thread = threading.Thread(target=run_fetch)
    thread.start()
    assert started.wait(timeout=1.0)

    with pytest.raises(RuntimeError, match="Raw fetch is already active for OpenAI Website\\."):
        service.sync_source_by_id("openai-website", trigger="manual_source_fetch")

    release.set()
    thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert not error_holder
    assert result_holder["run"].status == RunStatus.SUCCEEDED


def test_openai_source_sync_uses_feed_summary_when_article_fetch_fails(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "openai-website")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    article_url = "https://openai.com/index/introducing-openai-safety-fellowship"
    feed = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>OpenAI News</title>
    <item>
      <title>Announcing the OpenAI Safety Fellowship</title>
      <description><![CDATA[<p>A pilot program to support independent safety and alignment research.</p>]]></description>
      <link>{article_url}</link>
      <pubDate>Mon, 06 Apr 2026 10:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

    def fake_fetch(url: str, *, timeout: float, headers=None, max_redirects: int = 5) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        assert url == "https://openai.com/news/rss.xml"
        return _response(url, feed, content_type="application/rss+xml")

    def failing_extract(self, url: str) -> ExtractedContent:
        raise httpx.HTTPStatusError(
            "403 Forbidden",
            request=httpx.Request("GET", url),
            response=httpx.Response(403, request=httpx.Request("GET", url)),
        )

    monkeypatch.setattr("app.services.vault_sources.fetch_safe_response", fake_fetch)
    monkeypatch.setattr(
        "app.integrations.extractors.ContentExtractor.extract_from_url",
        failing_extract,
    )

    result = service.sync_enabled_sources(trigger="test_sync")
    documents = service.store.list_raw_documents()

    assert result.synced_document_count == 1
    assert len(documents) == 1
    assert documents[0].frontmatter.kind == "blog-post"
    assert documents[0].frontmatter.asset_paths == []
    assert documents[0].body.strip() == "A pilot program to support independent safety and alignment research."


def test_source_sync_uses_embedded_feed_html_without_refetching_article(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "openai-website")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    article_url = "https://openai.com/index/embedded-feed-article"
    repeated_paragraph = " ".join(
        [
            "This article explains how agent systems coordinate tools, memory, and evaluation signals."
            for _ in range(12)
        ]
    )
    embedded_html = f"""
<html>
  <head>
    <title>Embedded Feed Article</title>
    <meta property="article:published_time" content="2026-04-07T12:00:00Z" />
  </head>
  <body>
    <article>
      <p>{repeated_paragraph}</p>
      <p>{repeated_paragraph}</p>
    </article>
  </body>
</html>
""".strip()
    feed = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>OpenAI News</title>
    <item>
      <title>Embedded Feed Article</title>
      <description><![CDATA[<p>Short teaser only.</p>]]></description>
      <link>{article_url}</link>
      <pubDate>Tue, 07 Apr 2026 12:00:00 GMT</pubDate>
      <content:encoded><![CDATA[{embedded_html}]]></content:encoded>
    </item>
  </channel>
</rss>
"""

    def fake_fetch(url: str, *, timeout: float, headers=None, max_redirects: int = 5) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        assert url == "https://openai.com/news/rss.xml"
        return _response(url, feed, content_type="application/rss+xml")

    def unexpected_extract(self, url: str) -> ExtractedContent:
        raise AssertionError(f"extract_from_url should not be called for {url}")

    monkeypatch.setattr("app.services.vault_sources.fetch_safe_response", fake_fetch)
    monkeypatch.setattr(
        "app.integrations.extractors.ContentExtractor.extract_from_url",
        unexpected_extract,
    )

    result = service.sync_enabled_sources(trigger="test_sync")
    documents = service.store.list_raw_documents()

    assert result.synced_document_count == 1
    assert len(documents) == 1
    assert documents[0].frontmatter.title == "Embedded Feed Article"
    assert documents[0].frontmatter.asset_paths == ["original.html"]
    assert "agent systems coordinate tools, memory, and evaluation signals." in documents[0].body


def test_source_sync_can_fetch_multiple_sources_in_parallel(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    first_source = next(source for source in DEFAULT_SOURCES.sources if source.id == "openai-website")
    second_source = next(source for source in DEFAULT_SOURCES.sources if source.id == "anthropic-research")
    service.store.save_sources_config(
        VaultSourcesConfig(
            sources=[
                first_source.model_copy(deep=True),
                second_source.model_copy(deep=True),
            ]
        )
    )

    state = {
        "active": 0,
        "max_active": 0,
        "started": 0,
    }
    lock = threading.Lock()
    both_started = threading.Event()

    def fake_sync_source_with_run(source, *, trigger: str, max_items=None, alphaxiv_sort=None):
        del max_items, alphaxiv_sort
        run = service.runs.start(
            run_type=IngestionRunType.INGEST,
            operation_kind="raw_fetch",
            trigger=f"{trigger}:{source.id}",
            title=f"Fetch {source.name}",
            summary=f"Fetching raw documents for {source.name}.",
        )
        with lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            state["started"] += 1
            if state["started"] == 2:
                both_started.set()
        both_started.wait(timeout=1.0)
        run.total_titles = 1
        run.created_count = 1
        record = service.runs.finish(
            run,
            status=RunStatus.SUCCEEDED,
            summary=f"Fetched 1 raw document for {source.name}.",
        )
        with lock:
            state["active"] -= 1
        return 1, record

    monkeypatch.setattr(service, "_sync_source_with_run", fake_sync_source_with_run)

    result = service.sync_enabled_sources(trigger="test_sync", parallel=True)

    assert result.source_count == 2
    assert result.synced_document_count == 2
    assert result.failed_source_count == 0
    assert state["max_active"] == 2


def test_source_sync_can_be_stopped_mid_run(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "openai-website")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    entries = [
      WebsiteEntry(
          link=f"https://openai.com/index/test-launch-{index}",
          title=f"Test Launch {index}",
          published_at=datetime(2026, 4, index, 12, 0, tzinfo=UTC),
      )
      for index in range(1, 4)
    ]
    monkeypatch.setattr(
        service,
        "_discover_website_entries",
        lambda current_source, *, max_entries=20, alphaxiv_sort=None, run=None: entries[:max_entries],
    )

    extracted_count = {"value": 0}

    def fake_extract(entry: WebsiteEntry) -> ExtractedContent:
        extracted_count["value"] += 1
        if extracted_count["value"] == 1:
            run = service.latest_run_for_source("openai-website")
            assert run is not None
            service.store.request_operation_stop(
                run_id=run.id,
                source_id="openai-website",
                requested_by="test",
            )
        return ExtractedContent(
            title=entry.title,
            cleaned_text=f"{entry.title} body.",
            outbound_links=[],
            published_at=entry.published_at,
            mime_type="text/html",
            extraction_confidence=0.98,
            raw_payload={},
        )

    monkeypatch.setattr(service, "_extract_website_entry", fake_extract)

    with pytest.raises(SourceFetchCancelledError) as exc_info:
        service.sync_source_by_id("openai-website", trigger="test_cancel")

    assert "canceled" in str(exc_info.value).lower()
    documents = service.store.list_raw_documents()
    assert len(documents) == 0

    run = service.latest_run_for_source("openai-website")
    assert run is not None
    assert run.status == RunStatus.FAILED
    assert "canceled" in run.summary.lower()
    assert any(entry.label == "Canceled" and entry.value == "local-control" for entry in run.basic_info)
    assert not service.store.is_operation_stop_requested(run.id)


def test_alphaxiv_source_sync_writes_rich_paper_raw_documents(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "alphaxiv-paper")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    paper_url = "https://www.alphaxiv.org/abs/2604.03128"
    publication_ms = int(datetime(2026, 4, 3, 12, 0, tzinfo=UTC).timestamp() * 1000)
    updated_ms = int(datetime(2026, 4, 7, 9, 0, tzinfo=UTC).timestamp() * 1000)

    monkeypatch.setattr(
        service,
        "_discover_website_entries",
        lambda current_source, *, max_entries=20, alphaxiv_sort=None, run=None: [
            WebsiteEntry(
                link=paper_url,
                title="Self-Distilled RLVR",
                published_at=datetime(2026, 4, 3, 12, 0, tzinfo=UTC),
            )
        ],
    )

    paper_payload = {
        "type": "paper",
        "groupId": "group-123",
        "versionId": "version-123",
        "firstPublicationDate": publication_ms,
        "publicationDate": publication_ms,
        "sourceName": "alphaXiv",
        "sourceUrl": "https://arxiv.org/abs/2604.03128",
        "citationBibtex": "@article{rlsd2026,\n  title={Self-Distilled RLVR}\n}",
        "citationsCount": 7,
        "versionOrder": 1,
        "versionLabel": "v1",
        "title": "Self-Distilled RLVR",
        "abstract": "An alphaXiv paper abstract.",
        "license": "http://creativecommons.org/licenses/by/4.0/",
        "resources": [{"label": "Project page", "url": "https://example.com/project"}],
        "versions": [{"label": "v1"}],
        "universalId": "2604.03128",
    }
    preview_payload = {
        "id": "preview-123",
        "canonical_id": "2604.03128v1",
        "version_id": "version-123",
        "paper_group_id": "group-123",
        "title": "Self-Distilled RLVR",
        "abstract": "An alphaXiv paper abstract.",
        "paper_summary": {
            "summary": "RLSD separates environment-anchored update direction from self-distilled update magnitude.",
            "originalProblem": [
                "Sparse rewards make reasoning credit assignment coarse.",
                "OPSD leaks privileged information during training.",
            ],
            "solution": [
                "Use privileged teacher signals only to scale token-level credit.",
            ],
            "keyInsights": [
                "Directional isolation preserves stable optimization.",
            ],
            "results": [
                "RLSD outperforms standard GRPO on multimodal reasoning.",
            ],
        },
        "image_url": "image/2604.03128v1.png",
        "metrics": {
            "visits_count": {"all": 1040, "last_7_days": 77},
            "total_votes": 21,
            "public_total_votes": 51,
            "x_likes": 3,
        },
        "topics": ["agents", "cs.LG", "knowledge-distillation"],
        "authors": ["Chenxu Yang", "Chuanyu Qin"],
        "full_authors": [
            {"id": "author-1", "full_name": "Chenxu Yang"},
            {"id": "author-2", "full_name": "Chuanyu Qin"},
        ],
        "author_info": [{"name": "Chenxu Yang", "affiliation": "Chinese Academy of Sciences"}],
        "organization_info": [{"name": "Chinese Academy of Sciences"}],
        "github_url": "https://github.com/example/rlsd",
        "github_stars": 42,
        "first_publication_date": publication_ms,
        "publication_date": publication_ms,
        "updated_at": updated_ms,
    }
    legacy_payload = {
        "comments": "Accepted to ExampleConf.",
        "paper": {
            "paper_group": {
                "id": "group-123",
                "podcast_path": "group-123/podcast.mp3",
                "topics": ["agents", "cs.LG", "knowledge-distillation"],
                "metrics": {
                    "questions_count": 0,
                    "upvotes_count": 51,
                    "downvotes_count": 0,
                    "visits_count": {"all": 1040, "last7Days": 77},
                    "total_votes": 21,
                    "public_total_votes": 51,
                },
                "source": {
                    "name": "alphaXiv",
                    "url": "https://arxiv.org/abs/2604.03128",
                },
            },
            "paper_version": {
                "title": "Self-Distilled RLVR",
                "license": "http://creativecommons.org/licenses/by/4.0/",
                "imageURL": "image/2604.03128v1.png",
                "published_date": None,
                "release_date": None,
            },
            "pdf_info": {
                "fetcher_url": "https://fetcher.alphaxiv.org/v2/pdf/2604.03128v1.pdf",
            },
            "authors": [
                {"id": "author-1", "full_name": "Chenxu Yang"},
                {"id": "author-2", "full_name": "Chuanyu Qin"},
            ],
        },
    }
    similar_payload = [
        {
            "title": "Reinforcement Learning via Self-Distillation",
            "canonical_id": "2601.20802v2",
            "paper_summary": {
                "summary": "A related self-distillation method for RLVR.",
            },
            "github_url": "https://github.com/lasgroup/SDPO",
            "github_stars": 8,
        },
        {
            "title": "Self-Distilled Reasoner",
            "canonical_id": "2601.18734v3",
            "paper_summary": {
                "summary": "Another baseline for on-policy self-distillation.",
            },
        },
    ]
    overview_status_payload = {
        "state": "done",
        "translations": {
            "de": {"state": "done"},
            "en": {"state": "done"},
        },
    }
    overview_payload = {
        "title": "Self-Distilled RLVR",
        "summary": preview_payload["paper_summary"],
        "overview": "A longform AI overview.\n\nIt explains why RLSD is more stable than OPSD.",
        "citations": [
            {
                "title": "DeepSeekMath",
                "justification": "Introduces GRPO, the baseline RLVR method.",
            }
        ],
    }
    ai_detection_payload = {
        "state": "done",
        "predictionShort": "Mixed",
        "headline": "Mostly Human, AI Detected",
        "fractionAi": 0.23,
        "fractionAiAssisted": 0.02,
        "fractionHuman": 0.75,
    }
    transcript_payload = [
        {"speaker": "John", "line": "Welcome to the lecture."},
        {"speaker": "Noah", "line": "Is there a problem with OPSD?"},
    ]
    podcast_bytes = b"fake-podcast-audio"

    json_responses = {
        "https://api.alphaxiv.org/papers/v3/2604.03128": paper_payload,
        "https://api.alphaxiv.org/papers/v3/2604.03128/preview": preview_payload,
        "https://api.alphaxiv.org/papers/v3/legacy/2604.03128": legacy_payload,
        "https://api.alphaxiv.org/papers/v3/2604.03128/similar-papers": similar_payload,
        "https://api.alphaxiv.org/papers/v3/version-123/overview/status": overview_status_payload,
        "https://api.alphaxiv.org/papers/v3/version-123/overview/en": overview_payload,
        "https://api.alphaxiv.org/papers/v3/version-123/ai-detection": ai_detection_payload,
        "https://paper-podcasts.alphaxiv.org/group-123/transcript.json": transcript_payload,
    }

    def fake_alphaxiv_fetch(url: str, *, timeout: float, headers=None, max_redirects: int = 5) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        if url in json_responses:
            return _json_response(url, json_responses[url])
        if url == "https://paper-podcasts.alphaxiv.org/group-123/podcast.mp3":
            return _bytes_response(url, podcast_bytes, content_type="audio/mpeg")
        raise AssertionError(url)

    monkeypatch.setattr("app.integrations.alphaxiv.fetch_safe_response", fake_alphaxiv_fetch)

    result = service.sync_enabled_sources(trigger="test_sync")
    documents = service.store.list_raw_documents()

    assert result.source_count == 1
    assert result.synced_document_count == 1
    assert result.failed_source_count == 0
    assert len(documents) == 1

    document = documents[0]
    source_dir = (service.store.root / document.path).parent

    assert document.path.startswith("raw/paper/")
    assert document.frontmatter.kind == "paper"
    assert document.frontmatter.source_name == "alphaXiv Papers"
    assert document.frontmatter.source_pipeline_id == "alphaxiv-paper"
    assert document.frontmatter.source_url == paper_url
    assert document.frontmatter.short_summary == preview_payload["paper_summary"]["summary"]
    assert document.frontmatter.authors == ["Chenxu Yang", "Chuanyu Qin"]
    assert "knowledge-distillation" in document.frontmatter.tags
    assert "audio" in document.frontmatter.tags
    assert "transcript" in document.frontmatter.tags
    assert document.frontmatter.asset_paths == [
        "alphaxiv-ai-detection.json",
        "alphaxiv-citation.bib",
        "alphaxiv-legacy.json",
        "alphaxiv-metadata.json",
        "alphaxiv-overview-status.json",
        "alphaxiv-overview.json",
        "alphaxiv-overview.md",
        "alphaxiv-paper.json",
        "alphaxiv-podcast.mp3",
        "alphaxiv-preview.json",
        "alphaxiv-similar-papers.json",
        "alphaxiv-transcript.json",
        "alphaxiv-transcript.md",
    ]
    assert "## alphaXiv Summary" in document.body
    assert "## Audio Summary" in document.body
    assert "Audio asset: `alphaxiv-podcast.mp3`" in document.body
    assert "Mostly Human, AI Detected" in document.body
    assert "Reinforcement Learning via Self-Distillation" in document.body
    assert "Saved in `alphaxiv-overview.md` and `alphaxiv-overview.json`." in document.body

    transcript_markdown = (source_dir / "alphaxiv-transcript.md").read_text(encoding="utf-8")
    overview_markdown = (source_dir / "alphaxiv-overview.md").read_text(encoding="utf-8")
    metadata_json = (source_dir / "alphaxiv-metadata.json").read_text(encoding="utf-8")
    podcast_file = source_dir / "alphaxiv-podcast.mp3"

    assert "**John:** Welcome to the lecture." in transcript_markdown
    assert "Saved audio file: `alphaxiv-podcast.mp3`" in transcript_markdown
    assert "A longform AI overview." in overview_markdown
    assert '"podcast_url": "https://paper-podcasts.alphaxiv.org/group-123/podcast.mp3"' in metadata_json
    assert '"similar_papers_count": 2' in metadata_json
    assert podcast_file.read_bytes() == podcast_bytes

    index = VaultIngestionService().rebuild_items_index(trigger="test_sync")
    item = next(record for record in index.items if record.source_id == "alphaxiv-paper")
    assert item.kind == "paper"
    assert item.content_type == ContentType.PAPER
    assert item.short_summary == preview_payload["paper_summary"]["summary"]

    first_hash = document.frontmatter.content_hash
    paper_payload["citationsCount"] = 29
    preview_payload["metrics"] = {
        "visits_count": {"all": 2048, "last_7_days": 131},
        "total_votes": 55,
        "public_total_votes": 144,
        "x_likes": 9,
    }
    preview_payload["updated_at"] = updated_ms + 86_400_000

    second_result = service.sync_enabled_sources(trigger="test_sync_again")
    refreshed_documents = service.store.list_raw_documents()

    assert second_result.synced_document_count == 1
    assert len(refreshed_documents) == 1
    assert refreshed_documents[0].frontmatter.content_hash == first_hash
    assert "Visits (all): 2048" in refreshed_documents[0].body
    assert "Citations: 29" in refreshed_documents[0].body


def test_alphaxiv_source_discovery_pages_feed_and_uses_profile_filters(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "alphaxiv-paper")
    requested_urls: list[str] = []

    monkeypatch.setattr(
        "app.services.vault_sources.load_profile_snapshot",
        lambda: type(
            "ProfileSnapshot",
            (),
            {
                "alphaxiv_search_settings": AlphaXivSearchSettings(
                    topics=["agents", "reasoning"],
                    organizations=["OpenAI"],
                    sort="Recommended",
                    interval="90 Days",
                    source="GitHub",
                )
            },
        )(),
    )

    def build_paper_payload(index: int) -> dict[str, object]:
        return {
            "universalId": f"2604.{index:05d}",
            "title": f"alphaXiv Paper {index:03d}",
            "publicationDate": int(
                datetime(2026, 4, 1, 12, 0, tzinfo=UTC).timestamp() * 1000
            )
            + (index * 1_000),
            "paper_summary": {"summary": f"Summary {index:03d}"},
        }

    page_payloads = {
        1: {"papers": [build_paper_payload(index) for index in range(1, 51)]},
        2: {"papers": [build_paper_payload(index) for index in range(51, 61)]},
    }

    def fake_alphaxiv_fetch(
        url: str, *, timeout: float, headers=None, max_redirects: int = 5
    ) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        requested_urls.append(url)

        parsed = urlparse(url)
        assert parsed.path == "/papers/v3/feed"
        params = parse_qs(parsed.query)
        page_num = int(params["pageNum"][0])
        assert params["pageSize"] == ["50"]
        assert params["sort"] == ["Recommended"]
        assert params["interval"] == ["90 Days"]
        assert params["source"] == ["GitHub"]
        assert json.loads(params["topics"][0]) == ["agents", "reasoning"]
        assert json.loads(params["organizations"][0]) == ["OpenAI"]

        return _json_response(url, page_payloads[page_num])

    monkeypatch.setattr("app.integrations.alphaxiv.fetch_safe_response", fake_alphaxiv_fetch)

    entries = service._discover_website_entries(source, max_entries=55)

    assert len(entries) == 55
    assert [entry.link for entry in entries[:3]] == [
        "https://www.alphaxiv.org/abs/2604.00001",
        "https://www.alphaxiv.org/abs/2604.00002",
        "https://www.alphaxiv.org/abs/2604.00003",
    ]
    assert entries[-1].link == "https://www.alphaxiv.org/abs/2604.00055"
    assert entries[-1].title == "alphaXiv Paper 055"
    assert entries[-1].summary == "Summary 055"
    assert [parse_qs(urlparse(url).query)["pageNum"][0] for url in requested_urls] == ["1", "2"]


def test_alphaxiv_source_discovery_accepts_per_run_sort_override(monkeypatch) -> None:
    service = VaultSourceIngestionService()
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "alphaxiv-paper")

    monkeypatch.setattr(
        "app.services.vault_sources.load_profile_snapshot",
        lambda: type(
            "ProfileSnapshot",
            (),
            {
                "alphaxiv_search_settings": AlphaXivSearchSettings(
                    topics=["agents"],
                    organizations=["OpenAI"],
                    sort="Hot",
                    interval="30 Days",
                    source="GitHub",
                )
            },
        )(),
    )

    def fake_alphaxiv_fetch(
        url: str, *, timeout: float, headers=None, max_redirects: int = 5
    ) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)

        parsed = urlparse(url)
        assert parsed.path == "/papers/v3/feed"
        params = parse_qs(parsed.query)
        assert params["sort"] == ["Likes"]
        assert params["interval"] == ["30 Days"]
        assert params["source"] == ["GitHub"]
        assert json.loads(params["topics"][0]) == ["agents"]
        assert json.loads(params["organizations"][0]) == ["OpenAI"]

        return _json_response(
            url,
            {
                "papers": [
                    {
                        "universalId": "2604.00001",
                        "title": "Most liked AlphaXiv paper",
                        "publicationDate": int(
                            datetime(2026, 4, 1, 12, 0, tzinfo=UTC).timestamp() * 1000
                        ),
                        "paper_summary": {"summary": "Popular paper summary."},
                    }
                ]
            },
        )

    monkeypatch.setattr("app.integrations.alphaxiv.fetch_safe_response", fake_alphaxiv_fetch)

    entries = service._discover_website_entries(
        source,
        max_entries=1,
        alphaxiv_sort="Likes",
    )

    assert len(entries) == 1
    assert entries[0].link == "https://www.alphaxiv.org/abs/2604.00001"
    assert entries[0].title == "Most liked AlphaXiv paper"
    assert entries[0].summary == "Popular paper summary."


def test_anthropic_source_discovery_filters_research_index_links(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "anthropic-research")
    html = """
<html>
  <body>
    <a href="/research">Research</a>
    <a href="/research/team/alignment">Team</a>
    <a href="/research/constitutional-classifiers"><h2>Constitutional Classifiers</h2></a>
    <a href="https://www.anthropic.com/research/evals-at-scale">Evals at Scale</a>
    <a href="https://external.example.com/research/ignore">Ignore</a>
  </body>
</html>
"""

    def fake_fetch(url: str, *, timeout: float, headers=None, max_redirects: int = 5) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        assert url == "https://www.anthropic.com/research"
        return _response(url, html, content_type="text/html")

    monkeypatch.setattr("app.services.vault_sources.fetch_safe_response", fake_fetch)

    entries = service._discover_website_entries(source)

    assert [entry.link for entry in entries] == [
        "https://www.anthropic.com/research/constitutional-classifiers",
        "https://www.anthropic.com/research/evals-at-scale",
    ]


def test_the_batch_source_discovery_filters_research_tag_index_links(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "the-batch-research")
    html = """
<html>
  <body>
    <a href="/the-batch/">The Batch</a>
    <a href="/the-batch/about/">About</a>
    <a href="/the-batch/tag/research/">Research</a>
    <a href="/the-batch/stanford-and-together-ai-researchers-chart-edge-models-performance-in-intelligence-per-watt/">
      <h2>Can Local AI Stand In for the Cloud?</h2>
    </a>
    <a href="https://www.deeplearning.ai/the-batch/test-time-training-end-to-end-ttt-e2e-retrains-model-weights-to-handle-long-inputs/">
      Test-Time Training End-to-End
    </a>
    <a href="https://external.example.com/the-batch/offsite">Ignore</a>
  </body>
</html>
"""

    def fake_fetch(url: str, *, timeout: float, headers=None, max_redirects: int = 5) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        assert url == "https://www.deeplearning.ai/the-batch/tag/research/"
        return _response(url, html, content_type="text/html")

    monkeypatch.setattr("app.services.vault_sources.fetch_safe_response", fake_fetch)

    entries = service._discover_website_entries(source)

    assert [entry.link for entry in entries] == [
        (
            "https://www.deeplearning.ai/the-batch/"
            "stanford-and-together-ai-researchers-chart-edge-models-performance-in-intelligence-per-watt"
        ),
        (
            "https://www.deeplearning.ai/the-batch/"
            "test-time-training-end-to-end-ttt-e2e-retrains-model-weights-to-handle-long-inputs"
        ),
    ]
    assert [entry.title for entry in entries] == [
        "Can Local AI Stand In for the Cloud?",
        "Test-Time Training End-to-End",
    ]


def test_anthropic_source_sync_extracts_publish_date_from_header_markup(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "anthropic-research")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    article_url = "https://www.anthropic.com/research/vibe-physics"
    index_html = """
<html>
  <body>
    <a href="/research/vibe-physics"><h2>Vibe physics: The AI grad student</h2></a>
  </body>
</html>
"""
    article_html = """
<html>
  <head>
    <title>Vibe physics: The AI grad student \\ Anthropic</title>
  </head>
  <body>
    <main>
      <article>
        <div class="hero">
          <div class="header">
            <div class="body-3 bold"><span>Science</span></div>
            <h1>Vibe physics: The AI grad student</h1>
            <div class="body-3 agate">Mar 23, 2026</div>
          </div>
        </div>
        <div class="body">
          <p>Can AI do theoretical physics? This guest post explains the experiment.</p>
        </div>
      </article>
    </main>
  </body>
</html>
"""

    def fake_fetch(url: str, *, timeout: float, headers=None, max_redirects: int = 5) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        if url == "https://www.anthropic.com/research":
            return _response(url, index_html, content_type="text/html")
        if url == article_url:
            return _response(url, article_html, content_type="text/html")
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr("app.services.vault_sources.fetch_safe_response", fake_fetch)
    monkeypatch.setattr("app.integrations.extractors.fetch_safe_response", fake_fetch)

    result = service.sync_enabled_sources(trigger="test_sync")
    documents = service.store.list_raw_documents()

    assert result.source_count == 1
    assert result.synced_document_count == 1
    assert result.failed_source_count == 0
    assert len(documents) == 1
    assert documents[0].frontmatter.title == "Vibe physics: The AI grad student"
    assert documents[0].frontmatter.source_url == article_url
    assert documents[0].frontmatter.published_at == datetime(2026, 3, 23, tzinfo=UTC)
    assert documents[0].frontmatter.asset_paths == ["original.html"]


def test_vault_index_backfills_missing_published_at_from_original_html(client) -> None:
    store = VaultStore()
    store.ensure_layout()
    frontmatter = RawDocumentFrontmatter(
        id="2026-04-07-anthropic-research-vibe-physics-fixture",
        kind="blog-post",
        title="Vibe physics: The AI grad student \\ Anthropic",
        source_url="https://www.anthropic.com/research/vibe-physics",
        source_name="Anthropic Research",
        authors=["Anthropic"],
        published_at=None,
        ingested_at=datetime(2026, 4, 7, 12, 0, tzinfo=UTC),
        content_hash="",
        tags=["anthropic", "research"],
        status="active",
        asset_paths=["original.html"],
        source_id="anthropic-research",
        source_pipeline_id="anthropic-research",
        external_key="https://www.anthropic.com/research/vibe-physics",
        canonical_url="https://www.anthropic.com/research/vibe-physics",
        doc_role="primary",
        parent_id=None,
        index_visibility="visible",
        fetched_at=datetime(2026, 4, 7, 12, 0, tzinfo=UTC),
    )
    store.write_raw_document(
        kind="blog-post",
        doc_id=frontmatter.id,
        frontmatter=frontmatter,
        body="Can AI do theoretical physics? This guest post explains the experiment.",
    )
    html_path = store.raw_dir / "blog-post" / frontmatter.id / "original.html"
    store.write_text(
        html_path,
        """
<html>
  <body>
    <main>
      <article>
        <div class="hero">
          <div class="header">
            <div class="body-3 bold"><span>Science</span></div>
            <h1>Vibe physics: The AI grad student</h1>
            <div class="body-3 agate">Mar 23, 2026</div>
          </div>
        </div>
      </article>
    </main>
  </body>
</html>
""".strip(),
    )

    index = VaultIngestionService().rebuild_items_index(trigger="test_sync")
    document = next(item for item in store.list_raw_documents() if item.frontmatter.id == frontmatter.id)
    record = next(item for item in index.items if item.id == frontmatter.id)

    assert document.frontmatter.title == "Vibe physics: The AI grad student"
    assert document.frontmatter.published_at == datetime(2026, 3, 23, tzinfo=UTC)
    assert record.title == "Vibe physics: The AI grad student"
    assert record.published_at == datetime(2026, 3, 23, tzinfo=UTC)


def test_mistral_source_discovery_filters_research_category_page_links(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "mistral-research")
    html = r"""
<html>
  <body>
    <a href="/news/voxtral-tts">
      <span>Research</span>
      <h2>Speaking of Voxtral</h2>
      <p>Featured story card.</p>
    </a>
    <script>
      self.__next_f.push([1,"{\"id\":\"voxtral-research\",\"slug\":\"voxtral-tts\",\"author\":\"Mistral AI\",\"isPinned\":true,\"date\":\"2026-03-23T16:00:00\",\"category\":{\"id\":3,\"sort\":2,\"name\":\"Research\",\"parent\":1,\"locale\":\"en\"},\"title\":\"Speaking of Voxtral\",\"description\":\"Voxtral TTS release for voice agents.\",\"locale\":\"en\"},{\"id\":\"344b96e1-45ea-4572-8e1a-631552e96e19\",\"slug\":\"mistral-small-4\",\"author\":\"Mistral AI\",\"isPinned\":false,\"date\":\"2026-03-16T21:00:00\",\"category\":{\"id\":3,\"sort\":2,\"name\":\"Research\",\"parent\":1,\"locale\":\"en\"},\"title\":\"Introducing Mistral Small 4\",\"description\":null,\"locale\":\"en\"},{\"id\":\"af4fd3a1-b98b-4b51-aec4-e8f01a7f0e0c\",\"slug\":\"leanstral\",\"author\":\"Mistral AI\",\"isPinned\":false,\"date\":\"2026-03-16T16:00:00\",\"category\":{\"id\":3,\"sort\":2,\"name\":\"Research\",\"parent\":1,\"locale\":\"en\"},\"title\":\"Leanstral: Open-Source foundation for trustworthy vibe-coding\",\"description\":\"First open-source code agent for Lean 4.\",\"locale\":\"en\"},{\"id\":\"370e74b8-016d-4223-a90c-0dc60474a732\",\"slug\":\"mistral-ocr-3\",\"author\":\"Mistral AI\",\"isPinned\":true,\"date\":\"2025-12-17T15:00:00\",\"category\":{\"id\":3,\"sort\":2,\"name\":\"Research\",\"parent\":1,\"locale\":\"en\"},\"title\":\"Introducing Mistral OCR 3\",\"description\":\"Achieving a new frontier for both accuracy and efficiency in document processing.\",\"locale\":\"en\"},{\"id\":\"ignore-company\",\"slug\":\"mistral-ai-and-nvidia-partner-to-accelerate-open-frontier-models\",\"author\":\"Mistral AI\",\"isPinned\":false,\"date\":\"2026-03-16T20:00:00\",\"category\":{\"id\":2,\"sort\":5,\"name\":\"Company\",\"parent\":1,\"locale\":\"en\"},\"title\":\"Mistral AI partners with NVIDIA to accelerate open frontier models\",\"description\":\"Should be filtered out.\",\"locale\":\"en\"}"]);
    </script>
  </body>
</html>
"""

    def fake_fetch(url: str, *, timeout: float, headers=None, max_redirects: int = 5) -> httpx.Response:
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        assert url == "https://mistral.ai/news?category=research"
        return _response(url, html, content_type="text/html")

    monkeypatch.setattr("app.services.vault_sources.fetch_safe_response", fake_fetch)

    entries = service._discover_website_entries(source)

    assert [entry.link for entry in entries] == [
        "https://mistral.ai/news/voxtral-tts",
        "https://mistral.ai/news/mistral-small-4",
        "https://mistral.ai/news/leanstral",
        "https://mistral.ai/news/mistral-ocr-3",
    ]
    assert [entry.title for entry in entries] == [
        "Speaking of Voxtral",
        "Introducing Mistral Small 4",
        "Leanstral: Open-Source foundation for trustworthy vibe-coding",
        "Introducing Mistral OCR 3",
    ]
    assert entries[0].published_at == datetime(2026, 3, 23, 16, 0, tzinfo=UTC)
    assert entries[0].summary == "Voxtral TTS release for voice agents."


def test_jack_clark_source_sync_decomposes_issue_into_story_entries(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "jack-clark-import-ai")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    issue_url = (
        "https://jack-clark.net/2026/04/06/"
        "import-ai-452-scaling-laws-for-cyberwar-rising-tides-of-ai-automation-and-a-puzzle-over-gdp-forecasting/"
    )
    issue_fragment = """
<div class="entry-content">
  <p>Welcome to Import AI, a newsletter about AI research.</p>
  <p class="button-wrapper"><a href="https://importai.substack.com/subscribe?">Subscribe now</a></p>
  <p><strong>Uh oh, there's a scaling war for cyberattacks as well!:</strong><br /><em>...The smarter the system, the better the ability to cyberattack...</em>AI safety research organization Lyptus Research has looked at offensive cyber capabilities.</p>
  <p><strong>Why this matters:</strong> Offensive cyber capability is diffusing quickly.<br /><strong>Read more:</strong> <a href="https://lyptusresearch.org/research/offensive-cyber-time-horizons">Offensive Cybersecurity Time Horizons</a>.</p>
  <p>***</p>
  <p><strong>MIT: A rising tide of automation is going to make good enough AI for most text-based tasks by 2029:</strong><br /><em>...How do you revolutionize an economy? Gradually and consistently...</em>Researchers with MIT describe broad automation progress.</p>
  <p><strong>Read more:</strong> <a href="https://arxiv.org/abs/2604.01363">Crashing Waves vs. Rising Tides</a>.</p>
  <p>***</p>
  <p><strong>Tech Tales:</strong></p>
  <p>Warfare<br /><em>[2029]</em></p>
  <p><em>Thanks for reading!</em></p>
</div>
""".strip()
    feed = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>Import AI</title>
    <item>
      <title>Import AI 452: Scaling laws for cyberwar; rising tides of AI automation; and a puzzle over GDP forecasting</title>
      <link>{issue_url}</link>
      <pubDate>Mon, 06 Apr 2026 12:31:31 GMT</pubDate>
      <description><![CDATA[<p>Weekly issue summary.</p>]]></description>
      <content:encoded><![CDATA[{issue_fragment}]]></content:encoded>
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
        assert url == "https://jack-clark.net/feed/"
        return _response(url, feed, content_type="application/rss+xml")

    def failing_extract(self, url: str) -> ExtractedContent:
        raise httpx.HTTPStatusError(
            "403 Forbidden",
            request=httpx.Request("GET", url),
            response=httpx.Response(403, request=httpx.Request("GET", url)),
        )

    monkeypatch.setattr("app.services.vault_sources.fetch_safe_response", fake_fetch)
    monkeypatch.setattr(
        "app.integrations.extractors.ContentExtractor.extract_from_url",
        failing_extract,
    )

    result = service.sync_enabled_sources(trigger="test_sync")
    documents = service.store.list_raw_documents()

    assert result.source_count == 1
    assert result.synced_document_count == 3
    assert result.failed_source_count == 0
    assert len(documents) == 3

    parent = next(document for document in documents if document.frontmatter.doc_role == "primary")
    children = sorted(
        (document for document in documents if document.frontmatter.doc_role == "derived"),
        key=lambda document: document.frontmatter.title,
    )

    assert parent.frontmatter.kind == "newsletter"
    assert parent.frontmatter.source_name == "Import AI"
    assert parent.frontmatter.source_url == issue_url.rstrip("/")
    assert parent.frontmatter.index_visibility == "hidden"
    assert parent.frontmatter.asset_paths == ["original.html"]
    assert "Welcome to Import AI" not in parent.body
    assert "Subscribe now" not in parent.body
    assert "Tech Tales" not in parent.body
    assert (
        "### [Uh oh, there's a scaling war for cyberattacks as well!]"
        "(https://lyptusresearch.org/research/offensive-cyber-time-horizons)"
    ) in parent.body
    assert (
        "### [MIT: A rising tide of automation is going to make good enough AI for most "
        "text-based tasks by 2029](https://arxiv.org/abs/2604.01363)"
    ) in parent.body

    by_title = {document.frontmatter.title: document for document in children}
    assert set(by_title) == {
        "MIT: A rising tide of automation is going to make good enough AI for most text-based tasks by 2029",
        "Uh oh, there's a scaling war for cyberattacks as well!",
    }
    assert by_title["MIT: A rising tide of automation is going to make good enough AI for most text-based tasks by 2029"].frontmatter.kind == "paper"
    assert by_title["MIT: A rising tide of automation is going to make good enough AI for most text-based tasks by 2029"].frontmatter.canonical_url == "https://arxiv.org/abs/2604.01363"
    assert by_title["Uh oh, there's a scaling war for cyberattacks as well!"].frontmatter.kind == "blog-post"
    assert (
        by_title["Uh oh, there's a scaling war for cyberattacks as well!"].frontmatter.parent_id
        == parent.frontmatter.id
    )
    assert "Source newsletter: Import AI 452:" in by_title[
        "Uh oh, there's a scaling war for cyberattacks as well!"
    ].body
    assert f"Source issue: {issue_url.rstrip('/')}" in by_title[
        "Uh oh, there's a scaling war for cyberattacks as well!"
    ].body

    _install_summary_stub(monkeypatch)
    index = VaultIngestionService().rebuild_items_index(trigger="test_sync")

    visible_items = [item for item in index.items if item.index_visibility != "hidden"]
    hidden_items = [item for item in index.items if item.index_visibility == "hidden"]
    assert len(visible_items) == 2
    assert len(hidden_items) == 1
    assert {item.kind for item in visible_items} == {"blog-post", "paper"}
    assert hidden_items[0].kind == "newsletter"


def test_tldr_structured_newsletter_body_keeps_only_editorial_stories(client) -> None:
    service = VaultSourceIngestionService()
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "tldr-email")
    html = """
<table><tr><td><div class="text-block"><div style="text-align: center;"><h1><strong>TLDR 2026-04-07</strong></h1></div></div></td></tr></table>
<table><tr><td>
  <table><tr><td class="container"><div class="text-block"><span>
    <a href="https://tracking.tldrnewsletter.com/CL0/https:%2F%2Fwww.qawolf.com%2F/1/abc"><span><strong>Cut your QA cycles down to minutes with automated testing (Sponsor)</strong></span></a><br><br>
    <span>If QA is a bottleneck, you should check out QA Wolf.</span>
  </span></div></td></tr></table>
</td></tr></table>
<table><tr><td><div class="text-block"><div style="text-align: center;"><h1><strong>Big Tech &amp; Startups</strong></h1></div></div></td></tr></table>
<table><tr><td>
  <table><tr><td class="container"><div class="text-block"><span>
    <a href="https://tracking.tldrnewsletter.com/CL0/https:%2F%2Flinks.tldrnewsletter.com%2FZPmNRf/1/abc"><span><strong>Anthropic boasts revenue run rate of $30 billion (1 minute read)</strong></span></a><br><br>
    <span>Anthropic's annual revenue run-rate has spiked from roughly $9 billion to more than $30 billion.</span>
  </span></div></td></tr></table>
  <table><tr><td class="container"><div class="text-block"><span>
    <a href="https://tracking.tldrnewsletter.com/CL0/https:%2F%2Flinks.tldrnewsletter.com%2FAaOObO/1/abc"><span><strong>OpenAI's leadership reportedly disagrees about when to raise money and how to spend it (2 minute read)</strong></span></a><br><br>
    <span>Sam Altman and Sarah Friar reportedly disagree on timing and spending.</span>
  </span></div></td></tr></table>
</td></tr></table>
<table><tr><td><div class="text-block"><div style="text-align: center;"><h1><strong>Quick Links</strong></h1></div></div></td></tr></table>
<table><tr><td>
  <table><tr><td class="container"><div class="text-block"><span>
    <a href="https://tracking.tldrnewsletter.com/CL0/https:%2F%2Fblog.ronin.cloud%2Fronin-labs%2F/1/abc"><span><strong>Why the Azure Labs deprecation is the best thing to happen to your workflow. (Sponsor)</strong></span></a><br><br>
    <span>The end of Azure Labs is your chance to adopt an alternative.</span>
  </span></div></td></tr></table>
  <table><tr><td class="container"><div class="text-block"><span>
    <a href="https://tracking.tldrnewsletter.com/CL0/https:%2F%2Fjobs.ashbyhq.com%2Ftldr.tech%2F3b21aaf8-dea5-4127-be71-602d30e5001e/1/abc"><span><strong>TLDR is hiring a Senior Software Engineer, Applied AI ($250k-$350k, Fully Remote)</strong></span></a><br><br>
    <span>Join a small team using the latest AI tools with an unlimited token budget.</span>
  </span></div></td></tr></table>
  <table><tr><td class="container"><div class="text-block"><span>
    <a href="https://tracking.tldrnewsletter.com/CL0/https:%2F%2Fwww.natemeyvis.com%2Fagentic-coding-and-microservices%2F%3Futm_source=tldrnewsletter/1/abc"><span><strong>Agentic coding and microservices (2 minute read)</strong></span></a><br><br>
    <span>AI enables developers to work more monolithically.</span>
  </span></div></td></tr></table>
</td></tr></table>
"""
    message = NewsletterMessage(
        message_id="gmail-message-1",
        thread_id="gmail-thread-1",
        subject="Anthropic's revenue spike",
        sender="TLDR <dan@tldrnewsletter.com>",
        published_at=datetime(2026, 4, 7, 10, 59, tzinfo=UTC),
        text_body="A plain text fallback that should not be used when structured parsing succeeds.",
        html_body=html,
        outbound_links=[],
        permalink="https://mail.google.com/mail/u/0/#inbox/gmail-message-1",
    )

    entries = service._extract_newsletter_entries(source, message)
    body = service._render_newsletter_body(source, message, entries)

    assert "Sender:" not in body
    assert "Published At:" not in body
    assert "## Email Body" not in body
    assert "Cut your QA cycles down to minutes" not in body
    assert "Why the Azure Labs deprecation" not in body
    assert "TLDR is hiring" not in body
    assert "## Big Tech & Startups" in body
    assert "## Quick Links" in body
    assert "### [Anthropic boasts revenue run rate of $30 billion](https://links.tldrnewsletter.com/ZPmNRf)" in body
    assert "### [OpenAI's leadership reportedly disagrees about when to raise money and how to spend it](https://links.tldrnewsletter.com/AaOObO)" in body
    assert "### [Agentic coding and microservices](https://www.natemeyvis.com/agentic-coding-and-microservices)" in body
    assert "Anthropic's annual revenue run-rate has spiked from roughly $9 billion to more than $30 billion." in body
    assert "AI enables developers to work more monolithically." in body

    assert [entry.title for entry in entries] == [
        "Anthropic boasts revenue run rate of $30 billion",
        "OpenAI's leadership reportedly disagrees about when to raise money and how to spend it",
        "Agentic coding and microservices",
    ]
    assert [entry.link for entry in entries] == [
        "https://links.tldrnewsletter.com/ZPmNRf",
        "https://links.tldrnewsletter.com/AaOObO",
        "https://www.natemeyvis.com/agentic-coding-and-microservices",
    ]
    assert "Section: Big Tech & Startups" in entries[0].body
    assert "Section: Quick Links" in entries[2].body


def test_medium_structured_newsletter_body_renders_story_cards_as_markdown(client) -> None:
    service = VaultSourceIngestionService()
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "medium-email")
    html = """
<html>
  <body>
    <div>
      <p>Today's highlights</p>

      <div>
        <div>
          <a href="https://medium.com/@sa-liberty?source=email-digest"><img alt="Sam Liberty" src="author-1.png"></a>
          <span><a href="https://medium.com/@sa-liberty?source=email-digest">Sam Liberty</a></span>
          <span>in</span>
          <span><a href="https://medium.com/age-of-awareness?source=email-digest">Age of Awareness</a></span>
        </div>
        <div>
          <a href="https://medium.com/@sa-liberty/the-5-minute-mental-reset-that-actually-works-928116f8b78e?source=email-digest"><img alt="The 5-Minute Mental Reset That Actually Works" src="story-1.png"></a>
          <div>
            <a href="https://medium.com/@sa-liberty/the-5-minute-mental-reset-that-actually-works-928116f8b78e?source=email-digest">
              <h2>The 5-Minute Mental Reset That Actually Works</h2>
              <div><h3>An evidence-based routine you can try right now</h3></div>
            </a>
          </div>
        </div>
        <div>
          <span>10 min read</span>
          <img alt="Claps" src="claps.png">
          <span>6.1K</span>
          <img alt="Responses" src="responses.png">
          <span>336</span>
        </div>
      </div>

      <div>
        <div>
          <a href="https://medium.com/@vivedhaelango?source=email-digest"><img alt="Vivedha Elango" src="author-2.png"></a>
          <span><a href="https://medium.com/@vivedhaelango?source=email-digest">Vivedha Elango</a></span>
          <span>in</span>
          <span><a href="https://medium.com/gitconnected?source=email-digest">Level Up Coding</a></span>
        </div>
        <div>
          <a href="https://medium.com/@vivedhaelango/why-your-rag-system-fails-complex-questions-and-how-structure-fixes-everything-4adfc7e810d0?source=email-digest"><img alt="Why Your RAG System Fails Complex Questions? (And How Structure Fixes Everything)" src="story-2.png"></a>
          <div>
            <a href="https://medium.com/@vivedhaelango/why-your-rag-system-fails-complex-questions-and-how-structure-fixes-everything-4adfc7e810d0?source=email-digest">
              <h2>Why Your RAG System Fails Complex Questions? (And How Structure Fixes…</h2>
              <div><h3>Understanding the Retrieval and Structuring (RAS)…</h3></div>
            </a>
          </div>
        </div>
        <div>
          <span>17 min read</span>
          <img alt="Claps" src="claps.png">
          <span>747</span>
          <img alt="Responses" src="responses.png">
          <span>12</span>
        </div>
      </div>

      <div>
        <h2>See more of what you like and less of what you don’t.</h2>
        <a href="https://medium.com/me/missioncontrol?source=email-digest">Control your recommendations</a>
      </div>

      <div>
        <h2>Read from anywhere.</h2>
        <a href="https://medium.com/me/email-settings?source=email-digest">Unsubscribe</a>
      </div>
    </div>
  </body>
</html>
""".strip()
    message = NewsletterMessage(
        message_id="gmail-medium-1",
        thread_id="gmail-medium-thread-1",
        subject="The 5-Minute Mental Reset That Actually Works | Sam Liberty in Age of Awareness",
        sender="Medium Daily Digest <noreply@medium.com>",
        published_at=datetime(2026, 4, 7, 6, 40, tzinfo=UTC),
        text_body="A plain text fallback that should not be used when structured parsing succeeds.",
        html_body=html,
        outbound_links=[],
        permalink="https://mail.google.com/mail/u/0/#inbox/gmail-medium-1",
    )

    entries = service._extract_newsletter_entries(source, message)
    body = service._render_newsletter_body(source, message, entries)

    assert "Sender:" not in body
    assert "Published At:" not in body
    assert "Stories for" not in body
    assert "Become a member" not in body
    assert "Control your recommendations" not in body
    assert "Read from anywhere." not in body
    assert "Unsubscribe" not in body
    assert "## Today's highlights" in body
    assert "### [The 5-Minute Mental Reset That Actually Works](https://medium.com/@sa-liberty/the-5-minute-mental-reset-that-actually-works-928116f8b78e)" in body
    assert "### [Why Your RAG System Fails Complex Questions? (And How Structure Fixes Everything)](https://medium.com/@vivedhaelango/why-your-rag-system-fails-complex-questions-and-how-structure-fixes-everything-4adfc7e810d0)" in body
    assert "> Sam Liberty in Age of Awareness · 10 min read · 6.1K claps · 336 responses" in body
    assert "> Vivedha Elango in Level Up Coding · 17 min read · 747 claps · 12 responses" in body
    assert "An evidence-based routine you can try right now" in body
    assert "Understanding the Retrieval and Structuring (RAS)…" in body

    assert [entry.title for entry in entries] == [
        "The 5-Minute Mental Reset That Actually Works",
        "Why Your RAG System Fails Complex Questions? (And How Structure Fixes Everything)",
    ]
    assert [entry.link for entry in entries] == [
        "https://medium.com/@sa-liberty/the-5-minute-mental-reset-that-actually-works-928116f8b78e",
        "https://medium.com/@vivedhaelango/why-your-rag-system-fails-complex-questions-and-how-structure-fixes-everything-4adfc7e810d0",
    ]
    assert "Section: Today's highlights" in entries[0].body
    assert "> Sam Liberty in Age of Awareness · 10 min read · 6.1K claps · 336 responses" in entries[0].body
    assert "> Vivedha Elango in Level Up Coding · 17 min read · 747 claps · 12 responses" in entries[1].body


def test_alphasignal_structured_newsletter_body_extracts_feature_cards_and_signals(client) -> None:
    service = VaultSourceIngestionService()
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "alphasignal-email")
    html = """
<html>
  <body>
    <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
      <tr><td>Summary</td></tr>
      <tr><td>Top News</td></tr>
      <tr><td><a href="https://app.alphasignal.ai/c?lid=summary-1">▸ Meta releases Muse Spark , a reasoning model with parallel agent inference</a></td></tr>
      <tr><td>TELUS Digital</td></tr>
      <tr><td><a href="https://app.alphasignal.ai/c?lid=summary-sponsor">▸ Test your LLM against prompt injection attacks using real benchmarks</a></td></tr>
      <tr><td>Signals</td></tr>
      <tr><td><a href="https://app.alphasignal.ai/c?lid=summary-2">▸ Cursor lets you run agents remotely and control them from phone</a></td></tr>
    </table>

    <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
      <tr>
        <td style="font-family:system-ui, sans-serif;font-size:16px;font-weight:bold;">Top News</td>
      </tr>
      <tr>
        <td><table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%"><tr><td style="border-top:1px solid #6B6B6B;"></td></tr></table></td>
      </tr>
      <tr>
        <td class="h1"><span>Meta debuts Muse Spark combining multimodal inputs, tool use, and agent orchestration in one system</span></td>
      </tr>
      <tr>
        <td>23,539 Likes</td>
      </tr>
      <tr>
        <td><img alt="Feature image" src="feature-1.png"></td>
      </tr>
      <tr>
        <td>
          <div>
            <p>Meta introduces Muse Spark, the first model from its rebuilt AI stack after nine months of changes across infrastructure, architecture, and data.</p>
            <p><strong>What it does</strong></p>
            <ul>
              <li>Handles multimodal inputs in a single pipeline without separate models</li>
              <li>Runs parallel agents to improve reasoning without increasing latency</li>
            </ul>
          </div>
        </td>
      </tr>
      <tr>
        <td align="center">
          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation">
            <tr><td class="btn"><a href="https://app.alphasignal.ai/c?lid=story-1">TRY MUSE SPARK</a></td></tr>
          </table>
        </td>
      </tr>
    </table>

    <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
      <tr>
        <td style="font-family:system-ui, sans-serif;font-size:16px;font-weight:bold;">Top Papers</td>
      </tr>
      <tr>
        <td><table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%"><tr><td style="border-top:1px solid #6B6B6B;"></td></tr></table></td>
      </tr>
      <tr>
        <td class="h1"><span>Open models learn to plan better with staged verifier feedback</span></td>
      </tr>
      <tr>
        <td>8,104 Stars</td>
      </tr>
      <tr>
        <td>
          <div>
            <p>The paper studies staged verifier feedback for multi-step planning systems.</p>
            <p><strong>Key result</strong></p>
            <ul>
              <li>Improves final-answer accuracy without adding a larger base model</li>
            </ul>
          </div>
        </td>
      </tr>
      <tr>
        <td align="center">
          <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation">
            <tr><td class="btn"><a href="https://app.alphasignal.ai/c?lid=story-2">READ THE PAPER</a></td></tr>
          </table>
        </td>
      </tr>
    </table>

    <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
      <tr>
        <td class="h1">Signals</td>
      </tr>
      <tr><td><table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%"><tr><td style="border-top:1px solid #000000;"></td></tr></table></td></tr>
      <tr>
        <td style="padding:15px 0;">
          <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
            <tr>
              <td width="30" valign="top">1</td>
              <td valign="top">
                <a class="h1" href="https://app.alphasignal.ai/c?lid=signal-1">Cursor enables running agents on any machine while controlling them remotely from your phone</a>
                <span>4,301 Likes</span>
              </td>
            </tr>
          </table>
        </td>
      </tr>
      <tr><td><table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%"><tr><td style="border-top:1px solid #cbcbcb;"></td></tr></table></td></tr>
      <tr>
        <td style="padding:15px 0;">
          <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
            <tr>
              <td width="30" valign="top">2</td>
              <td valign="top">
                <a class="h1" href="https://app.alphasignal.ai/c?lid=signal-sponsor">H Company presents Computer Use Agent at HumanX reaching human level performance and topping OSW-V</a>
                <span>Presented by H Company</span>
              </td>
            </tr>
          </table>
        </td>
      </tr>
      <tr><td><table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%"><tr><td style="border-top:1px solid #cbcbcb;"></td></tr></table></td></tr>
      <tr>
        <td style="padding:15px 0;">
          <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
            <tr>
              <td width="30" valign="top">3</td>
              <td valign="top">
                <a class="h1" href="https://app.alphasignal.ai/c?lid=signal-2">OpenAI publishes Child Safety Blueprint outlining policies to prevent AI-enabled exploitation and improve safeguards</a>
                <span>2,419 Likes</span>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
""".strip()
    message = NewsletterMessage(
        message_id="gmail-alphasignal-1",
        thread_id="gmail-alphasignal-thread-1",
        subject="⚡️ Meta Muse Spark: 10x efficiency with parallel agent inference",
        sender="AlphaSignal <news@alphasignal.ai>",
        published_at=datetime(2026, 4, 9, 16, 14, 36, tzinfo=UTC),
        text_body="A plain text fallback that should not be used when structured parsing succeeds.",
        html_body=html,
        outbound_links=[],
        permalink="https://mail.google.com/mail/u/0/#inbox/gmail-alphasignal-1",
    )

    entries = service._extract_newsletter_entries(source, message)
    body = service._render_newsletter_body(source, message, entries)

    assert "Sender:" not in body
    assert "Published At:" not in body
    assert "## Email Body" not in body
    assert "Summary" not in body
    assert "Presented by H Company" not in body
    assert "Test your LLM against prompt injection attacks" not in body
    assert "## Top News" in body
    assert "## Top Papers" in body
    assert "## Signals" in body
    assert "### [Meta debuts Muse Spark combining multimodal inputs, tool use, and agent orchestration in one system](https://app.alphasignal.ai/c?lid=story-1)" in body
    assert "### [Open models learn to plan better with staged verifier feedback](https://app.alphasignal.ai/c?lid=story-2)" in body
    assert "### [Cursor enables running agents on any machine while controlling them remotely from your phone](https://app.alphasignal.ai/c?lid=signal-1)" in body
    assert "### [OpenAI publishes Child Safety Blueprint outlining policies to prevent AI-enabled exploitation and improve safeguards](https://app.alphasignal.ai/c?lid=signal-2)" in body
    assert "> 23,539 Likes" in body
    assert "> 8,104 Stars" in body
    assert "> 4,301 Likes" in body
    assert "**What it does**" in body
    assert "- Handles multimodal inputs in a single pipeline without separate models" in body
    assert "**Key result**" in body

    assert [entry.title for entry in entries] == [
        "Meta debuts Muse Spark combining multimodal inputs, tool use, and agent orchestration in one system",
        "Open models learn to plan better with staged verifier feedback",
        "Cursor enables running agents on any machine while controlling them remotely from your phone",
        "OpenAI publishes Child Safety Blueprint outlining policies to prevent AI-enabled exploitation and improve safeguards",
    ]
    assert [entry.link for entry in entries] == [
        "https://app.alphasignal.ai/c?lid=story-1",
        "https://app.alphasignal.ai/c?lid=story-2",
        "https://app.alphasignal.ai/c?lid=signal-1",
        "https://app.alphasignal.ai/c?lid=signal-2",
    ]
    assert "Section: Top News" in entries[0].body
    assert "> 23,539 Likes" in entries[0].body
    assert "Section: Signals" in entries[2].body
    assert "> 2,419 Likes" in entries[3].body


def test_alphasignal_gmail_source_sync_writes_newsletter_raw_documents(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "alphasignal-email")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    html = """
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
  <tr><td>Top News</td></tr>
  <tr><td><table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%"><tr><td style="border-top:1px solid #6B6B6B;"></td></tr></table></td></tr>
  <tr><td class="h1"><span>Meta debuts Muse Spark combining multimodal inputs, tool use, and agent orchestration in one system</span></td></tr>
  <tr><td>23,539 Likes</td></tr>
  <tr><td><div><p>Meta introduces Muse Spark, the first model from its rebuilt AI stack after nine months of changes across infrastructure, architecture, and data.</p></div></td></tr>
  <tr><td align="center"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation"><tr><td class="btn"><a href="https://app.alphasignal.ai/c?lid=story-1">TRY MUSE SPARK</a></td></tr></table></td></tr>
</table>
<table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
  <tr><td class="h1">Signals</td></tr>
  <tr><td><table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%"><tr><td style="border-top:1px solid #000000;"></td></tr></table></td></tr>
  <tr><td style="padding:15px 0;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%"><tr><td width="30" valign="top">1</td><td valign="top"><a class="h1" href="https://app.alphasignal.ai/c?lid=signal-1">Cursor enables running agents on any machine while controlling them remotely from your phone</a><span>4,301 Likes</span></td></tr></table></td></tr>
</table>
""".strip()

    captured: dict[str, object] = {}

    class FakeConnector:
        def list_newsletters(
            self,
            senders=None,
            labels=None,
            raw_query=None,
            max_results: int = 20,
            newer_than_days: int | None = 7,
        ) -> list[NewsletterMessage]:
            captured["senders"] = senders
            captured["labels"] = labels
            captured["raw_query"] = raw_query
            captured["max_results"] = max_results
            captured["newer_than_days"] = newer_than_days
            return [
                NewsletterMessage(
                    message_id="gmail-alphasignal-message-1",
                    thread_id="gmail-alphasignal-thread-1",
                    subject="⚡️ Meta Muse Spark: 10x efficiency with parallel agent inference",
                    sender="AlphaSignal <news@alphasignal.ai>",
                    published_at=datetime(2026, 4, 9, 16, 14, 36, tzinfo=UTC),
                    text_body="Top stories from the AlphaSignal briefing.",
                    html_body=html,
                    outbound_links=[
                        "https://app.alphasignal.ai/c?lid=story-1",
                        "https://app.alphasignal.ai/c?lid=signal-1",
                    ],
                    permalink="https://mail.google.com/mail/u/0/#inbox/gmail-alphasignal-message-1",
                )
            ]

    monkeypatch.setattr(service, "_build_gmail_connector", lambda: FakeConnector())

    result = service.sync_enabled_sources(trigger="test_sync")
    documents = service.store.list_raw_documents()

    assert result.source_count == 1
    assert result.synced_document_count == 3
    assert result.failed_source_count == 0
    assert captured == {
        "senders": ["news@alphasignal.ai"],
        "labels": None,
        "raw_query": None,
        "max_results": 20,
        "newer_than_days": 7,
    }
    assert len(documents) == 3

    parent = next(document for document in documents if document.frontmatter.doc_role == "primary")
    children = [document for document in documents if document.frontmatter.doc_role == "derived"]

    assert parent.frontmatter.kind == "newsletter"
    assert parent.frontmatter.source_name == "AlphaSignal Email"
    assert parent.frontmatter.index_visibility == "hidden"
    assert parent.frontmatter.asset_paths == ["original.html"]
    assert "## Top News" in parent.body
    assert "## Signals" in parent.body

    assert len(children) == 2
    assert {child.frontmatter.parent_id for child in children} == {parent.frontmatter.id}
    assert {child.frontmatter.canonical_url for child in children} == {
        "https://app.alphasignal.ai/c?lid=story-1",
        "https://app.alphasignal.ai/c?lid=signal-1",
    }
    assert all("sub-document" in child.frontmatter.tags for child in children)


def test_tldr_gmail_source_sync_writes_newsletter_raw_documents(client, monkeypatch) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "tldr-email")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    captured: dict[str, object] = {}

    class FakeConnector:
        def list_newsletters(
            self,
            senders=None,
            labels=None,
            raw_query=None,
            max_results: int = 20,
            newer_than_days: int | None = 7,
        ) -> list[NewsletterMessage]:
            captured["senders"] = senders
            captured["labels"] = labels
            captured["raw_query"] = raw_query
            captured["max_results"] = max_results
            captured["newer_than_days"] = newer_than_days
            return [
                NewsletterMessage(
                    message_id="gmail-message-1",
                    thread_id="gmail-thread-1",
                    subject="TLDR AI",
                    sender="TLDR AI <hi@tldrnewsletter.com>",
                    published_at=datetime(2026, 4, 7, 8, 30, tzinfo=UTC),
                    text_body="Top stories from the AI ecosystem.",
                    html_body="<p>Top stories from the AI ecosystem.</p>",
                    outbound_links=["https://example.com/story"],
                    permalink="https://mail.google.com/mail/u/0/#inbox/gmail-message-1",
                )
            ]

    monkeypatch.setattr(service, "_build_gmail_connector", lambda: FakeConnector())

    result = service.sync_enabled_sources(trigger="test_sync")
    documents = service.store.list_raw_documents()

    assert result.source_count == 1
    assert result.synced_document_count == 2
    assert result.failed_source_count == 0
    assert captured == {
        "senders": [
            "dan@tldrnewsletter.com",
            "hi@tldrnewsletter.com",
            "newsletter@tldrnewsletter.com",
        ],
        "labels": None,
        "raw_query": None,
        "max_results": 20,
        "newer_than_days": 7,
    }
    assert len(documents) == 2

    parent = next(document for document in documents if document.frontmatter.doc_role == "primary")
    child = next(document for document in documents if document.frontmatter.doc_role == "derived")
    parent_dir = (service.store.root / parent.path).parent

    assert parent.path.startswith("raw/newsletter/")
    assert parent.frontmatter.kind == "newsletter"
    assert parent.frontmatter.source_name == "TLDR Email"
    assert parent.frontmatter.index_visibility == "hidden"
    assert parent.frontmatter.asset_paths == ["original.html"]
    assert parent.frontmatter.id.startswith("2026-04-07-tldr-email-tldr-ai-")
    assert "# TLDR AI" in parent.body
    assert "## Email Body" in parent.body
    assert "## Extracted Entries" in parent.body
    assert (parent_dir / "original.html").read_text(encoding="utf-8") == "<p>Top stories from the AI ecosystem.</p>"

    assert child.path.startswith("raw/article/")
    assert child.frontmatter.kind == "article"
    assert child.frontmatter.parent_id == parent.frontmatter.id
    assert child.frontmatter.index_visibility == "visible"
    assert "sub-document" in child.frontmatter.tags
    assert child.frontmatter.id.startswith("2026-04-07-tldr-email-story-")
    assert child.frontmatter.canonical_url == "https://example.com/story"

    _install_summary_stub(monkeypatch)
    index = VaultIngestionService().rebuild_items_index(trigger="test_sync")

    assert len(index.items) == 2
    visible_items = [item for item in index.items if item.index_visibility != "hidden"]
    hidden_items = [item for item in index.items if item.index_visibility == "hidden"]
    assert len(visible_items) == 1
    assert len(hidden_items) == 1
    assert visible_items[0].kind == "article"
    assert visible_items[0].content_type == ContentType.ARTICLE
    assert visible_items[0].parent_id == parent.frontmatter.id
    assert "sub-document" in visible_items[0].tags
    assert hidden_items[0].kind == "newsletter"


def test_tldr_gmail_source_sync_without_published_at_uses_stable_undated_ids(
    client,
    monkeypatch,
) -> None:
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "tldr-email")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    class FakeConnector:
        def list_newsletters(
            self,
            senders=None,
            labels=None,
            raw_query=None,
            max_results: int = 20,
            newer_than_days: int | None = 7,
        ) -> list[NewsletterMessage]:
            return [
                NewsletterMessage(
                    message_id="gmail-message-undated",
                    thread_id="gmail-thread-undated",
                    subject="TLDR AI",
                    sender="TLDR AI <hi@tldrnewsletter.com>",
                    published_at=None,
                    text_body="Top stories from the AI ecosystem.",
                    html_body="<p>Top stories from the AI ecosystem. <a href=\"https://example.com/story\">Story</a></p>",
                    outbound_links=["https://example.com/story"],
                    permalink="https://mail.google.com/mail/u/0/#inbox/gmail-message-undated",
                )
            ]

    monkeypatch.setattr(service, "_build_gmail_connector", lambda: FakeConnector())

    first = service.sync_enabled_sources(trigger="test_sync")
    first_documents = service.store.list_raw_documents()
    first_parent = next(document for document in first_documents if document.frontmatter.doc_role == "primary")
    first_child = next(document for document in first_documents if document.frontmatter.doc_role == "derived")

    second = service.sync_enabled_sources(trigger="test_sync_again")
    second_documents = service.store.list_raw_documents()
    second_parent = next(document for document in second_documents if document.frontmatter.doc_role == "primary")
    second_child = next(document for document in second_documents if document.frontmatter.doc_role == "derived")

    assert first.synced_document_count == 2
    assert second.synced_document_count == 2
    assert len(second_documents) == 2
    assert first_parent.frontmatter.id.startswith("undated-tldr-email-tldr-ai-")
    assert first_child.frontmatter.id.startswith("undated-tldr-email-story-")
    assert "Published At:" not in first_parent.body
    assert "Published At:" not in first_child.body
    assert second_parent.frontmatter.id == first_parent.frontmatter.id
    assert second_child.frontmatter.id == first_child.frontmatter.id
    assert second_parent.frontmatter.content_hash == first_parent.frontmatter.content_hash
    assert second_child.frontmatter.content_hash == first_child.frontmatter.content_hash


def test_raw_document_identity_hash_stays_stable_when_revision_changes(client) -> None:
    service = VaultSourceIngestionService()
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "openai-website")

    service._upsert_raw_document(
        source=source,
        kind="blog-post",
        stable_key="https://example.com/post",
        external_key="https://example.com/post",
        title="OpenAI update",
        body="This post contains teh original typo.",
        source_url="https://example.com/post",
        source_name=source.name,
        authors=["OpenAI"],
        published_at=datetime(2026, 4, 7, 8, 0, tzinfo=UTC),
        tags=["openai"],
        asset_map={},
        doc_role="primary",
        parent_id=None,
        index_visibility="visible",
    )
    first_document = service.store.list_raw_documents()[0]

    service._upsert_raw_document(
        source=source,
        kind="blog-post",
        stable_key="https://example.com/post",
        external_key="https://example.com/post",
        title="OpenAI update",
        body="This post contains the corrected typo.",
        source_url="https://example.com/post",
        source_name=source.name,
        authors=["OpenAI"],
        published_at=datetime(2026, 4, 7, 8, 0, tzinfo=UTC),
        tags=["openai"],
        asset_map={},
        doc_role="primary",
        parent_id=None,
        index_visibility="visible",
    )

    refreshed_document = service.store.list_raw_documents()[0]
    index = VaultIngestionService().rebuild_items_index(trigger="test_sync")
    item = next(record for record in index.items if record.id == refreshed_document.frontmatter.id)

    assert first_document.frontmatter.identity_hash is not None
    assert refreshed_document.frontmatter.id == first_document.frontmatter.id
    assert refreshed_document.frontmatter.identity_hash == first_document.frontmatter.identity_hash
    assert refreshed_document.frontmatter.content_hash != first_document.frontmatter.content_hash
    assert item.identity_hash == refreshed_document.frontmatter.identity_hash


def test_tldr_gmail_source_sync_uses_stored_oauth_connection_when_no_ingest_env(
    client,
    monkeypatch,
) -> None:
    Base.metadata.create_all(bind=get_engine())
    service = VaultSourceIngestionService()
    service.settings.vault_source_pipelines_enabled = True
    source = next(source for source in DEFAULT_SOURCES.sources if source.id == "tldr-email")
    service.store.save_sources_config(VaultSourcesConfig(sources=[source.model_copy(deep=True)]))

    db = get_session_factory()()
    try:
        ConnectionService(db).store_connection(
            provider=ConnectionProvider.GMAIL,
            label="Primary Gmail",
            payload={
                "access_token": "stored-gmail-access-token",
                "refresh_token": "stored-gmail-refresh-token",
                "expires_at": datetime(2030, 4, 8, 8, 30, tzinfo=UTC).isoformat(),
            },
            metadata_json={"auth_mode": "oauth", "connected_email": "reader@example.com"},
            status=ConnectionStatus.CONNECTED,
        )
    finally:
        db.close()

    captured: dict[str, object] = {}

    def fake_list_newsletters(
        self,
        senders=None,
        labels=None,
        raw_query=None,
        max_results: int = 20,
        newer_than_days: int | None = 7,
    ) -> list[NewsletterMessage]:
        captured["access_token"] = self.access_token
        captured["senders"] = senders
        captured["labels"] = labels
        captured["raw_query"] = raw_query
        captured["max_results"] = max_results
        captured["newer_than_days"] = newer_than_days
        return [
            NewsletterMessage(
                message_id="gmail-message-2",
                thread_id="gmail-thread-2",
                subject="TLDR AI via Stored OAuth",
                sender="TLDR AI <hi@tldrnewsletter.com>",
                published_at=datetime(2026, 4, 7, 9, 0, tzinfo=UTC),
                text_body="A stored OAuth connection should power vault newsletter syncs.",
                html_body="<p>A stored OAuth connection should power vault newsletter syncs.</p>",
                outbound_links=["https://example.com/stored-oauth"],
                permalink="https://mail.google.com/mail/u/0/#inbox/gmail-message-2",
            )
        ]

    monkeypatch.setattr(
        "app.services.vault_sources.GmailConnector.list_newsletters",
        fake_list_newsletters,
    )

    result = service.sync_enabled_sources(trigger="test_sync")
    documents = service.store.list_raw_documents()

    assert result.source_count == 1
    assert result.synced_document_count == 2
    assert result.failed_source_count == 0
    assert captured == {
        "access_token": "stored-gmail-access-token",
        "senders": [
            "dan@tldrnewsletter.com",
            "hi@tldrnewsletter.com",
            "newsletter@tldrnewsletter.com",
        ],
        "labels": None,
        "raw_query": None,
        "max_results": 20,
        "newer_than_days": 7,
    }
    assert len(documents) == 2
    assert any(document.frontmatter.title == "TLDR AI via Stored OAuth" for document in documents)
    assert any(document.frontmatter.doc_role == "derived" for document in documents)


def test_paper_raw_documents_index_as_paper_content_type(client, monkeypatch) -> None:
    store = VaultStore()
    store.ensure_layout()
    frontmatter = RawDocumentFrontmatter(
        id="alphaxiv-paper-fixture",
        kind="paper",
        title="Verifier Routing for Research Agents",
        source_url="https://www.alphaxiv.org/abs/2504.01234",
        source_name="alphaXiv",
        authors=["Example Author"],
        published_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 4, 7, 12, 0, tzinfo=UTC),
        content_hash="",
        tags=["paper", "research", "agents"],
        status="active",
        asset_paths=[],
    )
    store.write_raw_document(
        kind="paper",
        doc_id=frontmatter.id,
        frontmatter=frontmatter,
        body="A paper about verifier routing, evaluation discipline, and agentic research workflows.",
    )

    _install_summary_stub(monkeypatch)
    index = VaultIngestionService().rebuild_items_index(trigger="test_sync")

    item = next(record for record in index.items if record.id == "alphaxiv-paper-fixture")
    assert item.kind == "paper"
    assert item.source_name == "alphaXiv"
    assert item.content_type == ContentType.PAPER
