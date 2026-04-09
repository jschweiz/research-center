from __future__ import annotations

import json
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from app.db.base import Base
from app.db.models import ConnectionProvider, ConnectionStatus, ContentType, RunStatus
from app.db.session import get_engine, get_session_factory
from app.integrations.extractors import ExtractedContent
from app.integrations.gmail import NewsletterMessage
from app.schemas.profile import AlphaXivSearchSettings
from app.services.connections import ConnectionService
from app.services.vault_ingestion import VaultIngestionService
from app.services.vault_sources import (
    DEFAULT_SOURCES,
    SourceFetchCancelledError,
    VaultSourceIngestionService,
    WebsiteEntry,
)
from app.vault.models import RawDocumentFrontmatter, VaultSourcesConfig
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
            "tldr-email",
            "medium-email",
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
    assert by_id["alphaxiv-paper"].type == "website"
    assert by_id["alphaxiv-paper"].raw_kind == "paper"
    assert by_id["alphaxiv-paper"].enabled is True
    assert all(source.created_at is not None for source in config.sources)
    assert all(source.updated_at is not None for source in config.sources)


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
        lambda current_source, *, max_entries=20, run=None: entries[:max_entries],
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
    assert len(documents) == 1

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
        lambda current_source, *, max_entries=20, run=None: [
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
