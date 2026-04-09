from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier, Lock
from time import sleep

from app.db.models import IngestionRunType, RunStatus
from app.integrations.llm import LLMClient
from app.services.vault_lightweight_enrichment import VaultLightweightEnrichmentService
from app.services.vault_runtime import RunRecorder, content_hash
from app.vault.models import RawDocumentFrontmatter
from app.vault.store import VaultStore


def _fake_score_payload(
    *, relevance_score: float = 0.84, model: str = "gemma4:e2b"
) -> dict[str, object]:
    return {
        "relevance_score": relevance_score,
        "source_fit_score": 0.74,
        "topic_fit_score": 0.88,
        "author_fit_score": 0.63,
        "evidence_fit_score": 0.79,
        "confidence_score": 0.77,
        "bucket_hint": "must_read" if relevance_score >= 0.76 else "worth_a_skim",
        "reason": "Strong fit for the current research profile.",
        "evidence_quotes": ["verifier routing", "faster research triage"],
        "model": model,
    }


def _seed_tldr_newsletter_document() -> RawDocumentFrontmatter:
    store = VaultStore()
    now = datetime(2026, 4, 7, 12, 0, tzinfo=UTC)
    title = "Anthropic's revenue spike 💸, Sam Altman excludes CFO 🧮"
    body = "\n".join(
        [
            f"# {title}",
            "",
            "## Big Tech & Startups",
            "",
            "### [Anthropic revenue spikes as enterprise demand grows](https://example.com/revenue)",
            "",
            "Anthropic is growing quickly as more teams adopt Claude across support and coding workflows.",
            "",
            "### [OpenAI leadership keeps finance decisions close](https://example.com/openai-finance)",
            "",
            "Sam Altman is reportedly excluding the CFO from part of OpenAI's fundraising process.",
            "",
            "## Programming, Design & Data Science",
            "",
            "### [Claude Code system prompts show how context is assembled](https://example.com/claude-code)",
            "",
            "A leaked prompt bundle shows how Claude Code layers system instructions and tool context.",
            "",
            "## Quick Links",
            "",
            "### [A smaller AI tooling note](https://example.com/quick-link)",
            "",
            "A one-line quick link.",
        ]
    )
    frontmatter = RawDocumentFrontmatter(
        id="2026-04-07-tldr-email-anthropic-revenue-spike-sam-altman-1234abcd",
        kind="newsletter",
        title=title,
        source_url="https://mail.google.com/mail/u/0/#inbox/test-message",
        source_name="TLDR Email",
        authors=["TLDR <dan@tldrnewsletter.com>", "TLDR"],
        published_at=now,
        ingested_at=now,
        content_hash=content_hash(title, body),
        tags=["newsletter", "tldr", "ai"],
        status="active",
        asset_paths=[],
        source_id="tldr-email",
        source_pipeline_id="tldr-email",
        external_key="test-message",
        canonical_url="https://mail.google.com/mail/u/0/#inbox/test-message",
        doc_role="primary",
        parent_id=None,
        index_visibility="visible",
        fetched_at=now,
        short_summary=None,
        lightweight_enrichment_status="pending",
        lightweight_enriched_at=None,
        lightweight_enrichment_model=None,
        lightweight_enrichment_input_hash=None,
        lightweight_enrichment_error=None,
    )
    store.write_raw_document(
        kind=frontmatter.kind, doc_id=frontmatter.id, frontmatter=frontmatter, body=body
    )
    return frontmatter


def _seed_article_document(
    *,
    doc_id: str = "2026-04-08-example-verifier-routing-article",
    title: str = "Verifier routing for faster triage",
) -> RawDocumentFrontmatter:
    store = VaultStore()
    now = datetime(2026, 4, 8, 7, 0, tzinfo=UTC)
    body = "\n".join(
        [
            f"# {title}",
            "",
            "A short article about verifier routing, faster research triage, and practical review workflows.",
        ]
    )
    frontmatter = RawDocumentFrontmatter(
        id=doc_id,
        kind="article",
        title=title,
        source_url=f"https://example.com/{doc_id}",
        source_name="Example Research",
        authors=[],
        published_at=now,
        ingested_at=now,
        content_hash=content_hash(title, body),
        tags=[],
        status="active",
        asset_paths=[],
        source_id="example-research",
        source_pipeline_id="example-research",
        external_key=doc_id,
        canonical_url=f"https://example.com/{doc_id}",
        doc_role="primary",
        parent_id=None,
        index_visibility="visible",
        fetched_at=now,
        short_summary=None,
        lightweight_enrichment_status="pending",
        lightweight_enriched_at=None,
        lightweight_enrichment_model=None,
        lightweight_enrichment_input_hash=None,
        lightweight_enrichment_error=None,
    )
    store.write_raw_document(
        kind=frontmatter.kind, doc_id=frontmatter.id, frontmatter=frontmatter, body=body
    )
    return frontmatter


def _seed_alphaxiv_paper_document(
    *,
    doc_id: str = "2026-04-01-alphaxiv-paper-embarrassingly-simple-self-distillation-improves-93a70f44",
    title: str = "Embarrassingly Simple Self-Distillation Improves Code Generation",
) -> RawDocumentFrontmatter:
    store = VaultStore()
    published_at = datetime(2026, 4, 1, 17, 39, 50, tzinfo=UTC)
    ingested_at = datetime(2026, 4, 9, 12, 8, 39, tzinfo=UTC)
    canonical_url = "https://www.alphaxiv.org/abs/2604.01193"
    body = "\n".join(
        [
            f"# {title}",
            "",
            "A code-generation paper about simple self-distillation, knowledge distillation, and post-training for language models.",
            "",
            "The paper studies how self-generated solutions can improve code generation without a verifier or teacher model.",
        ]
    )
    frontmatter = RawDocumentFrontmatter(
        id=doc_id,
        kind="paper",
        title=title,
        source_url=canonical_url,
        source_name="alphaXiv Papers",
        authors=[
            "Ruixiang Zhang",
            "Richard He Bai",
            "Huangjie Zheng",
            "Navdeep Jaitly",
            "Ronan Collobert",
            "Yizhe Zhang",
        ],
        published_at=published_at,
        ingested_at=ingested_at,
        content_hash=content_hash(title, body),
        tags=[
            "paper",
            "alphaxiv",
            "knowledge-distillation",
            "code generation",
            "language models",
        ],
        status="active",
        asset_paths=["alphaxiv-metadata.json"],
        source_id="alphaxiv-paper",
        source_pipeline_id="alphaxiv-paper",
        external_key=canonical_url,
        canonical_url=canonical_url,
        doc_role="primary",
        parent_id=None,
        index_visibility="visible",
        fetched_at=ingested_at,
        short_summary=None,
        lightweight_enrichment_status="pending",
        lightweight_enriched_at=None,
        lightweight_enrichment_model=None,
        lightweight_enrichment_input_hash=None,
        lightweight_enrichment_error=None,
    )
    path = store.write_raw_document(
        kind=frontmatter.kind,
        doc_id=frontmatter.id,
        frontmatter=frontmatter,
        body=body,
    )
    metadata = {
        "paper_id": "2604.01193",
        "title": title,
        "short_summary": (
            "Simple Self-Distillation (SSD) improves code generation by training large language models "
            "on self-generated solutions without a verifier."
        ),
        "citations_count": 0,
        "metrics": {
            "public_total_votes": 207,
            "total_votes": 75,
            "visits_count": {
                "all": 3772,
                "last_7_days": 3764,
            },
            "x_likes": 0,
        },
    }
    (path.parent / "alphaxiv-metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n",
        encoding="utf-8",
    )
    return frontmatter


def _seed_medium_newsletter_document() -> RawDocumentFrontmatter:
    store = VaultStore()
    now = datetime(2026, 4, 8, 8, 0, tzinfo=UTC)
    title = "The 5-Minute Mental Reset That Actually Works | Sam Liberty in Age of Awareness"
    body = "\n".join(
        [
            f"# {title}",
            "",
            "## Today's highlights",
            "",
            "### [The 5-Minute Mental Reset That Actually Works](https://medium.com/@sa-liberty/the-5-minute-mental-reset-that-actually-works-928116f8b78e)",
            "",
            "> Sam Liberty in Age of Awareness · 10 min read · 6.1K claps · 336 responses",
            "",
            "An evidence-based routine you can try right now",
            "",
            "### [Why Your RAG System Fails Complex Questions? (And How Structure Fixes Everything)](https://medium.com/@vivedhaelango/why-your-rag-system-fails-complex-questions-and-how-structure-fixes-everything-4adfc7e810d0)",
            "",
            "> Vivedha Elango in Level Up Coding · 17 min read · 747 claps · 12 responses",
            "",
            "Understanding the Retrieval and Structuring (RAS)…",
            "",
            "### [Data Engineering: Incremental Data Loading Strategies](https://medium.com/@husseinjundi/data-engineering-incremental-data-loading-strategies-bd1a79d98ed0)",
            "",
            "> Hussein Jundi in Data Engineer Things · 9 min read · 227 claps · 7 responses",
            "",
            "Outlining strategies and solution architectures to…",
        ]
    )
    frontmatter = RawDocumentFrontmatter(
        id="2026-04-07-medium-email-the-5-minute-mental-reset-that-actually-works-sa-e457c87f",
        kind="newsletter",
        title=title,
        source_url="https://mail.google.com/mail/u/0/#inbox/19d66ab627cf3725",
        source_name="Medium Email",
        authors=["Medium Daily Digest <noreply@medium.com>", "Sam Liberty"],
        published_at=now,
        ingested_at=now,
        content_hash=content_hash(title, body),
        tags=["newsletter", "medium", "email"],
        status="active",
        asset_paths=["original.html"],
        source_id="medium-email",
        source_pipeline_id="medium-email",
        external_key="19d66ab627cf3725",
        canonical_url="https://mail.google.com/mail/u/0/#inbox/19d66ab627cf3725",
        doc_role="primary",
        parent_id=None,
        index_visibility="visible",
        fetched_at=now,
        short_summary=None,
        lightweight_enrichment_status="pending",
        lightweight_enriched_at=None,
        lightweight_enrichment_model=None,
        lightweight_enrichment_input_hash=None,
        lightweight_enrichment_error=None,
    )
    store.write_raw_document(
        kind=frontmatter.kind, doc_id=frontmatter.id, frontmatter=frontmatter, body=body
    )
    return frontmatter


def test_tldr_newsletter_lightweight_summary_is_deterministic_and_plain_text(
    client,
    monkeypatch,
) -> None:
    del client
    frontmatter = _seed_tldr_newsletter_document()
    service = VaultLightweightEnrichmentService()

    assert service.count_stale_documents() == 1
    assert service.count_stale_documents(doc_id=frontmatter.id) == 1

    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": False,
            "model": "gemma4:e2b",
            "detail": "Ollama is unavailable for this test.",
        },
    )

    def _should_not_run(self, item, text):
        del self, item, text
        raise AssertionError(
            "TLDR parent newsletters should not use the LLM lightweight enrichment path."
        )

    monkeypatch.setattr(LLMClient, "lightweight_enrich_raw_document", _should_not_run)

    run = service.enrich_stale_documents(doc_id=frontmatter.id, force=True)
    updated = VaultStore().read_raw_document_relative(f"raw/newsletter/{frontmatter.id}/source.md")

    assert run.status == "succeeded"
    assert updated is not None
    assert updated.frontmatter.short_summary == (
        "TLDR roundup on Anthropic's revenue spike, Sam Altman excludes CFO. "
        "Includes 3 editorial stories across 2 sections, plus 1 quick link."
    )
    assert "#" not in updated.frontmatter.short_summary
    assert "[" not in updated.frontmatter.short_summary
    assert "](" not in updated.frontmatter.short_summary
    assert updated.frontmatter.authors == frontmatter.authors
    assert updated.frontmatter.tags == frontmatter.tags
    assert updated.frontmatter.lightweight_enrichment_status == "succeeded"
    assert updated.frontmatter.lightweight_enrichment_model == "deterministic:tldr-newsletter"
    assert service.count_stale_documents() == 0
    assert service.count_stale_documents(doc_id=frontmatter.id) == 0


def test_medium_newsletter_lightweight_summary_is_deterministic_and_plain_text(
    client,
    monkeypatch,
) -> None:
    del client
    frontmatter = _seed_medium_newsletter_document()
    service = VaultLightweightEnrichmentService()

    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": False,
            "model": "gemma4:e2b",
            "detail": "Ollama is unavailable for this test.",
        },
    )

    def _should_not_run(self, item, text):
        del self, item, text
        raise AssertionError(
            "Medium parent newsletters should not use the LLM lightweight enrichment path."
        )

    monkeypatch.setattr(LLMClient, "lightweight_enrich_raw_document", _should_not_run)

    run = service.enrich_stale_documents(doc_id=frontmatter.id, force=True)
    updated = VaultStore().read_raw_document_relative(f"raw/newsletter/{frontmatter.id}/source.md")

    assert run.status == "succeeded"
    assert updated is not None
    assert updated.frontmatter.short_summary == (
        'Medium digest led by "The 5-Minute Mental Reset That Actually Works". '
        'Includes 3 highlighted stories, including "Why Your RAG System Fails Complex Questions? (And How Structure Fixes Everything)".'
    )
    assert "#" not in updated.frontmatter.short_summary
    assert "[" not in updated.frontmatter.short_summary
    assert "](" not in updated.frontmatter.short_summary
    assert updated.frontmatter.authors == frontmatter.authors
    assert updated.frontmatter.tags == frontmatter.tags
    assert updated.frontmatter.lightweight_enrichment_status == "succeeded"
    assert updated.frontmatter.lightweight_enrichment_model == "deterministic:medium-newsletter"


def test_lightweight_enrichment_records_ai_trace_metadata(
    client,
    monkeypatch,
) -> None:
    del client
    frontmatter = _seed_article_document()
    store = VaultStore()
    prompt_path = store.local_state_root / "ai-traces" / "test-trace" / "prompt.md"
    trace_path = store.local_state_root / "ai-traces" / "test-trace" / "trace.json"
    store.write_text(prompt_path, "# Prompt\n")
    store.write_json(trace_path, {"trace_id": "trace-lightweight"})

    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )

    def _fake_enrich(self, item, text):
        del self, item, text
        return {
            "short_summary": "Verifier routing short summary.",
            "authors": ["Casey Researcher"],
            "tags": ["verifier routing", "triage"],
            "model": "gemma4:e2b",
            "_trace": {
                "trace_id": "trace-lightweight",
                "provider": "ollama",
                "model": "gemma4:e2b",
                "operation": "lightweight_enrich_raw_document",
                "status": "succeeded",
                "recorded_at": "2026-04-08T07:10:00Z",
                "duration_ms": 980,
                "prompt_sha256": "tracehash",
                "prompt_path": str(prompt_path),
                "trace_path": str(trace_path),
                "prompt_tokens": 42,
                "completion_tokens": 18,
                "total_tokens": 60,
                "cost_usd": 0.0,
                "context": {"doc_id": frontmatter.id, "source_id": frontmatter.source_id},
            },
        }

    monkeypatch.setattr(LLMClient, "lightweight_enrich_raw_document", _fake_enrich)
    monkeypatch.setattr(
        LLMClient,
        "judge_lightweight_document",
        lambda self, item, text, *, profile, source_context=None: _fake_score_payload(),
    )

    run = VaultLightweightEnrichmentService().enrich_stale_documents(
        doc_id=frontmatter.id, force=True
    )

    assert run.status == "succeeded"
    assert run.ai_total_tokens == 60
    assert run.ai_prompt_tokens == 42
    assert run.ai_completion_tokens == 18
    assert run.prompt_path == str(prompt_path)
    assert run.manifest_path is not None
    assert Path(run.manifest_path).exists()


def test_lightweight_enrichment_persists_profile_aware_score(
    client,
    monkeypatch,
) -> None:
    del client
    frontmatter = _seed_article_document()
    service = VaultLightweightEnrichmentService()

    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.load_profile_snapshot",
        lambda: type(
            "ProfileSnapshot",
            (),
            {
                "favorite_topics": ["verifier routing", "research triage"],
                "favorite_authors": ["Casey Researcher"],
                "favorite_sources": ["Example Research"],
                "ignored_topics": ["consumer gadget news"],
                "prompt_guidance": type(
                    "PromptGuidance", (), {"enrichment": "Prefer workflow leverage."}
                )(),
            },
        )(),
    )
    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "lightweight_enrich_raw_document",
        lambda self, item, text: {
            "short_summary": "A workflow note about verifier routing and triage speed.",
            "authors": ["Casey Researcher"],
            "tags": ["verifier routing", "triage"],
            "model": "gemma4:e2b",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "judge_lightweight_document",
        lambda self, item, text, *, profile, source_context=None: _fake_score_payload(
            relevance_score=0.91
        ),
    )

    run = service.enrich_stale_documents(doc_id=frontmatter.id, force=True)
    updated = VaultStore().read_raw_document_relative(f"raw/article/{frontmatter.id}/source.md")

    assert run.status == "succeeded"
    assert updated is not None
    assert updated.frontmatter.lightweight_score is not None
    assert updated.frontmatter.lightweight_score.relevance_score == 0.91
    assert updated.frontmatter.lightweight_score.topic_fit_score == 0.88
    assert (
        updated.frontmatter.lightweight_score.reason
        == "Strong fit for the current research profile."
    )
    assert updated.frontmatter.lightweight_scoring_model == "gemma4:e2b"
    assert updated.frontmatter.lightweight_scoring_input_hash is not None


def test_alphaxiv_heuristic_scores_use_reported_engagement_metrics(
    client,
    monkeypatch,
) -> None:
    del client
    frontmatter = _seed_alphaxiv_paper_document()
    service = VaultLightweightEnrichmentService()

    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.load_profile_snapshot",
        lambda: type(
            "ProfileSnapshot",
            (),
            {
                "favorite_topics": ["code generation", "knowledge-distillation"],
                "favorite_authors": [],
                "favorite_sources": [],
                "ignored_topics": [],
                "prompt_guidance": type("PromptGuidance", (), {"enrichment": ""})(),
            },
        )(),
    )
    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )

    def _should_not_enrich_metadata(self, item, text):
        del self, item, text
        raise AssertionError("alphaXiv papers should reuse deterministic sidecar metadata.")

    monkeypatch.setattr(
        LLMClient,
        "lightweight_enrich_raw_document",
        _should_not_enrich_metadata,
    )

    def _timeout_score(self, item, text, *, profile, source_context=None):
        del self, item, text, profile, source_context
        raise RuntimeError("Ollama lightweight scoring failed: timed out")

    monkeypatch.setattr(LLMClient, "judge_lightweight_document", _timeout_score)

    run = service.enrich_stale_documents(doc_id=frontmatter.id, force=True)
    updated = VaultStore().read_raw_document_relative(f"raw/paper/{frontmatter.id}/source.md")

    assert run.status == "succeeded"
    assert updated is not None
    assert updated.frontmatter.lightweight_score is not None
    assert updated.frontmatter.lightweight_score.relevance_score > 0.7
    assert updated.frontmatter.lightweight_score.source_fit_score >= 0.75
    assert "alphaXiv engagement signals" in (updated.frontmatter.lightweight_score.reason or "")
    assert "207 public votes" in (updated.frontmatter.lightweight_score.reason or "")
    assert updated.frontmatter.lightweight_scoring_model == "heuristic:profile-fallback+alphaxiv-metrics-v1"


def test_alphaxiv_model_scores_are_boosted_by_reported_engagement_metrics(
    client,
    monkeypatch,
) -> None:
    del client
    frontmatter = _seed_alphaxiv_paper_document(
        doc_id="2026-04-02-alphaxiv-paper-engagement-boosted-model-score",
        title="Self-Distillation Signals for Code Models",
    )
    service = VaultLightweightEnrichmentService()

    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.load_profile_snapshot",
        lambda: type(
            "ProfileSnapshot",
            (),
            {
                "favorite_topics": ["code generation", "knowledge-distillation"],
                "favorite_authors": [],
                "favorite_sources": [],
                "ignored_topics": [],
                "prompt_guidance": type("PromptGuidance", (), {"enrichment": ""})(),
            },
        )(),
    )
    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )

    def _should_not_enrich_metadata(self, item, text):
        del self, item, text
        raise AssertionError("alphaXiv papers should reuse deterministic sidecar metadata.")

    monkeypatch.setattr(
        LLMClient,
        "lightweight_enrich_raw_document",
        _should_not_enrich_metadata,
    )
    monkeypatch.setattr(
        LLMClient,
        "judge_lightweight_document",
        lambda self, item, text, *, profile, source_context=None: {
            "relevance_score": 0.24,
            "source_fit_score": 0.2,
            "topic_fit_score": 0.88,
            "author_fit_score": 0.1,
            "evidence_fit_score": 0.45,
            "confidence_score": 0.28,
            "bucket_hint": "archive",
            "reason": "Weak initial profile-fit estimate.",
            "evidence_quotes": ["self-distillation", "code generation"],
            "model": "gemma4:e2b",
        },
    )

    run = service.enrich_stale_documents(doc_id=frontmatter.id, force=True)
    updated = VaultStore().read_raw_document_relative(f"raw/paper/{frontmatter.id}/source.md")

    assert run.status == "succeeded"
    assert updated is not None
    assert updated.frontmatter.lightweight_score is not None
    assert updated.frontmatter.lightweight_score.relevance_score > 0.35
    assert updated.frontmatter.lightweight_score.source_fit_score >= 0.75
    assert updated.frontmatter.lightweight_score.bucket_hint == "worth_a_skim"
    assert "3764 visits in the last 7 days" in (updated.frontmatter.lightweight_score.reason or "")
    assert updated.frontmatter.lightweight_scoring_model == "gemma4:e2b+alphaxiv-metrics-v1"


def test_profile_change_marks_document_stale_for_rescoring(
    client,
    monkeypatch,
) -> None:
    del client
    frontmatter = _seed_article_document()
    service = VaultLightweightEnrichmentService()

    profile_a = type(
        "ProfileSnapshot",
        (),
        {
            "favorite_topics": ["verifier routing"],
            "favorite_authors": [],
            "favorite_sources": ["Example Research"],
            "ignored_topics": [],
            "prompt_guidance": type("PromptGuidance", (), {"enrichment": ""})(),
        },
    )()
    profile_b = type(
        "ProfileSnapshot",
        (),
        {
            "favorite_topics": ["multimodal generation"],
            "favorite_authors": [],
            "favorite_sources": ["Different Source"],
            "ignored_topics": [],
            "prompt_guidance": type("PromptGuidance", (), {"enrichment": ""})(),
        },
    )()

    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "lightweight_enrich_raw_document",
        lambda self, item, text: {
            "short_summary": "A workflow note about verifier routing and triage speed.",
            "authors": [],
            "tags": ["verifier routing", "triage"],
            "model": "gemma4:e2b",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "judge_lightweight_document",
        lambda self, item, text, *, profile, source_context=None: _fake_score_payload(
            relevance_score=0.82
        ),
    )
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.load_profile_snapshot", lambda: profile_a
    )

    first_run = service.enrich_stale_documents(doc_id=frontmatter.id, force=True)
    assert first_run.status == "succeeded"
    assert service.count_stale_documents(doc_id=frontmatter.id) == 0

    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.load_profile_snapshot", lambda: profile_b
    )

    assert service.count_stale_documents(doc_id=frontmatter.id) == 1


def test_scoring_pipeline_signature_change_marks_document_stale(
    client,
    monkeypatch,
) -> None:
    del client
    frontmatter = _seed_article_document()
    service = VaultLightweightEnrichmentService()

    profile = type(
        "ProfileSnapshot",
        (),
        {
            "favorite_topics": ["verifier routing"],
            "favorite_authors": [],
            "favorite_sources": ["Example Research"],
            "ignored_topics": [],
            "prompt_guidance": type("PromptGuidance", (), {"enrichment": ""})(),
        },
    )()

    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.load_profile_snapshot",
        lambda: profile,
    )
    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "lightweight_enrich_raw_document",
        lambda self, item, text: {
            "short_summary": "A workflow note about verifier routing and triage speed.",
            "authors": ["Casey Researcher"],
            "tags": ["verifier routing", "triage"],
            "model": "gemma4:e2b",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "judge_lightweight_document",
        lambda self, item, text, *, profile, source_context=None: _fake_score_payload(
            relevance_score=0.87
        ),
    )
    monkeypatch.setattr(
        service,
        "_scoring_pipeline_signature",
        lambda: "ollama:gemma4:e2b:lightweight_scoring:test-v1",
    )

    first_run = service.enrich_stale_documents(doc_id=frontmatter.id, force=True)

    assert first_run.status == "succeeded"
    assert service.count_stale_documents(doc_id=frontmatter.id) == 0

    monkeypatch.setattr(
        service,
        "_scoring_pipeline_signature",
        lambda: "ollama:gemma4:e2b:lightweight_scoring:test-v2",
    )

    assert service.count_stale_documents(doc_id=frontmatter.id) == 1


def test_lightweight_enrichment_limits_parallel_single_doc_queries_to_four(
    client,
    monkeypatch,
) -> None:
    del client
    for index in range(5):
        _seed_article_document(
            doc_id=f"2026-04-08-example-verifier-routing-article-{index}",
            title=f"Verifier routing note {index}",
        )

    service = VaultLightweightEnrichmentService()
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.load_profile_snapshot",
        lambda: type(
            "ProfileSnapshot",
            (),
            {
                "favorite_topics": ["verifier routing"],
                "favorite_authors": [],
                "favorite_sources": ["Example Research"],
                "ignored_topics": [],
                "prompt_guidance": type("PromptGuidance", (), {"enrichment": ""})(),
            },
        )(),
    )
    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )

    def _make_probe(*, response_factory):
        barrier = Barrier(4)
        lock = Lock()
        current = 0
        max_seen = 0
        started = 0

        def _wrapper(*args, **kwargs):
            nonlocal current, max_seen, started
            del args, kwargs
            with lock:
                started += 1
                start_index = started
                current += 1
                max_seen = max(max_seen, current)
            try:
                if start_index <= 4:
                    barrier.wait(timeout=1)
                    sleep(0.05)
                return response_factory(start_index)
            finally:
                with lock:
                    current -= 1

        return _wrapper, lambda: max_seen

    metadata_call, metadata_max_seen = _make_probe(
        response_factory=lambda start_index: {
            "short_summary": f"Summary {start_index}",
            "authors": ["Casey Researcher"],
            "tags": ["verifier routing", "triage"],
            "model": "gemma4:e2b",
        }
    )
    scoring_call, scoring_max_seen = _make_probe(
        response_factory=lambda start_index: _fake_score_payload(
            relevance_score=0.8 + (start_index * 0.001),
        )
    )

    monkeypatch.setattr(LLMClient, "lightweight_enrich_raw_document", metadata_call)
    monkeypatch.setattr(LLMClient, "judge_lightweight_document", scoring_call)

    run = service.enrich_stale_documents(force=True)

    assert run.status == "succeeded"
    assert metadata_max_seen() == 4
    assert scoring_max_seen() == 4
    assert any(info.label == "Parallelism" and info.value == "4" for info in run.basic_info)


def test_lightweight_enrichment_publishes_live_run_counts_while_running(
    client,
    monkeypatch,
) -> None:
    del client
    for index in range(2):
        _seed_article_document(
            doc_id=f"2026-04-08-example-live-progress-article-{index}",
            title=f"Live progress note {index}",
        )

    service = VaultLightweightEnrichmentService()
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.load_profile_snapshot",
        lambda: type(
            "ProfileSnapshot",
            (),
            {
                "favorite_topics": ["verifier routing"],
                "favorite_authors": [],
                "favorite_sources": ["Example Research"],
                "ignored_topics": [],
                "prompt_guidance": type("PromptGuidance", (), {"enrichment": ""})(),
            },
        )(),
    )
    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "lightweight_enrich_raw_document",
        lambda self, item, text: {
            "short_summary": "Short summary",
            "authors": ["Casey Researcher"],
            "tags": ["verifier routing", "triage"],
            "model": "gemma4:e2b",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "judge_lightweight_document",
        lambda self, item, text, *, profile, source_context=None: _fake_score_payload(
            relevance_score=0.87
        ),
    )

    original_log_step = RunRecorder.log_step
    progress_snapshots: list[tuple[int, int, int, int, str]] = []

    def _recording_log_step(self, run, step, message, *, level="info"):
        progress_snapshots.append(
            (
                run.total_titles,
                run.updated_count,
                step.updated_count,
                step.skipped_count,
                message,
            )
        )
        return original_log_step(self, run, step, message, level=level)

    monkeypatch.setattr(RunRecorder, "log_step", _recording_log_step)

    run = service.enrich_stale_documents(force=True)

    success_snapshots = [
        snapshot for snapshot in progress_snapshots if snapshot[4].startswith("Enriched ")
    ]

    assert run.status == "succeeded"
    assert run.total_titles == 2
    assert run.updated_count == 2
    assert [snapshot[0] for snapshot in success_snapshots] == [2, 2]
    assert [snapshot[1] for snapshot in success_snapshots] == [1, 2]
    assert [snapshot[2] for snapshot in success_snapshots] == [1, 2]
    assert [snapshot[3] for snapshot in success_snapshots] == [0, 0]


def test_lightweight_enrichment_emits_detailed_run_logs(
    client,
    monkeypatch,
) -> None:
    del client
    for index in range(2):
        _seed_article_document(
            doc_id=f"2026-04-08-example-detailed-log-article-{index}",
            title=f"Detailed run log note {index}",
        )

    service = VaultLightweightEnrichmentService()
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.load_profile_snapshot",
        lambda: type(
            "ProfileSnapshot",
            (),
            {
                "favorite_topics": ["verifier routing"],
                "favorite_authors": [],
                "favorite_sources": ["Example Research"],
                "ignored_topics": [],
                "prompt_guidance": type("PromptGuidance", (), {"enrichment": ""})(),
            },
        )(),
    )
    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "lightweight_enrich_raw_document",
        lambda self, item, text: {
            "short_summary": "Short summary",
            "authors": ["Casey Researcher"],
            "tags": ["verifier routing", "triage"],
            "model": "gemma4:e2b",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "judge_lightweight_document",
        lambda self, item, text, *, profile, source_context=None: _fake_score_payload(
            relevance_score=0.87
        ),
    )

    run = service.enrich_stale_documents(force=True)
    log_messages = [entry.message for entry in run.logs]

    assert run.status == "succeeded"
    assert any(message.startswith("Starting lightweight enrichment") for message in log_messages)
    assert "Loaded 2 raw documents for evaluation." in log_messages
    assert any("Metadata phase starting for 2 documents" in message for message in log_messages)
    assert any("Metadata phase progress 1/2" in message for message in log_messages)
    assert any("Scoring phase starting for 2 documents" in message for message in log_messages)
    assert any("Scoring phase progress 1/2" in message for message in log_messages)
    assert any("Enrichment progress 1/2" in message for message in log_messages)
    assert any(
        "Finished lightweight enrichment processing: updated 2, failed 0, skipped 0." in message
        for message in log_messages
    )


def test_lightweight_enrichment_logs_timeout_with_phase_and_document(
    client,
    monkeypatch,
) -> None:
    del client
    frontmatter = _seed_article_document(
        doc_id="2026-04-08-example-timeout-log-article",
        title="Timeout log note",
    )

    service = VaultLightweightEnrichmentService()
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.load_profile_snapshot",
        lambda: type(
            "ProfileSnapshot",
            (),
            {
                "favorite_topics": ["verifier routing"],
                "favorite_authors": [],
                "favorite_sources": ["Example Research"],
                "ignored_topics": [],
                "prompt_guidance": type("PromptGuidance", (), {"enrichment": ""})(),
            },
        )(),
    )
    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )

    def _timeout_metadata(self, item, text):
        del self, item, text
        raise RuntimeError("Ollama lightweight enrichment failed: timed out")

    monkeypatch.setattr(LLMClient, "lightweight_enrich_raw_document", _timeout_metadata)

    run = service.enrich_stale_documents(doc_id=frontmatter.id, force=True)
    run_log_messages = [entry.message for entry in run.logs]
    step_log_messages = [entry.message for entry in run.steps[0].logs]

    assert run.status == "failed"
    assert any("Metadata phase starting for 1 document" in message for message in run_log_messages)
    assert any(
        "Metadata phase completed for 1 document: 0 succeeded, 1 failed." in message
        for message in run_log_messages
    )
    assert any(
        "Metadata phase 1/1 failed for" in message and "timed out" in message
        for message in step_log_messages
    )


def test_lightweight_enrichment_honors_stop_requests_from_local_control(
    client,
    monkeypatch,
) -> None:
    del client
    for index in range(5):
        _seed_article_document(
            doc_id=f"2026-04-09-stop-lightweight-article-{index}",
            title=f"Stop lightweight note {index}",
        )

    service = VaultLightweightEnrichmentService()
    monkeypatch.setattr(
        "app.services.vault_lightweight_enrichment.load_profile_snapshot",
        lambda: type(
            "ProfileSnapshot",
            (),
            {
                "favorite_topics": ["verifier routing"],
                "favorite_authors": [],
                "favorite_sources": ["Example Research"],
                "ignored_topics": [],
                "prompt_guidance": type("PromptGuidance", (), {"enrichment": ""})(),
            },
        )(),
    )
    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )

    stop_lock = Lock()
    stop_requested = {"value": False}

    def _canceling_enrich(self, item, text):
        del self, item, text
        with stop_lock:
            if not stop_requested["value"]:
                VaultLightweightEnrichmentService().request_stop_for_run(
                    trigger="manual_lightweight_enrich"
                )
                stop_requested["value"] = True
        sleep(0.05)
        return {
            "short_summary": "Summary before stop.",
            "authors": ["Casey Researcher"],
            "tags": ["verifier routing", "triage"],
            "model": "gemma4:e2b",
        }

    def _should_not_score(self, item, text, *, profile, source_context=None):
        del self, item, text, profile, source_context
        raise AssertionError("Scoring should not run after a lightweight stop request.")

    monkeypatch.setattr(LLMClient, "lightweight_enrich_raw_document", _canceling_enrich)
    monkeypatch.setattr(LLMClient, "judge_lightweight_document", _should_not_score)

    run = service.enrich_stale_documents(force=True)
    documents = [
        document
        for document in VaultStore().list_raw_documents()
        if document.frontmatter.id.startswith("2026-04-09-stop-lightweight-article-")
    ]

    assert stop_requested["value"] is True
    assert run.status == "failed"
    assert "canceled" in run.summary.lower()
    assert "canceled" in " ".join(run.errors).lower()
    assert any(
        info.label == "Canceled" and info.value == "local-control" for info in run.basic_info
    )
    assert all(
        document.frontmatter.lightweight_enrichment_status == "pending" for document in documents
    )


def test_request_stop_for_run_targets_live_lightweight_run_even_after_newer_failed_attempt(
    client,
) -> None:
    del client
    store = VaultStore()
    recorder = RunRecorder(store)

    live_run = recorder.start(
        run_type=IngestionRunType.INGEST,
        operation_kind="lightweight_enrichment",
        trigger="manual_source_enrich:openai-website",
        title="Lightweight enrichment",
        summary="Lightweight enrichment is running for a source pipeline.",
    )
    newer_failed_run = recorder.start(
        run_type=IngestionRunType.INGEST,
        operation_kind="lightweight_enrichment",
        trigger="manual_lightweight_enrich",
        title="Lightweight enrichment",
        summary="Another attempt was made from the lightweight card.",
    )
    recorder.finish(
        newer_failed_run,
        status=RunStatus.FAILED,
        summary="Lightweight enrichment skipped because another enrichment run is already active.",
    )

    requested = VaultLightweightEnrichmentService().request_stop_for_run()

    assert requested.id == live_run.id
    assert store.is_operation_stop_requested(live_run.id) is True
    assert store.is_operation_stop_requested(newer_failed_run.id) is False


def test_lightweight_enrichment_recovers_from_stale_interrupted_lease(
    client,
    monkeypatch,
) -> None:
    del client
    frontmatter = _seed_article_document(
        doc_id="2026-04-09-stale-lease-lightweight-article",
        title="Stale lease recovery note",
    )
    store = VaultStore()
    stale_handle = store.acquire_lease(
        name="lightweight-enrichment",
        owner="mac",
        ttl_seconds=900,
    )

    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "lightweight_enrich_raw_document",
        lambda self, item, text: {
            "short_summary": "Recovered from a stale lease.",
            "authors": ["Casey Researcher"],
            "tags": ["verifier routing", "triage"],
            "model": "gemma4:e2b",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "judge_lightweight_document",
        lambda self, item, text, *, profile, source_context=None: _fake_score_payload(
            relevance_score=0.88
        ),
    )

    run = VaultLightweightEnrichmentService().enrich_stale_documents(
        doc_id=frontmatter.id, force=True
    )
    updated = VaultStore().read_raw_document_relative(f"raw/article/{frontmatter.id}/source.md")

    assert run.status == "succeeded"
    assert any("stale lightweight-enrichment lease" in log.message for log in run.logs)
    assert updated is not None
    assert updated.frontmatter.lightweight_enrichment_status == "succeeded"
    assert store.is_operation_stop_requested(run.id) is False
    store.release_lease(stale_handle)


def test_lightweight_enrichment_interrupts_orphaned_live_runs_when_a_new_run_takes_over(
    client,
    monkeypatch,
) -> None:
    del client
    stale_frontmatter = _seed_article_document(
        doc_id="2026-04-09-orphaned-lightweight-article",
        title="Orphaned lightweight note",
    )
    fresh_frontmatter = _seed_article_document(
        doc_id="2026-04-09-fresh-lightweight-article",
        title="Fresh lightweight note",
    )
    store = VaultStore()
    recorder = RunRecorder(store)
    stale_run = recorder.start(
        run_type=IngestionRunType.INGEST,
        operation_kind="lightweight_enrichment",
        trigger="manual_lightweight_enrich",
        title="Lightweight enrichment",
        summary="An older lightweight run is still marked live.",
    )
    recorder.start_step(
        stale_run,
        step_kind="lightweight_enrichment",
        doc_id=stale_frontmatter.id,
    )

    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "lightweight_enrich_raw_document",
        lambda self, item, text: {
            "short_summary": "A fresh summary.",
            "authors": ["Casey Researcher"],
            "tags": ["verifier routing", "triage"],
            "model": "gemma4:e2b",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "judge_lightweight_document",
        lambda self, item, text, *, profile, source_context=None: _fake_score_payload(
            relevance_score=0.91
        ),
    )

    service = VaultLightweightEnrichmentService()
    run = service.enrich_stale_documents(doc_id=fresh_frontmatter.id, force=True)
    interrupted = service._load_run_record(stale_run.id)

    assert run.status == "succeeded"
    assert interrupted is not None
    assert interrupted.status == "interrupted"
    assert interrupted.finished_at is not None
    assert interrupted.steps[0].status == "interrupted"
    assert any(
        info.label == "Interrupted by" and info.value == run.id for info in interrupted.basic_info
    )
    assert any("lease expired" in log.message for log in interrupted.logs)
    assert any("Interrupted 1 stale lightweight run" in log.message for log in run.logs)


def test_lightweight_enrichment_renews_its_lease_while_work_is_running(
    client,
    monkeypatch,
) -> None:
    del client
    frontmatter = _seed_article_document(
        doc_id="2026-04-09-renew-lightweight-article",
        title="Lease renewal note",
    )

    monkeypatch.setattr(
        LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": "gemma4:e2b",
            "detail": "ready",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "lightweight_enrich_raw_document",
        lambda self, item, text: {
            "short_summary": "Lease renewal summary.",
            "authors": ["Casey Researcher"],
            "tags": ["lease", "triage"],
            "model": "gemma4:e2b",
        },
    )
    monkeypatch.setattr(
        LLMClient,
        "judge_lightweight_document",
        lambda self, item, text, *, profile, source_context=None: _fake_score_payload(
            relevance_score=0.82
        ),
    )

    service = VaultLightweightEnrichmentService()
    renew_calls: list[tuple[str, int]] = []
    original_renew = service.store.renew_lease

    def _track_renew(handle, *, ttl_seconds: int = 600) -> None:
        renew_calls.append((handle.name, ttl_seconds))
        original_renew(handle, ttl_seconds=ttl_seconds)

    monkeypatch.setattr(service.store, "renew_lease", _track_renew)

    run = service.enrich_stale_documents(doc_id=frontmatter.id, force=True)

    assert run.status == "succeeded"
    assert renew_calls
    assert all(name == "lightweight-enrichment" for name, _ttl in renew_calls)
