import importlib
import json
import logging
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.core.logging import bind_log_context, reset_log_context
from app.core.metrics import render_metrics, reset_metrics


class _FakeResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": (
                                    '{"short_summary":"Summary","why_it_matters":"Why","whats_new":"New",'
                                    '"caveats":"Caveat","follow_up_questions":["Q1","Q2"],'
                                    '"contribution":"Contribution","method":"Method","result":"Result",'
                                    '"limitation":"Limitation","possible_extension":"Extension"}'
                                )
                            }
                        ]
                    }
                }
            ]
        }


class _FakeTagResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": '{"selected_tags":["area/agents","method/tool_use","type/framework"]}'
                            }
                        ]
                    }
                }
            ]
        }


class _FakeAudioBriefResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": (
                                    '{"intro":"Good morning. Start with the lead stories.",'
                                    '"outro":"Carry the strongest question into your next read.",'
                                    '"chapters":['
                                    '{"item_id":"item-1","headline":"Lead story: Verifier routing","narration":"A concise spoken summary."},'
                                    '{"item_id":"item-2","headline":"Paper to watch: Tool use","narration":"A second spoken summary."}'
                                    "]}"
                                )
                            }
                        ]
                    }
                }
            ]
        }


class _FakeEditorialNoteResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "usageMetadata": {
                "promptTokenCount": 128,
                "candidatesTokenCount": 24,
                "totalTokenCount": 152,
            },
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": (
                                    '{"note":"Verifier routing and lower-latency tool releases set the tone today"}'
                                )
                            }
                        ]
                    }
                }
            ]
        }


class _FakeNewsletterFactsResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "usageMetadata": {
                "promptTokenCount": 184,
                "candidatesTokenCount": 62,
                "totalTokenCount": 246,
            },
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": (
                                    '{"facts":['
                                    '{"headline":"Model launch to watch",'
                                    '"summary":"A new model release focuses on lower-latency tool use.",'
                                    '"why_it_matters":"faster tool use, lower latency"},'
                                    '{"headline":"Paper worth a skim",'
                                    '"summary":"A new paper argues verifier routing improves research triage.",'
                                    '"why_it_matters":"better triage, verifier routing"}'
                                    "]}"
                                )
                            }
                        ]
                    }
                }
            ]
        }


class _FakeItemEnrichmentResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "usageMetadata": {
                "promptTokenCount": 220,
                "candidatesTokenCount": 44,
                "totalTokenCount": 264,
            },
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": (
                                    '{"items":['
                                    '{"item_id":"item-1","relevance_score":0.92,"reason":"Strong topic overlap.",'
                                    '"tags":["agents","tool-use"],"authors":["Alex Researcher"]},'
                                    '{"item_id":"item-2","relevance_score":0.24,"reason":"Weak profile alignment.",'
                                    '"tags":["benchmark"],"authors":[]}'
                                    "]}"
                                )
                            }
                        ]
                    }
                }
            ],
        }


def test_llm_client_uses_gemini_generate_content(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    get_settings.cache_clear()

    captured: dict = {}

    def _fake_post(url, *, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr("httpx.post", _fake_post)
    module = importlib.import_module("app.integrations.llm")

    payload = module.LLMClient().summarize_item(
        {"title": "Verifier routing", "source_name": "arXiv", "content_type": "paper"},
        "A paper about verifier routing.",
    )

    assert captured["url"].endswith("/models/gemini-2.5-flash:generateContent")
    assert captured["headers"]["x-goog-api-key"] == "test-gemini-key"
    assert captured["json"]["generationConfig"]["responseMimeType"] == "application/json"
    assert payload["short_summary"] == "Summary"

    get_settings.cache_clear()


def test_llm_client_uses_gemini_generate_content_for_zotero_tags(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    get_settings.cache_clear()

    captured: dict = {}

    def _fake_post(url, *, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeTagResponse()

    monkeypatch.setattr("httpx.post", _fake_post)
    module = importlib.import_module("app.integrations.llm")

    payload = module.LLMClient().suggest_zotero_tags(
        {"title": "Tool-Using Research Agents", "source_name": "arXiv", "content_type": "paper"},
        "A paper about tool-using research agents.",
        ["area/agents", "method/tool_use", "type/framework"],
        insight={"short_summary": "Agents with tools.", "why_it_matters": "Improves research workflows."},
    )

    schema = captured["json"]["generationConfig"]["responseJsonSchema"]
    assert schema["properties"]["selected_tags"]["items"]["enum"] == [
        "area/agents",
        "method/tool_use",
        "type/framework",
    ]
    assert payload == ["area/agents", "method/tool_use", "type/framework"]

    get_settings.cache_clear()


def test_llm_client_uses_gemini_generate_content_for_batch_item_enrichment(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    get_settings.cache_clear()

    captured: dict = {}

    def _fake_post(url, *, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeItemEnrichmentResponse()

    monkeypatch.setattr("httpx.post", _fake_post)
    module = importlib.import_module("app.integrations.llm")

    payload = module.LLMClient().batch_enrich_items(
        [
            {
                "item_id": "item-1",
                "title": "Tool-Using Research Agents",
                "source_name": "arXiv",
                "content_type": "paper",
                "authors": [],
                "organization_name": "OpenAI",
                "analysis_text": "A paper about tool-using research agents.",
            },
            {
                "item_id": "item-2",
                "title": "Benchmark Update",
                "source_name": "Example Feed",
                "content_type": "article",
                "authors": ["Reporter"],
                "organization_name": None,
                "analysis_text": "A general benchmark update.",
            },
        ],
        {
            "favorite_topics": ["agents", "tool use"],
            "favorite_authors": ["Alex Researcher"],
            "favorite_sources": ["arXiv"],
            "ignored_topics": ["crypto"],
        },
    )

    schema = captured["json"]["generationConfig"]["responseJsonSchema"]
    assert schema["properties"]["items"]["maxItems"] == 10
    assert payload["items"][0]["item_id"] == "item-1"
    assert payload["items"][0]["tags"] == ["agents", "tool-use"]
    assert payload["_usage"]["total_tokens"] == 264

    get_settings.cache_clear()


def test_llm_client_uses_gemini_generate_content_for_audio_brief(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    get_settings.cache_clear()

    captured: dict = {}

    def _fake_post(url, *, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeAudioBriefResponse()

    monkeypatch.setattr("httpx.post", _fake_post)
    module = importlib.import_module("app.integrations.llm")

    payload = module.LLMClient().compose_audio_brief(
        {
            "title": "Morning Brief • 2026-03-27",
            "brief_date": "2026-03-27",
            "target_duration_minutes": 5,
            "editorial_note": "Start with the strongest item.",
            "suggested_follow_ups": ["What deserves a deeper read next?"],
            "shortlisted_items": [
                {
                    "item_id": "item-1",
                    "title": "Verifier routing",
                    "source_name": "arXiv",
                    "section": "editorial_shortlist",
                    "rank": 1,
                    "note": "Improves research triage.",
                    "short_summary": "A concise summary.",
                    "why_it_matters": "It could reduce wasted reading time.",
                    "whats_new": "The routing policy now adapts to uncertainty.",
                    "caveats": "The benchmarks are still narrow.",
                    "follow_up_questions": ["Which baselines does it beat?"],
                    "source_excerpt": "The paper compares verifier routing against a fixed review pass.",
                },
                {
                    "item_id": "item-2",
                    "title": "Tool use",
                    "source_name": "OpenAI",
                    "section": "papers_table",
                    "rank": 1,
                    "note": "Improves workflow execution.",
                    "short_summary": "Another concise summary.",
                    "why_it_matters": "It changes where tool latency becomes acceptable.",
                    "whats_new": "The release focuses on lower-latency execution loops.",
                    "caveats": "The real-world ceiling is still unclear.",
                    "follow_up_questions": ["How does it behave under load?"],
                    "source_excerpt": "The release notes emphasize faster execution and tighter iteration cycles.",
                },
            ],
        }
    )

    assert captured["url"].endswith("/models/gemini-2.5-flash:generateContent")
    assert captured["headers"]["x-goog-api-key"] == "test-gemini-key"
    assert captured["json"]["generationConfig"]["responseJsonSchema"]["required"] == [
        "intro",
        "outro",
        "chapters",
    ]
    prompt = captured["json"]["contents"][0]["parts"][0]["text"]
    system_instruction = captured["json"]["systemInstruction"]["parts"][0]["text"]
    assert "calm, well-produced morning podcast" in prompt
    assert "The output will be spoken out loud" in prompt
    assert "casual newsletter reader" in prompt
    assert "coffee" not in prompt
    assert "Aim for roughly 5 minutes total" in prompt
    assert "Cover every shortlisted item and use the fuller context for each one" in prompt
    assert "Do not read field labels, section names, ranks, or bullet structure aloud." in prompt
    assert "Avoid sounding like pasted summaries or a task manager." in prompt
    assert "Do not start spoken paragraphs with title-like fragments" in prompt
    assert "The headline field is metadata only" in prompt
    assert "Why it matters: It could reduce wasted reading time." in prompt
    assert "Source excerpt: The paper compares verifier routing against a fixed review pass." in prompt
    assert "Favor complete spoken sentences, soft connective phrasing, and understated confidence." in prompt
    assert "thoughtful morning podcast host" in system_instruction
    assert "This output will be read aloud" in system_instruction
    assert "narration should begin as natural speech" in system_instruction
    assert payload["intro"] == "Good morning. Start with the lead stories."
    assert payload["chapters"][0]["item_id"] == "item-1"
    assert payload["chapters"][1]["item_id"] == "item-2"
    assert payload["generation_mode"] == "remote"

    get_settings.cache_clear()


def test_llm_client_uses_gemini_generate_content_for_editorial_note(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    get_settings.cache_clear()

    captured: dict = {}

    def _fake_post(url, *, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeEditorialNoteResponse()

    monkeypatch.setattr("httpx.post", _fake_post)
    module = importlib.import_module("app.integrations.llm")

    payload = module.LLMClient().compose_editorial_note(
        {
            "title": "Morning Brief • 2026-03-27",
            "brief_date": "2026-03-27",
            "editorial_shortlist": [
                {
                    "item_id": "item-1",
                    "section": "editorial_shortlist",
                    "rank": 1,
                    "title": "Verifier routing",
                    "source_name": "arXiv",
                    "content_type": "paper",
                    "note": "Verifier routing could reduce wasted reading time.",
                    "short_summary": "A paper argues verifier routing improves research triage.",
                    "why_it_matters": "It could make daily research scanning faster.",
                    "whats_new": "The routing policy adapts to uncertainty.",
                    "caveats": "The evaluation is still narrow.",
                }
            ],
            "headlines": [
                {
                    "item_id": "item-2",
                    "section": "headlines",
                    "rank": 1,
                    "title": "Latency-focused model release",
                    "source_name": "Example Labs",
                    "content_type": "article",
                    "note": "Lower-latency tool use is getting more practical.",
                    "short_summary": "A model release focuses on lower-latency tool use.",
                    "why_it_matters": "It could change when agents feel usable in production.",
                    "whats_new": "The release narrows response time under load.",
                    "caveats": "External validation is still limited.",
                }
            ],
            "interesting_side_signals": [],
            "remaining_reads": [],
            "audio_script": "Good morning. Verifier routing and lower-latency tool use are the main threads today.",
            "fallback_note": "Fallback note.",
        }
    )

    assert captured["url"].endswith("/models/gemini-2.5-flash:generateContent")
    assert captured["headers"]["x-goog-api-key"] == "test-gemini-key"
    assert captured["json"]["generationConfig"]["responseJsonSchema"]["required"] == ["note"]
    prompt = captured["json"]["contents"][0]["parts"][0]["text"]
    system_instruction = captured["json"]["systemInstruction"]["parts"][0]["text"]
    assert "Write one short editorial note for a daily research/news brief." in prompt
    assert "summarize the day's main news" in prompt
    assert "If an existing voice-summary script is present" in prompt
    assert "Verifier routing" in prompt
    assert "Latency-focused model release" in prompt
    assert "Existing voice-summary script" in prompt
    assert "Prefer synthesis over enumeration." in system_instruction
    assert payload["note"] == "Verifier routing and lower-latency tool releases set the tone today."
    assert payload["_usage"] == {"prompt_tokens": 128, "completion_tokens": 24, "total_tokens": 152}

    get_settings.cache_clear()


def test_llm_client_splits_newsletter_into_fast_reads_with_gemini(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    get_settings.cache_clear()

    captured: dict = {}

    def _fake_post(url, *, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeNewsletterFactsResponse()

    monkeypatch.setattr("httpx.post", _fake_post)
    module = importlib.import_module("app.integrations.llm")

    payload = module.LLMClient().split_newsletter_message(
        {
            "source_name": "TLDR AI",
            "subject": "TLDR AI",
            "sender": "TLDR AI <hi@tldrnewsletter.com>",
            "published_at": "2026-03-27T07:00:00+00:00",
            "text_body": (
                "First update. A new model release focuses on lower-latency tool use. "
                "Second update. A new paper argues verifier routing improves research triage."
            ),
            "outbound_links": ["https://example.com/model", "https://example.com/paper"],
        }
    )

    prompt = captured["json"]["contents"][0]["parts"][0]["text"]
    assert "Split one newsletter email into several compact fast reads." in prompt
    assert "Return between 2 and 6 facts" in prompt
    assert payload["generation_mode"] == "remote"
    assert payload["_usage"]["total_tokens"] == 246
    assert len(payload["facts"]) == 2
    assert payload["facts"][0]["headline"] == "Model launch to watch"
    assert payload["facts"][0]["follow_up_questions"]
    assert payload["facts"][0]["relevant_links"] == ["https://example.com/model"]
    assert payload["facts"][1]["why_it_matters"] == "better triage, verifier routing"
    assert payload["facts"][1]["relevant_links"] == ["https://example.com/paper"]

    get_settings.cache_clear()


def test_llm_client_uses_morning_update_style_for_audio_brief_without_gemini(monkeypatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    get_settings.cache_clear()

    module = importlib.import_module("app.integrations.llm")

    payload = module.LLMClient().compose_audio_brief(
        {
            "title": "Morning Brief • 2026-03-27",
            "brief_date": "2026-03-27",
            "editorial_note": "Start with the strongest item.",
            "suggested_follow_ups": ["What deserves a deeper read next?"],
            "shortlisted_items": [
                {
                    "item_id": "item-1",
                    "title": "Verifier routing",
                    "source_name": "arXiv",
                    "section": "editorial_shortlist",
                    "rank": 1,
                    "note": "Improves research triage.",
                    "short_summary": "A concise summary.",
                },
                {
                    "item_id": "item-2",
                    "title": "Tool use",
                    "source_name": "OpenAI",
                    "section": "papers_table",
                    "rank": 1,
                    "note": "Improves workflow execution.",
                    "short_summary": "Another concise summary.",
                },
            ],
        }
    )

    assert payload["generation_mode"] == "heuristic"
    assert payload["intro"] == (
        "Good morning. Here's your calm research briefing for March 27, 2026. "
        "Start with the strongest item."
    )
    assert payload["outro"] == (
        "That is the morning briefing. As you get into the day, keep this question in mind: "
        "What deserves a deeper read next?"
    )
    assert payload["chapters"][0]["headline"] == "To start: Verifier routing"
    assert payload["chapters"][0]["narration"].startswith(
        "To start, this is the item to keep in view this morning."
    )
    assert "What matters most is this: Improves research triage." in payload["chapters"][0][
        "narration"
    ]
    assert "In short: A concise summary." in payload["chapters"][0]["narration"]
    assert payload["chapters"][1]["headline"] == "On the research side: Tool use"
    assert payload["chapters"][1]["narration"].startswith(
        "On the research side, this paper is the clearest place to go a bit deeper."
    )

    get_settings.cache_clear()


def test_llm_client_heuristically_splits_newsletter_without_gemini(monkeypatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    get_settings.cache_clear()

    module = importlib.import_module("app.integrations.llm")

    payload = module.LLMClient().split_newsletter_message(
        {
            "subject": "Morning newsletter",
            "sender": "Sender",
            "text_body": (
                "First item. A new launch improves tool use in production. "
                "It reduces latency on agent tasks. "
                "Second item. A fresh paper proposes verifier routing for research triage. "
                "It reports cleaner escalation decisions."
            ),
        }
    )

    assert payload["generation_mode"] == "heuristic"
    assert len(payload["facts"]) >= 2
    assert payload["facts"][0]["headline"]
    assert payload["facts"][0]["summary"]
    assert payload["facts"][0]["why_it_matters"]
    assert payload["facts"][0]["whats_new"]
    assert payload["facts"][0]["caveats"]
    assert payload["facts"][0]["follow_up_questions"]

    get_settings.cache_clear()


def test_llm_client_uses_heuristics_for_zotero_tags_without_gemini(monkeypatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    get_settings.cache_clear()

    module = importlib.import_module("app.integrations.llm")

    payload = module.LLMClient().suggest_zotero_tags(
        {"title": "Multi-Agent Retrieval with a 70B Transformer", "source_name": "arXiv", "content_type": "paper"},
        (
            "We study a multi-agent retrieval-augmented generation framework built on a 70B transformer. "
            "The paper evaluates reasoning and code performance with extensive experiments and ablations."
        ),
        [
            "area/agents",
            "method/retrieval",
            "arch/transformer",
            "type/framework",
            "type/empirical",
            "scale/10b",
            "eval/reasoning",
            "eval/code",
        ],
        insight={"short_summary": "Multi-agent RAG system.", "why_it_matters": "Improves reasoning and code tasks."},
    )

    assert "area/agents" in payload
    assert "method/retrieval" in payload
    assert "arch/transformer" in payload
    assert "type/empirical" in payload
    assert "scale/10b" in payload
    assert "eval/reasoning" in payload
    assert "eval/code" in payload

    get_settings.cache_clear()


def test_llm_client_falls_back_without_call_when_daily_budget_is_exhausted(
    client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
    monkeypatch.setenv("AI_DAILY_COST_LIMIT_USD", "0.0")
    get_settings.cache_clear()

    def _unexpected_post(*args, **kwargs):
        raise AssertionError("Gemini should not be called once the daily AI budget is exhausted.")

    monkeypatch.setattr("httpx.post", _unexpected_post)
    module = importlib.import_module("app.integrations.llm")

    payload = module.LLMClient().summarize_item(
        {"title": "Verifier routing", "source_name": "arXiv", "content_type": "paper"},
        "A paper about verifier routing under a strict cost cap.",
    )

    assert payload["short_summary"]
    assert payload["why_it_matters"]

    get_settings.cache_clear()


def test_llm_client_persists_trace_artifacts_and_metrics(
    client: TestClient,
    monkeypatch,
    caplog,
) -> None:
    del client
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    get_settings.cache_clear()
    reset_metrics()
    caplog.set_level(logging.INFO)

    def _fake_post(url, *, headers, json, timeout):
        del url, headers, json, timeout
        return _FakeEditorialNoteResponse()

    monkeypatch.setattr("httpx.post", _fake_post)
    module = importlib.import_module("app.integrations.llm")

    payload = module.LLMClient().compose_editorial_note(
        {
            "title": "Research Brief",
            "brief_date": "2026-04-08",
            "summary_depth": "balanced",
            "editorial_shortlist": [],
            "headlines": [],
            "interesting_side_signals": [],
            "remaining_reads": [],
            "audio_script": None,
            "fallback_note": "Fallback note.",
        }
    )

    trace = payload.get("_trace")
    assert isinstance(trace, dict)
    prompt_path = Path(str(trace["prompt_path"]))
    trace_path = Path(str(trace["trace_path"]))
    assert prompt_path.exists()
    assert trace_path.exists()
    assert "Write one short editorial note for a daily research/news brief." in prompt_path.read_text(
        encoding="utf-8"
    )
    artifact = json.loads(trace_path.read_text(encoding="utf-8"))
    assert artifact["operation"] == "compose_editorial_note"
    assert artifact["status"] == "succeeded"
    assert artifact["usage"]["total_tokens"] == 152
    assert trace["cost_usd"] > 0

    metrics = render_metrics()
    assert (
        'research_center_llm_requests_total{provider="gemini",model="gemini-2.5-flash",'
        'operation="compose_editorial_note",status="success"} 1'
        in metrics
    )
    assert (
        'research_center_llm_tokens_total{provider="gemini",model="gemini-2.5-flash",'
        'operation="compose_editorial_note",token_type="total"} 152'
        in metrics
    )
    assert any(
        record.getMessage() == "ai.invocation.completed"
        and getattr(record, "trace_id", None) == trace["trace_id"]
        for record in caplog.records
    )

    get_settings.cache_clear()
    reset_metrics()


def test_llm_client_logs_completion_with_bound_operation_context(
    client: TestClient,
    monkeypatch,
    caplog,
) -> None:
    del client
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    get_settings.cache_clear()
    reset_metrics()
    caplog.set_level(logging.INFO)

    def _fake_post(url, *, headers, json, timeout):
        del url, headers, json, timeout
        return _FakeEditorialNoteResponse()

    monkeypatch.setattr("httpx.post", _fake_post)
    module = importlib.import_module("app.integrations.llm")

    token = bind_log_context(
        operation_run_id="run-lightweight",
        operation_kind="lightweight_enrichment",
        doc_id="doc-123",
    )
    try:
        payload = module.LLMClient().compose_editorial_note(
            {
                "title": "Research Brief",
                "brief_date": "2026-04-08",
                "summary_depth": "balanced",
                "editorial_shortlist": [],
                "headlines": [],
                "interesting_side_signals": [],
                "remaining_reads": [],
                "audio_script": None,
                "fallback_note": "Fallback note.",
            }
        )
    finally:
        reset_log_context(token)

    trace = payload.get("_trace")
    assert isinstance(trace, dict)
    completed = [
        record
        for record in caplog.records
        if record.getMessage() == "ai.invocation.completed"
        and getattr(record, "trace_id", None) == trace["trace_id"]
    ]
    assert completed
    assert completed[-1].operation_run_id == "run-lightweight"
    assert completed[-1].operation_kind == "lightweight_enrichment"
    assert completed[-1].doc_id == "doc-123"

    get_settings.cache_clear()
    reset_metrics()


def test_llm_client_records_failed_invocation_trace_and_metrics(
    client: TestClient,
    monkeypatch,
) -> None:
    del client
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    get_settings.cache_clear()
    reset_metrics()

    def _timeout(*args, **kwargs):
        del args, kwargs
        raise httpx.ReadTimeout("network timeout")

    monkeypatch.setattr("httpx.post", _timeout)
    module = importlib.import_module("app.integrations.llm")

    with pytest.raises(httpx.ReadTimeout) as exc_info:
        module.LLMClient().batch_enrich_items(
            [
                {
                    "item_id": "item-1",
                    "title": "Verifier routing",
                    "source_name": "arXiv",
                    "content_type": "paper",
                    "authors": [],
                    "organization_name": None,
                    "short_summary": None,
                    "why_it_matters": None,
                    "whats_new": None,
                    "analysis_text": "Verifier routing with benchmark results.",
                }
            ],
            {
                "favorite_topics": ["agents"],
                "favorite_authors": [],
                "favorite_sources": [],
                "ignored_topics": [],
            },
        )

    trace = getattr(exc_info.value, "ai_trace", None)
    assert isinstance(trace, dict)
    trace_path = Path(str(trace["trace_path"]))
    assert trace_path.exists()
    artifact = json.loads(trace_path.read_text(encoding="utf-8"))
    assert artifact["operation"] == "batch_enrich_items"
    assert artifact["status"] == "failed"
    assert "network timeout" in artifact["error"]

    metrics = render_metrics()
    assert (
        'research_center_llm_requests_total{provider="gemini",model="gemini-2.5-flash",'
        'operation="batch_enrich_items",status="error"} 1'
        in metrics
    )
    assert (
        'research_center_llm_failures_total{provider="gemini",model="gemini-2.5-flash",'
        'operation="batch_enrich_items",reason="ReadTimeout"} 1'
        in metrics
    )

    get_settings.cache_clear()
    reset_metrics()


def test_llm_client_uses_schema_constrained_ollama_output_for_lightweight_scoring(
    client: TestClient,
    monkeypatch,
) -> None:
    del client
    get_settings.cache_clear()

    captured: dict[str, object] = {}

    class _FakeOllamaResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "response": json.dumps(
                    {
                        "relevance_score": 0.81,
                        "source_fit_score": 0.74,
                        "topic_fit_score": 0.88,
                        "author_fit_score": 0.42,
                        "evidence_fit_score": 0.79,
                        "confidence_score": 0.72,
                        "bucket_hint": "must_read",
                        "reason": "Strong workflow fit with concrete evidence.",
                        "evidence_quotes": ["verifier routing", "research triage"],
                    }
                ),
                "prompt_eval_count": 112,
                "eval_count": 28,
            }

    def _fake_post(url, *, json, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeOllamaResponse()

    monkeypatch.setattr("httpx.post", _fake_post)
    module = importlib.import_module("app.integrations.llm")
    monkeypatch.setattr(
        module.LLMClient,
        "ollama_status",
        lambda self: {
            "available": True,
            "model": self.settings.ollama_model,
            "detail": None,
        },
    )

    payload = module.LLMClient().judge_lightweight_document(
        {
            "title": "Verifier routing for faster triage",
            "source_name": "alphaXiv Papers",
            "source_id": "alphaxiv-paper",
            "content_type": "paper",
            "authors": ["Casey Researcher"],
            "tags": ["verifier routing"],
            "short_summary": "A workflow note about verifier routing and triage speed.",
        },
        "Verifier routing can make research triage faster when you have strong evidence checks.",
        profile={
            "favorite_topics": ["verifier routing"],
            "favorite_authors": ["Casey Researcher"],
            "favorite_sources": ["Example Research"],
            "ignored_topics": ["consumer gadget news"],
            "prompt_guidance": {"enrichment": "Prefer workflow leverage."},
            "scoring_rubric": {
                "persona": "AI researcher training large-scale frontier LLMs.",
                "highest_priority_topics": [
                    "post-training methods for LLMs",
                    "reasoning in LLMs",
                ],
                "deprioritize": [
                    "fellowships and org announcements",
                    "generic benchmark churn",
                ],
                "alphaxiv_preferences": [
                    "Treat 50+ X likes as a strong signal.",
                ],
            },
        },
        source_context={
            "source_id": "alphaxiv-paper",
            "name": "alphaXiv Papers",
            "type": "paper",
            "description": "Community-ranked AI papers.",
            "tags": ["paper", "research"],
            "alphaxiv_metrics": {
                "public_total_votes": 63,
                "total_votes": 91,
                "visits_last_7_days": 864,
                "visits_all": 1452,
                "x_likes": 52,
                "citations_count": 6,
            },
            "alphaxiv_engagement_tier": "high",
            "alphaxiv_engagement_summary": "52 X likes, 63 public votes, 864 visits in the last 7 days",
        },
    )

    request_payload = captured["json"]
    assert isinstance(request_payload, dict)
    assert request_payload["format"] == module.LIGHTWEIGHT_SCORING_SCHEMA
    assert "<json_schema>" in str(request_payload["prompt"])
    assert '"relevance_score"' in str(request_payload["prompt"])
    assert "<research_rubric>" in str(request_payload["prompt"])
    assert "frontier-LLM researcher" in str(request_payload["prompt"])
    assert "alphaXiv X likes: 52" in str(request_payload["prompt"])
    assert payload["relevance_score"] == 0.81
    assert payload["topic_fit_score"] == 0.88
    assert payload["bucket_hint"] == "must_read"

    get_settings.cache_clear()
