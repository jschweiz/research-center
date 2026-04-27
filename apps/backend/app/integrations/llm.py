from __future__ import annotations

import json
import logging
import re
from datetime import UTC, date, datetime
from time import perf_counter
from typing import Any

import httpx

from app.core.config import get_settings
from app.core.metrics import record_llm_fallback
from app.services.ai_budget import AIBudgetExceededError, AIBudgetService
from app.services.ai_observability import AIInvocationRecorder
from app.services.text import compact_signal_note, normalize_item_title, normalize_whitespace

SUMMARY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "short_summary": {"type": "string"},
        "why_it_matters": {"type": "string"},
        "whats_new": {"type": "string"},
        "caveats": {"type": "string"},
        "follow_up_questions": {
            "type": "array",
            "items": {"type": "string"},
        },
        "contribution": {"type": ["string", "null"]},
        "method": {"type": ["string", "null"]},
        "result": {"type": ["string", "null"]},
        "limitation": {"type": ["string", "null"]},
        "possible_extension": {"type": ["string", "null"]},
    },
    "required": [
        "short_summary",
        "why_it_matters",
        "whats_new",
        "caveats",
        "follow_up_questions",
        "contribution",
        "method",
        "result",
        "limitation",
        "possible_extension",
    ],
}

DEEPER_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "deeper_summary": {"type": "string"},
        "experiment_ideas": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["deeper_summary", "experiment_ideas"],
}

AUDIO_BRIEF_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "intro": {"type": "string"},
        "outro": {"type": "string"},
        "chapters": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "item_id": {"type": "string"},
                    "headline": {"type": "string"},
                    "narration": {"type": "string"},
                },
                "required": ["item_id", "headline", "narration"],
            },
        },
    },
    "required": ["intro", "outro", "chapters"],
}

EDITORIAL_NOTE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "note": {"type": "string"},
    },
    "required": ["note"],
}

NEWSLETTER_FACT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "facts": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "headline": {"type": "string"},
                    "summary": {"type": "string"},
                    "why_it_matters": {"type": "string"},
                    "whats_new": {"type": "string"},
                    "caveats": {"type": "string"},
                    "follow_up_questions": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "relevant_links": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": [
                    "headline",
                    "summary",
                    "why_it_matters",
                    "whats_new",
                    "caveats",
                    "follow_up_questions",
                    "relevant_links",
                ],
            },
        },
    },
    "required": ["facts"],
}

ITEM_ENRICHMENT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "items": {
            "type": "array",
            "maxItems": 10,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "item_id": {"type": "string"},
                    "relevance_score": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "reason": {"type": "string"},
                    "tags": {
                        "type": "array",
                        "maxItems": 8,
                        "items": {"type": "string"},
                    },
                    "authors": {
                        "type": "array",
                        "maxItems": 5,
                        "items": {"type": "string"},
                    },
                },
                "required": ["item_id", "relevance_score", "reason", "tags", "authors"],
            },
        },
    },
    "required": ["items"],
}

LIGHTWEIGHT_ENRICHMENT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "short_summary": {"type": ["string", "null"]},
        "authors": {
            "type": "array",
            "maxItems": 6,
            "items": {"type": "string"},
        },
        "tags": {
            "type": "array",
            "maxItems": 10,
            "items": {"type": "string"},
        },
    },
    "required": ["short_summary", "authors", "tags"],
}

LIGHTWEIGHT_ENRICHMENT_PROMPT_VERSION = "2026-04-08-structured-v1"
LIGHTWEIGHT_SCORING_PROMPT_VERSION = "2026-04-10-frontier-rubric-v3"

LIGHTWEIGHT_SCORING_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "relevance_score": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "source_fit_score": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "topic_fit_score": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "author_fit_score": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "evidence_fit_score": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "confidence_score": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "bucket_hint": {
            "type": "string",
            "enum": ["must_read", "worth_a_skim", "archive"],
        },
        "reason": {"type": "string"},
        "evidence_quotes": {
            "type": "array",
            "maxItems": 3,
            "items": {"type": "string"},
        },
    },
    "required": [
        "relevance_score",
        "source_fit_score",
        "topic_fit_score",
        "author_fit_score",
        "evidence_fit_score",
        "confidence_score",
        "bucket_hint",
        "reason",
        "evidence_quotes",
    ],
}

AUDIO_SECTION_BRIEF_STYLE = {
    "editorial_shortlist": {
        "headline_prefix": "To start",
        "lead_in": "To start, this is the item to keep in view this morning",
    },
    "papers_table": {
        "headline_prefix": "On the research side",
        "lead_in": "On the research side, this paper is the clearest place to go a bit deeper",
    },
    "headlines": {
        "headline_prefix": "In the news",
        "lead_in": "In the faster-moving lane, this is the update worth clocking early",
    },
    "interesting_side_signals": {
        "headline_prefix": "At the edges",
        "lead_in": "At the edges of the main story, this is the signal to keep in the background",
    },
    "remaining_reads": {
        "headline_prefix": "One more for later",
        "lead_in": "And one more read to keep warm for later",
    },
}

NEWSLETTER_FACT_LIMIT = 6
NEWSLETTER_NOISE_RE = re.compile(
    r"\b("
    r"unsubscribe|manage preferences|privacy policy|view in browser|read online|"
    r"all rights reserved|copyright|advertisement|advertising|sponsored|"
    r"follow us|share this|forwarded message"
    r")\b",
    re.IGNORECASE,
)
URL_RE = re.compile(r"https?://\S+")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")
NEWSLETTER_FACT_LINK_LIMIT = 3
NEWSLETTER_LINK_TOKEN_RE = re.compile(r"[a-z0-9]{4,}")
NEWSLETTER_LINK_STOPWORDS = {
    "about",
    "after",
    "against",
    "because",
    "between",
    "could",
    "first",
    "lower",
    "matters",
    "newsletter",
    "second",
    "summary",
    "their",
    "there",
    "these",
    "this",
    "through",
    "update",
    "watch",
    "worth",
}
LLM_REQUEST_TOKEN_BUFFER = 1024
SUMMARY_MAX_OUTPUT_TOKENS = 700
DEEPER_MAX_OUTPUT_TOKENS = 900
ZOTERO_TAGS_MAX_OUTPUT_TOKENS = 240
ITEM_ENRICHMENT_MAX_OUTPUT_TOKENS = 1600
LIGHTWEIGHT_SCORING_MAX_OUTPUT_TOKENS = 700
AUDIO_BRIEF_MAX_OUTPUT_TOKENS = 2600
EDITORIAL_NOTE_MAX_OUTPUT_TOKENS = 256
NEWSLETTER_FACTS_MAX_OUTPUT_TOKENS = 1800

TAGGING_MAX_TAGS = 10
TAG_PREFIX_LIMITS = {
    "area": 3,
    "method": 3,
    "arch": 2,
    "type": 2,
    "scale": 2,
    "data": 2,
    "eval": 3,
    "status": 1,
    "hook": 2,
}
logger = logging.getLogger(__name__)
TAG_KEYWORDS = {
    "area/llm": ("language model", "llm", "gpt", "instruction tuning", "in-context", "prompting"),
    "area/vision": ("vision", "image", "visual", "video", "segmentation", "detection", "captioning"),
    "area/multimodal": ("multimodal", "vision-language", "vision language", "vlm", "image-text", "audio-text", "video-language"),
    "area/rl": ("reinforcement learning", "policy gradient", "q-learning", "actor-critic", "bandit"),
    "area/alignment": ("alignment", "constitutional", "preference optimization", "harmless", "helpful honest"),
    "area/interpretability": ("interpretability", "mechanistic", "probing", "feature attribution", "saliency", "circuit"),
    "area/agents": ("agent", "agents", "multi-agent", "tool use", "tool-use", "planning", "workflow"),
    "area/theory": ("theorem", "proof", "bound", "convergence", "sample complexity", "theoretical"),
    "area/efficiency": ("efficient", "efficiency", "latency", "throughput", "compression", "speedup"),
    "area/safety": ("safety", "jailbreak", "red team", "harmful", "toxicity", "misuse"),
    "area/evals": ("benchmark", "evaluation", "eval", "leaderboard", "test suite"),
    "area/robotics": ("robot", "robotics", "manipulation", "locomotion", "embodied"),
    "area/systems": ("serving", "inference engine", "distributed training", "scheduler", "kernel fusion"),
    "area/data": ("dataset", "data curation", "data filtering", "corpus", "annotation", "data quality"),
    "method/pretraining": ("pretrain", "pre-training", "next-token prediction", "masked language modeling"),
    "method/posttraining": ("posttrain", "post-training", "fine-tuning", "finetuning", "instruction tuning"),
    "method/rhf": ("rlhf", "human feedback fine-tuning", "reinforcement learning from human feedback"),
    "method/rlaif": ("rlaif", "ai feedback", "model feedback"),
    "method/dpo": ("dpo", "direct preference optimization"),
    "method/distillation": ("distillation", "teacher-student", "student model"),
    "method/moe": ("mixture-of-experts", "mixture of experts", "moe"),
    "method/sparsity": ("sparsity", "sparse activation", "sparse autoencoder", "sparse model"),
    "method/quantization": ("quantization", "quantized", "int8", "int4", "4-bit", "8-bit", "fp8"),
    "method/pruning": ("pruning", "pruned", "lottery ticket"),
    "method/retrieval": ("retrieval", "rag", "retriever", "reranker", "dense retrieval"),
    "method/tool_use": ("tool use", "function calling", "api calling", "toolformer"),
    "method/self_improvement": ("self-improvement", "self improvement", "self-refine", "self-correction", "verifier"),
    "method/synthetic_data": ("synthetic data", "generated data", "model-generated", "distilled data"),
    "method/curriculum": ("curriculum", "curriculum learning"),
    "method/meta_learning": ("meta-learning", "meta learning", "few-shot adaptation"),
    "method/contrastive": ("contrastive", "contrastive learning", "simclr", "clip-style"),
    "method/scaling_laws": ("scaling law", "scaling laws", "compute-optimal", "chinchilla"),
    "arch/transformer": ("transformer", "decoder-only", "encoder-decoder"),
    "arch/attention": ("attention", "self-attention", "cross-attention", "flashattention"),
    "arch/moe": ("mixture-of-experts", "mixture of experts", "moe"),
    "arch/state_space": ("state space", "state-space", "mamba", "ssm"),
    "arch/diffusion": ("diffusion", "denoising diffusion"),
    "arch/gan": ("gan", "generative adversarial"),
    "arch/rl_policy": ("policy model", "policy network", "policy gradient", "actor-critic"),
    "arch/world_model": ("world model", "dreamer", "latent dynamics"),
    "arch/memory": ("memory", "episodic memory", "long-term memory", "memory module"),
    "type/theory": ("theorem", "proof", "convergence", "guarantee", "bound"),
    "type/empirical": ("experiment", "evaluation", "benchmark", "ablation", "empirical"),
    "type/benchmark": ("benchmark", "leaderboard", "evaluation suite", "test suite"),
    "type/survey": ("survey", "review", "overview", "taxonomy"),
    "type/framework": ("framework", "toolkit", "library", "platform", "stack"),
    "type/negative_result": ("negative result", "fails to", "failure case", "no improvement", "does not improve"),
    "type/reproduction": ("reproduce", "reproduction", "replication", "replicate"),
    "type/scaling_study": ("scaling study", "scaling law", "scaling laws", "compute-optimal"),
    "scale/compute_efficient": ("compute efficient", "compute-efficient", "parameter efficient", "low compute"),
    "scale/low_resource": ("low-resource", "low resource", "few-shot", "few shot", "resource-constrained"),
    "scale/edge": ("on-device", "edge", "mobile", "embedded"),
    "data/webscale": ("web-scale", "webscale", "common crawl", "internet-scale"),
    "data/synthetic": ("synthetic data", "generated data", "model-generated"),
    "data/human_feedback": ("human feedback", "preference data", "human preference"),
    "data/self_play": ("self-play", "self play"),
    "data/simulation": ("simulation", "simulated", "simulator"),
    "data/curated": ("curated", "expert curated", "handcrafted dataset"),
    "data/filtered": ("filtered", "data filtering", "deduplicated", "filtered corpus"),
    "eval/reasoning": ("reasoning", "chain-of-thought", "logical reasoning"),
    "eval/code": ("code", "coding", "program synthesis", "software engineering"),
    "eval/math": ("math", "mathematics", "gsm8k", "aime", "olympiad"),
    "eval/long_context": ("long context", "long-context", "context window", "needle in a haystack"),
    "eval/safety": ("safety eval", "red teaming", "toxicity", "harmful", "refusal"),
    "eval/robustness": ("robustness", "adversarial", "stress test", "distribution shift"),
    "eval/ood": ("out-of-distribution", "ood", "out of domain"),
    "eval/multilingual": ("multilingual", "cross-lingual", "cross lingual", "many languages"),
    "eval/efficiency": ("latency", "throughput", "tokens/s", "energy", "inference cost"),
    "hook/theory_gap": ("theory gap", "lacks theory", "not theoretically understood"),
    "hook/contradiction": ("contradict", "counterexample", "inconsistent result"),
    "hook/scaling_question": ("scaling law", "scaling laws", "scale up"),
    "hook/replication_candidate": ("reproduction", "replication", "replicate"),
}
SCALE_BILLION_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(?:b|billion)\b")
SCALE_TRILLION_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*(?:t|trillion)\b")


def build_tagging_schema(allowed_tags: list[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "selected_tags": {
                "type": "array",
                "items": {"type": "string", "enum": allowed_tags},
                "maxItems": TAGGING_MAX_TAGS,
            },
        },
        "required": ["selected_tags"],
    }


class LLMClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.budget_service = AIBudgetService()
        self.invocation_recorder = AIInvocationRecorder()

    def ollama_status(self) -> dict[str, Any]:
        try:
            response = httpx.get(
                f"{self.settings.ollama_base_url.rstrip('/')}/api/tags",
                timeout=self.settings.ollama_timeout_seconds,
            )
            response.raise_for_status()
        except Exception as exc:
            return {
                "available": False,
                "model": self.settings.ollama_model,
                "detail": f"Ollama is unavailable: {exc}",
            }

        payload = response.json()
        models = payload.get("models") if isinstance(payload, dict) else None
        names = {
            str(model.get("name") or "").strip()
            for model in (models or [])
            if isinstance(model, dict)
        }
        if self.settings.ollama_model in names:
            return {
                "available": True,
                "model": self.settings.ollama_model,
                "detail": f"Ollama model {self.settings.ollama_model} is ready.",
            }
        if names:
            return {
                "available": False,
                "model": self.settings.ollama_model,
                "detail": (
                    f"Ollama is reachable, but model {self.settings.ollama_model} is not pulled. "
                    f"Available models: {', '.join(sorted(names))}."
                ),
            }
        return {
            "available": False,
            "model": self.settings.ollama_model,
            "detail": "Ollama is reachable, but no models are available.",
        }

    def lightweight_enrich_raw_document(self, item: dict[str, Any], text: str) -> dict[str, Any]:
        normalized_item = self._normalize_item_context(item)
        prompt = (
            "Produce a lightweight metadata pass for one raw research document.\n"
            "Return JSON only.\n"
            "Only infer authors when the source text clearly supports them.\n"
            "Keep the summary to 1 or 2 tight sentences.\n"
            "Tags should be short lowercase phrases and should stay close to the document's actual content.\n"
            "Do not invent claims beyond the provided text.\n"
            f"Document kind: {normalized_item.get('content_type')}\n"
            f"Title: {normalized_item.get('title')}\n"
            f"Source: {normalized_item.get('source_name')}\n"
            f"Current authors: {', '.join(normalized_item.get('authors') or []) or 'None'}\n"
            f"Current tags: {', '.join(normalized_item.get('tags') or []) or 'None'}\n\n"
            f"Source text:\n{text[:12000]}"
        )
        parsed, usage, trace = self._generate_ollama_json(
            operation_name="lightweight_enrich_raw_document",
            prompt=prompt,
            schema_name="lightweight_enrichment",
            schema=LIGHTWEIGHT_ENRICHMENT_SCHEMA,
            temperature=0.1,
            error_prefix="Ollama lightweight enrichment failed",
        )

        normalized_payload = self._normalize_lightweight_enrichment_payload(
            normalized_item,
            parsed,
            text,
        )
        return self._attach_trace_metadata(
            normalized_payload,
            usage=usage,
            trace=trace,
        )

    def lightweight_enrichment_pipeline_signature(self) -> str:
        return (
            f"ollama:{self.settings.ollama_model}:lightweight_enrichment:"
            f"{LIGHTWEIGHT_ENRICHMENT_PROMPT_VERSION}"
        )

    def judge_lightweight_document(
        self,
        item: dict[str, Any],
        text: str,
        *,
        profile: dict[str, Any],
        source_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_item = self._normalize_item_context(item)
        normalized_source = source_context if isinstance(source_context, dict) else {}
        prompt_guidance = ""
        if isinstance(profile.get("prompt_guidance"), dict):
            prompt_guidance = normalize_whitespace(str(profile["prompt_guidance"].get("enrichment") or ""))
        scoring_rubric = (
            profile.get("scoring_rubric")
            if isinstance(profile.get("scoring_rubric"), dict)
            else {}
        )

        profile_lines = [
            f"- Favorite topics: {', '.join(profile.get('favorite_topics') or []) or 'None'}",
            f"- Favorite authors: {', '.join(profile.get('favorite_authors') or []) or 'None'}",
            f"- Favorite sources: {', '.join(profile.get('favorite_sources') or []) or 'None'}",
            f"- Ignored topics: {', '.join(profile.get('ignored_topics') or []) or 'None'}",
        ]
        rubric_lines = self._format_scoring_rubric_lines(scoring_rubric)
        source_lines = [
            f"- Source name: {normalized_source.get('name') or normalized_item.get('source_name') or 'Unknown'}",
            f"- Source id: {normalized_source.get('source_id') or normalized_item.get('source_id') or 'Unknown'}",
            f"- Source type: {normalized_source.get('type') or 'Unknown'}",
            f"- Source description: {normalized_source.get('description') or 'None'}",
            f"- Source tags: {', '.join(normalized_source.get('tags') or []) or 'None'}",
        ]
        source_lines.extend(self._format_source_metric_lines(normalized_source))
        document_lines = [
            f"- Document kind: {normalized_item.get('content_type')}",
            f"- Title: {normalized_item.get('title')}",
            f"- Current summary: {normalized_item.get('short_summary') or 'None'}",
            f"- Authors: {', '.join(normalized_item.get('authors') or []) or 'None'}",
            f"- Tags: {', '.join(normalized_item.get('tags') or []) or 'None'}",
        ]
        prompt = (
            "<task>\n"
            "Judge how relevant this document is for the specific user profile.\n"
            "Treat the user as a frontier-LLM researcher and evaluate direct usefulness for what they should read next.\n"
            "Evaluate each rubric criterion separately before deciding the overall score.\n"
            "Ground every score in explicit evidence from the document, source context, and user profile.\n"
            "Be conservative: weak, generic, indirect, or announcement-heavy evidence should reduce the score.\n"
            "</task>\n\n"
            "<evaluation_process>\n"
            "1. First decide whether the document is directly useful for frontier LLM training, post-training, evaluation, efficiency, reasoning, memory, or interpretability.\n"
            "2. Identify concrete evidence and mismatches before scoring.\n"
            "3. Score each rubric criterion independently instead of letting one strong signal dominate.\n"
            "4. Use alphaXiv or source engagement only as a secondary boost after technical fit is established.\n"
            "5. Set relevance_score from the rubric scores, not from general enthusiasm about the topic.\n"
            "6. Use must_read only when the document is clearly high-priority for this specific user right now.\n"
            "</evaluation_process>\n\n"
            "<rubric>\n"
            "- topic_fit_score: how strongly the document topics match favorite topics and the research rubric, while avoiding ignored or deprioritized topics.\n"
            "- source_fit_score: how useful this source and its stated remit are for the user's workflow; engagement can help, but it must stay secondary to technical fit.\n"
            "- author_fit_score: how much the named authors overlap with the user's preferred authors.\n"
            "- evidence_fit_score: how concrete, information-dense, and actionable the document appears from the title, summary, and text.\n"
            "- relevance_score: overall fit for what the user should read next; weight topic fit and evidence most heavily.\n"
            "- confidence_score: confidence in the judgment based on the available evidence.\n"
            "- bucket_hint: use must_read only for clearly high-priority fit, worth_a_skim for meaningful but secondary fit, archive for weak or noisy fit.\n"
            "Scoring anchors:\n"
            "  * 0.00-0.15: clearly irrelevant or unsupported.\n"
            "  * 0.16-0.35: weak overlap or generic interest only.\n"
            "  * 0.36-0.55: some relevance, but not urgent.\n"
            "  * 0.56-0.75: strong fit and likely worth skimming soon.\n"
            "  * 0.76-1.00: clear high-priority fit and likely must-read.\n"
            "Calibration notes:\n"
            "  * Do not reward missing evidence; unknown authors or weak source fit should stay low.\n"
            "  * Do not give >0.75 relevance on a vague title or generic summary without strong body evidence.\n"
            "  * Do not treat fellowships, grants, acquisitions, customer stories, or generic industry chatter as high-priority research.\n"
            "  * Benchmark mentions alone should rarely exceed 0.55 relevance unless the benchmark is genuinely hard or changes frontier model training or evaluation decisions.\n"
            "  * Treat alphaXiv metrics as a tiebreaker: 50+ X likes or 500+ recent views is strong, 10-49 likes or 200-499 recent views is modest, and weak engagement should barely move the score.\n"
            "  * A document can be broadly interesting and still score low for this user if the profile fit is weak.\n"
            "</rubric>\n\n"
            "<user_profile>\n"
            f"{chr(10).join(profile_lines)}\n"
            f"{f'- Additional operator guidance: {prompt_guidance}{chr(10)}' if prompt_guidance else ''}"
            "</user_profile>\n\n"
            "<research_rubric>\n"
            f"{chr(10).join(rubric_lines) if rubric_lines else '- None'}\n"
            "</research_rubric>\n\n"
            "<source_context>\n"
            f"{chr(10).join(source_lines)}\n"
            "</source_context>\n\n"
            "<document>\n"
            f"{chr(10).join(document_lines)}\n\n"
            f"{self._truncate_context_block(text, limit=12000)}\n"
            "</document>\n\n"
            "<output_contract>\n"
            "Return JSON only.\n"
            "Keep reason to one concise sentence.\n"
            "Return up to 3 short evidence_quotes copied verbatim from the document when possible.\n"
            "If there is not enough evidence for a strong claim, lower the score instead of guessing.\n"
            "Do not invent evidence or profile matches.\n"
            "</output_contract>"
        )
        parsed, usage, trace = self._generate_ollama_json(
            operation_name="judge_lightweight_document",
            prompt=prompt,
            schema_name="lightweight_scoring",
            schema=LIGHTWEIGHT_SCORING_SCHEMA,
            temperature=0.1,
            error_prefix="Ollama lightweight scoring failed",
        )
        normalized_payload = self._normalize_lightweight_score_payload(parsed)
        return self._attach_trace_metadata(
            normalized_payload,
            usage=usage,
            trace=trace,
        )

    def lightweight_scoring_pipeline_signature(self) -> str:
        return (
            f"ollama:{self.settings.ollama_model}:lightweight_scoring:"
            f"{LIGHTWEIGHT_SCORING_PROMPT_VERSION}"
        )

    def summarize_item(self, item: dict[str, Any], text: str) -> dict[str, Any]:
        normalized_item = self._normalize_item_context(item)
        fallback_trace: dict[str, Any] | None = None
        if self.settings.gemini_api_key:
            try:
                return self._normalize_summary_payload(
                    normalized_item,
                    self._remote_summary(normalized_item, text),
                    text,
                )
            except AIBudgetExceededError as exc:
                self._log_budget_fallback("summarize_item", exc)
            except Exception as exc:
                record_llm_fallback(operation="summarize_item", reason="remote_error")
                fallback_trace = self._fallback_trace_from_exception(exc)
        return self._attach_trace_metadata(
            self._normalize_summary_payload(normalized_item, self._heuristic_summary(normalized_item, text), text),
            usage=None,
            trace=fallback_trace,
        )

    def deepen_item(self, item: dict[str, Any], text: str) -> dict[str, Any]:
        normalized_item = self._normalize_item_context(item)
        fallback_trace: dict[str, Any] | None = None
        if self.settings.gemini_api_key:
            try:
                return self._remote_deeper(normalized_item, text)
            except AIBudgetExceededError as exc:
                self._log_budget_fallback("deepen_item", exc)
            except Exception as exc:
                record_llm_fallback(operation="deepen_item", reason="remote_error")
                fallback_trace = self._fallback_trace_from_exception(exc)
        summary = self._normalize_summary_payload(normalized_item, self._heuristic_summary(normalized_item, text), text)
        summary["deeper_summary"] = (
            f"{summary['short_summary']} This deserves a closer read for evidence quality, "
            f"baseline comparisons, and transferability."
        )
        summary["experiment_ideas"] = [
            "Check the strongest competing baseline and whether the gain survives the comparison.",
            "List the missing ablations or controls before trusting the main claim.",
            "Identify one follow-up experiment that would falsify the central argument.",
        ]
        return self._attach_trace_metadata(summary, usage=None, trace=fallback_trace)

    def suggest_zotero_tags(
        self,
        item: dict[str, Any],
        text: str,
        allowed_tags: list[str],
        *,
        insight: dict[str, Any] | None = None,
    ) -> list[str]:
        tags, _ = self.suggest_zotero_tags_with_usage(
            item,
            text,
            allowed_tags,
            insight=insight,
        )
        return tags

    def suggest_zotero_tags_with_usage(
        self,
        item: dict[str, Any],
        text: str,
        allowed_tags: list[str],
        *,
        insight: dict[str, Any] | None = None,
    ) -> tuple[list[str], dict[str, int] | None]:
        normalized_item = self._normalize_item_context(item)
        normalized_allowed_tags = self._normalize_allowed_tags(allowed_tags)
        if not normalized_allowed_tags:
            return [], None
        if self.settings.gemini_api_key:
            try:
                payload = self._remote_zotero_tags(normalized_item, text, normalized_allowed_tags, insight or {})
                return (
                    self._normalize_suggested_tags(payload.get("selected_tags"), normalized_allowed_tags),
                    payload.get("_usage"),
                )
            except AIBudgetExceededError as exc:
                self._log_budget_fallback("suggest_zotero_tags", exc)
            except Exception:
                record_llm_fallback(operation="suggest_zotero_tags", reason="remote_error")
        return self._heuristic_zotero_tags(normalized_item, text, normalized_allowed_tags, insight or {}), None

    def batch_enrich_items(
        self,
        items: list[dict[str, Any]],
        profile: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.settings.gemini_api_key:
            raise RuntimeError("Gemini API key is not configured.")
        if not items:
            return {"items": []}
        if len(items) > 10:
            raise ValueError("batch_enrich_items accepts at most 10 items per request.")

        normalized_items = [self._normalize_item_context(item) for item in items]
        prompt_guidance = (
            profile.get("prompt_guidance")
            if isinstance(profile.get("prompt_guidance"), dict)
            else {}
        )
        enrichment_guidance = normalize_whitespace(str(prompt_guidance.get("enrichment") or ""))
        profile_lines = [
            "User profile:",
            f"- Favorite topics: {', '.join(profile.get('favorite_topics') or []) or 'None'}",
            f"- Favorite authors: {', '.join(profile.get('favorite_authors') or []) or 'None'}",
            f"- Favorite sources: {', '.join(profile.get('favorite_sources') or []) or 'None'}",
            f"- Ignored topics: {', '.join(profile.get('ignored_topics') or []) or 'None'}",
        ]
        item_blocks = []
        for item in normalized_items:
            lines = [
                f"Item ID: {item.get('item_id')}",
                f"Title: {item.get('title')}",
                f"Source: {item.get('source_name')}",
                f"Content type: {item.get('content_type')}",
                f"Published at: {item.get('published_at') or 'Unknown'}",
                f"Current authors: {', '.join(item.get('authors') or []) or 'None'}",
                f"Organization: {item.get('organization_name') or 'None'}",
                f"Short summary: {item.get('short_summary') or 'None'}",
                f"Why it matters: {item.get('why_it_matters') or 'None'}",
                f"What's new: {item.get('whats_new') or 'None'}",
                f"Primary text: {self._truncate_context_block(item.get('analysis_text'), limit=2200) or 'None'}",
            ]
            item_blocks.append("\n".join(lines))

        prompt = (
            "Enrich a batch of stored research inbox items for this user.\n"
            "For each item, return:\n"
            "1. relevance_score in [0,1] for this specific user profile\n"
            "2. one short reason grounded in the provided evidence\n"
            "3. 0 to 8 short freeform tags\n"
            "4. 0 to 5 authors only when the provided context supports them\n\n"
            "Be conservative.\n"
            "Do not invent authors, affiliations, or claims.\n"
            "If an item matches ignored topics or is weakly aligned, lower the relevance score.\n"
            "If the evidence for authors is weak, return an empty authors array.\n"
            "Prefer concise lowercase tags such as topic areas, methods, or themes.\n\n"
            f"{chr(10).join(profile_lines)}\n\n"
            f"{'Additional operator guidance: ' + enrichment_guidance + chr(10) + chr(10) if enrichment_guidance else ''}"
            "Items:\n"
            f"{chr(10).join(item_blocks)}"
        )
        return self._generate_json(
            system_instruction=(
                "Return JSON only. Ground every field in the provided item context and user profile. "
                "Do not emit commentary outside the schema."
            ),
            operation_name="batch_enrich_items",
            prompt=prompt,
            schema_name="item_enrichment",
            schema=ITEM_ENRICHMENT_SCHEMA,
            max_output_tokens=ITEM_ENRICHMENT_MAX_OUTPUT_TOKENS,
        )

    def compose_audio_brief(self, digest: dict[str, Any]) -> dict[str, Any]:
        shortlisted_items = digest.get("shortlisted_items", [])
        fallback_trace: dict[str, Any] | None = None
        if self.settings.gemini_api_key:
            try:
                heuristic_fallback = self._heuristic_audio_brief(digest)
                payload = self._remote_audio_brief(digest)
                payload["generation_mode"] = "remote"
                payload["chapters"] = self._normalize_audio_brief_chapters(payload.get("chapters"), shortlisted_items)
                payload["intro"] = self._normalize_brief_block(
                    payload.get("intro"),
                    fallback=heuristic_fallback["intro"],
                )
                payload["outro"] = self._normalize_brief_block(
                    payload.get("outro"),
                    fallback=heuristic_fallback["outro"],
                )
                return payload
            except AIBudgetExceededError as exc:
                self._log_budget_fallback("compose_audio_brief", exc)
            except Exception as exc:
                record_llm_fallback(operation="compose_audio_brief", reason="remote_error")
                fallback_trace = self._fallback_trace_from_exception(exc)
        payload = self._heuristic_audio_brief(digest)
        payload["generation_mode"] = "heuristic"
        return self._attach_trace_metadata(payload, usage=None, trace=fallback_trace)

    def compose_editorial_note(self, digest: dict[str, Any]) -> dict[str, Any]:
        fallback_trace: dict[str, Any] | None = None
        if self.settings.gemini_api_key:
            try:
                payload = self._remote_editorial_note(digest)
                payload["generation_mode"] = "remote"
                payload["note"] = self._normalize_editorial_note_text(
                    payload.get("note"),
                    fallback=str(digest.get("fallback_note") or ""),
                )
                return payload
            except AIBudgetExceededError as exc:
                self._log_budget_fallback("compose_editorial_note", exc)
            except Exception as exc:
                record_llm_fallback(operation="compose_editorial_note", reason="remote_error")
                fallback_trace = self._fallback_trace_from_exception(exc)
        payload = self._heuristic_editorial_note(digest)
        payload["generation_mode"] = "heuristic"
        return self._attach_trace_metadata(payload, usage=None, trace=fallback_trace)

    def split_newsletter_message(self, newsletter: dict[str, Any]) -> dict[str, Any]:
        heuristic_fallback = self._heuristic_newsletter_facts(newsletter)
        fallback_trace: dict[str, Any] | None = None
        if self.settings.gemini_api_key:
            try:
                payload = self._remote_newsletter_facts(newsletter)
                payload["generation_mode"] = "remote"
                payload["facts"] = self._normalize_newsletter_facts(
                    payload.get("facts"),
                    fallback=heuristic_fallback["facts"],
                    newsletter=newsletter,
                )
                return payload
            except AIBudgetExceededError as exc:
                self._log_budget_fallback("split_newsletter_message", exc)
            except Exception as exc:
                record_llm_fallback(operation="split_newsletter_message", reason="remote_error")
                fallback_trace = self._fallback_trace_from_exception(exc)
        heuristic_fallback["generation_mode"] = "heuristic"
        return self._attach_trace_metadata(heuristic_fallback, usage=None, trace=fallback_trace)

    def _remote_summary(self, item: dict[str, Any], text: str) -> dict[str, Any]:
        prompt = (
            "Generate a concise research briefing for one item.\n"
            "Keep each field specific and editorial rather than generic.\n"
            "For papers, prefer abstract-first framing and only mention methods or results that are actually present.\n"
            "Return why_it_matters as a compact comma-separated phrase, not a full sentence.\n"
            f"Item type: {item.get('content_type')}\n"
            f"Title: {item.get('title')}\n"
            f"Source: {item.get('source_name')}\n\n"
            f"Source text:\n{text[:12000]}"
        )
        payload = self._generate_json(
            system_instruction=(
                "Return JSON only. Write concise, high-signal research notes. "
                "Do not invent details that are absent from the source text."
            ),
            operation_name="summarize_item",
            prompt=prompt,
            schema_name="summary",
            schema=SUMMARY_SCHEMA,
            max_output_tokens=SUMMARY_MAX_OUTPUT_TOKENS,
        )
        payload["generated_at"] = datetime.now().isoformat()
        return payload

    def _normalize_lightweight_enrichment_payload(
        self,
        item: dict[str, Any],
        payload: dict[str, Any],
        text: str,
    ) -> dict[str, Any]:
        current_authors = [str(author).strip() for author in (item.get("authors") or []) if str(author).strip()]
        current_tags = [str(tag).strip().lower() for tag in (item.get("tags") or []) if str(tag).strip()]
        summary = normalize_whitespace(str(payload.get("short_summary") or ""))
        summary = summary[:500].strip() or self._heuristic_short_summary(text)
        authors = [
            str(author).strip()
            for author in (payload.get("authors") or [])
            if str(author).strip()
        ]
        tags = [
            normalize_whitespace(str(tag).strip().lower())
            for tag in (payload.get("tags") or [])
            if str(tag).strip()
        ]

        merged_authors = list(dict.fromkeys([*current_authors, *authors]))[:6]
        merged_tags = list(dict.fromkeys([*current_tags, *tags]))[:10]
        return {
            "short_summary": summary or None,
            "authors": merged_authors,
            "tags": merged_tags,
            "generation_mode": "ollama",
            "model": self.settings.ollama_model,
        }

    def _normalize_lightweight_score_payload(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        relevance_score = self._normalize_unit_score(payload.get("relevance_score"))
        source_fit_score = self._normalize_unit_score(payload.get("source_fit_score"))
        topic_fit_score = self._normalize_unit_score(payload.get("topic_fit_score"))
        author_fit_score = self._normalize_unit_score(payload.get("author_fit_score"))
        evidence_fit_score = self._normalize_unit_score(payload.get("evidence_fit_score"))
        confidence_score = self._normalize_unit_score(payload.get("confidence_score"))
        reason = normalize_whitespace(str(payload.get("reason") or ""))
        evidence_quotes = [
            normalize_whitespace(str(quote))
            for quote in (payload.get("evidence_quotes") or [])
            if normalize_whitespace(str(quote))
        ][:3]

        return {
            "relevance_score": relevance_score,
            "source_fit_score": source_fit_score,
            "topic_fit_score": topic_fit_score,
            "author_fit_score": author_fit_score,
            "evidence_fit_score": evidence_fit_score,
            "confidence_score": confidence_score,
            "bucket_hint": self._normalize_score_bucket(
                payload.get("bucket_hint"),
                fallback_score=relevance_score,
            ),
            "reason": reason[:400] or "Profile-fit judgment grounded in the document content.",
            "evidence_quotes": [quote[:160] for quote in evidence_quotes],
            "model": self.settings.ollama_model,
        }

    @staticmethod
    def _format_scoring_rubric_lines(rubric: dict[str, Any]) -> list[str]:
        lines: list[str] = []
        persona = normalize_whitespace(str(rubric.get("persona") or ""))
        if persona:
            lines.append(f"- Research persona: {persona}")
        priorities = [
            normalize_whitespace(str(value))
            for value in (rubric.get("highest_priority_topics") or [])
            if normalize_whitespace(str(value))
        ]
        if priorities:
            lines.append(f"- Highest-priority topics: {', '.join(priorities)}")
        deprioritize = [
            normalize_whitespace(str(value))
            for value in (rubric.get("deprioritize") or [])
            if normalize_whitespace(str(value))
        ]
        if deprioritize:
            lines.append(f"- Deprioritize: {', '.join(deprioritize)}")
        alphaxiv_preferences = [
            normalize_whitespace(str(value))
            for value in (rubric.get("alphaxiv_preferences") or [])
            if normalize_whitespace(str(value))
        ]
        lines.extend(
            f"- alphaXiv preference: {value}" for value in alphaxiv_preferences[:3]
        )
        return lines

    @classmethod
    def _format_source_metric_lines(cls, source_context: dict[str, Any]) -> list[str]:
        metrics = source_context.get("alphaxiv_metrics")
        if not isinstance(metrics, dict):
            return []
        return [
            f"- alphaXiv engagement tier: {source_context.get('alphaxiv_engagement_tier') or 'Unknown'}",
            f"- alphaXiv public votes: {cls._format_numeric_metric(metrics.get('public_total_votes'))}",
            f"- alphaXiv total votes: {cls._format_numeric_metric(metrics.get('total_votes'))}",
            f"- alphaXiv visits in last 7 days: {cls._format_numeric_metric(metrics.get('visits_last_7_days'))}",
            f"- alphaXiv lifetime visits: {cls._format_numeric_metric(metrics.get('visits_all'))}",
            f"- alphaXiv X likes: {cls._format_numeric_metric(metrics.get('x_likes'))}",
            f"- alphaXiv citations: {cls._format_numeric_metric(metrics.get('citations_count'))}",
            f"- alphaXiv engagement summary: {source_context.get('alphaxiv_engagement_summary') or 'None'}",
        ]

    @staticmethod
    def _format_numeric_metric(value: Any) -> str:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return "Unknown"
        if parsed.is_integer():
            return str(int(parsed))
        return f"{parsed:.2f}"

    @staticmethod
    def _normalize_unit_score(value: Any) -> float:
        try:
            score = float(value)
        except (TypeError, ValueError):
            return 0.0
        return round(min(max(score, 0.0), 1.0), 4)

    @classmethod
    def _normalize_score_bucket(cls, value: Any, *, fallback_score: float) -> str:
        normalized = normalize_whitespace(str(value or "")).casefold().replace("-", "_").replace(" ", "_")
        if normalized in {"must_read", "worth_a_skim", "archive"}:
            return normalized
        if fallback_score >= 0.76:
            return "must_read"
        if fallback_score >= 0.36:
            return "worth_a_skim"
        return "archive"

    @staticmethod
    def _heuristic_short_summary(text: str) -> str | None:
        normalized = normalize_whitespace(text)
        if not normalized:
            return None
        sentences = re.split(r"(?<=[.!?])\s+", normalized)
        joined = " ".join(sentence.strip() for sentence in sentences[:2] if sentence.strip())
        return joined[:500].strip() or None

    def _remote_deeper(self, item: dict[str, Any], text: str) -> dict[str, Any]:
        prompt = (
            "Generate deeper research follow-up analysis for one item.\n"
            "Focus on what to verify, what to compare, and what experiment or reading path should come next.\n"
            f"Item type: {item.get('content_type')}\n"
            f"Title: {item.get('title')}\n"
            f"Source: {item.get('source_name')}\n\n"
            f"Source text:\n{text[:12000]}"
        )
        return self._generate_json(
            system_instruction=(
                "Return JSON only. Make the deeper summary concrete, skeptical, and useful for follow-up work."
            ),
            operation_name="deepen_item",
            prompt=prompt,
            schema_name="deeper_summary",
            schema=DEEPER_SCHEMA,
            max_output_tokens=DEEPER_MAX_OUTPUT_TOKENS,
        )

    def _remote_audio_brief(self, digest: dict[str, Any]) -> dict[str, Any]:
        shortlisted_items = digest.get("shortlisted_items", [])
        target_duration_minutes = int(digest.get("target_duration_minutes") or 5)
        summary_depth = normalize_whitespace(str(digest.get("summary_depth") or "balanced"))
        audio_guidance = normalize_whitespace(str(digest.get("audio_prompt_guidance") or ""))
        shortlist_lines = [
            "\n".join(
                [
                    f"Item ID: {item.get('item_id')}",
                    f"Section: {item.get('section')}",
                    f"Rank: {item.get('rank')}",
                    f"Title: {item.get('title')}",
                    f"Source: {item.get('source_name')}",
                    f"Note: {item.get('note')}",
                    f"Short summary: {item.get('short_summary')}",
                    f"Why it matters: {item.get('why_it_matters') or 'None'}",
                    f"What's new: {item.get('whats_new') or 'None'}",
                    f"Caveats: {item.get('caveats') or 'None'}",
                    "Follow-up questions: "
                    f"{'; '.join(item.get('follow_up_questions') or []) or 'None'}",
                    f"Source excerpt: {item.get('source_excerpt') or 'None'}",
                ]
            )
            for item in shortlisted_items
        ]
        shortlist_text = (
            "\n\n".join(shortlist_lines) if shortlist_lines else "No shortlisted items."
        )
        prompt = (
            "Create a TTS-ready spoken morning update from a written digest.\n"
            "The output will be spoken out loud, so every line must sound natural "
            "when heard once rather than read on a screen.\n"
            "This should sound like a calm, well-produced morning podcast for a "
            "casual newsletter reader.\n"
            "Aim for warm, measured delivery with gentle momentum, light "
            "curiosity, and no hype.\n"
            "Use one short intro, one short outro, and one spoken chapter for "
            "each shortlisted digest item.\n"
            f"Aim for roughly {target_duration_minutes} minutes total at a natural "
            "pace, usually around 650 to 850 words overall.\n"
            "Synthesize the notes into natural speech. Do not read field labels, "
            "section names, ranks, or bullet structure aloud.\n"
            "Avoid sounding like pasted summaries or a task manager. Use light "
            "transitions across chapters such as 'To start', 'Also worth your "
            "attention', or 'One more thing before we wrap'.\n"
            "Make the narration feel current and conversational, focused on what "
            "happened, why it matters, and what to keep in mind next.\n"
            "Cover every shortlisted item and use the fuller context for each one, "
            "including why it matters, what is new, caveats, and the source excerpt "
            "when helpful.\n"
            "Each chapter should be understandable in one listen and usually stay "
            "within 3 to 5 sentences.\n"
            "Do not start spoken paragraphs with title-like fragments, colon-led "
            "labels, or raw article names.\n"
            "Do not write standalone titles or screen-style headers. The headline "
            "field is metadata only, and the narration must stand on its own when "
            "spoken without the headline.\n"
            "Favor complete spoken sentences, soft connective phrasing, and "
            "understated confidence.\n"
            "Mention the source only when it adds useful context. Preserve "
            "uncertainty, and do not invent details beyond the provided digest notes.\n"
            "Each chapter must use the exact item_id from the shortlist.\n"
            f"Digest title: {digest.get('title')}\n"
            f"Brief date: {digest.get('brief_date')}\n"
            f"Preferred briefing depth: {summary_depth}\n"
            f"Target duration minutes: {target_duration_minutes}\n"
            f"Editorial note: {digest.get('editorial_note')}\n"
            f"Suggested follow-up: "
            f"{', '.join(digest.get('suggested_follow_ups', [])) or 'None'}\n\n"
            f"{'Additional operator guidance: ' + audio_guidance + chr(10) + chr(10) if audio_guidance else ''}"
            "Shortlisted items:\n"
            f"{shortlist_text}"
        )
        return self._generate_json(
            system_instruction=(
                "Return JSON only. Write for speech, not for the screen. This "
                "output will be read aloud, so avoid written-only titles, visual "
                "formatting cues, list markers, field labels, or quoted digest "
                "notes. The headline field is metadata only; narration should begin "
                "as natural speech rather than a title or label. Sound like a "
                "thoughtful morning podcast host: warm, measured, lightly "
                "conversational, and easy to follow at low volume."
            ),
            operation_name="compose_audio_brief",
            prompt=prompt,
            schema_name="audio_brief",
            schema=AUDIO_BRIEF_SCHEMA,
            max_output_tokens=AUDIO_BRIEF_MAX_OUTPUT_TOKENS,
        )

    def _remote_editorial_note(self, digest: dict[str, Any]) -> dict[str, Any]:
        editorial_shortlist = digest.get("editorial_shortlist", [])
        headlines = digest.get("headlines", [])
        side_signals = digest.get("interesting_side_signals", [])
        remaining_reads = digest.get("remaining_reads", [])
        audio_script = self._truncate_context_block(digest.get("audio_script"), limit=1800)
        summary_depth = normalize_whitespace(str(digest.get("summary_depth") or "balanced"))
        editorial_guidance = normalize_whitespace(str(digest.get("editorial_note_guidance") or ""))

        def format_items(label: str, items: list[dict[str, Any]]) -> str:
            if not items:
                return f"{label}: None"
            blocks = []
            for item in items:
                blocks.append(
                    "\n".join(
                        [
                            f"Section: {item.get('section')}",
                            f"Rank: {item.get('rank')}",
                            f"Title: {item.get('title')}",
                            f"Source: {item.get('source_name')}",
                            f"Type: {item.get('content_type')}",
                            f"Note: {item.get('note') or 'None'}",
                            f"Short summary: {item.get('short_summary') or 'None'}",
                            f"Why it matters: {item.get('why_it_matters') or 'None'}",
                            f"What's new: {item.get('whats_new') or 'None'}",
                            f"Caveats: {item.get('caveats') or 'None'}",
                        ]
                    )
                )
            return f"{label}:\n" + "\n\n".join(blocks)

        prompt = (
            "Write one short editorial note for a daily research/news brief.\n"
            "The note sits at the top of the edition and should summarize the day's main news in one tight paragraph.\n"
            "Keep it to 1 or 2 sentences, usually 30 to 55 words total.\n"
            "Sound like a calm editor, not a hypey announcer.\n"
            "Focus on the most important developments and the connective thread across them.\n"
            "Do not use bullet points, labels, quoted headlines, or generic framing such as 'This edition highlights'.\n"
            "Do not invent details that are absent from the provided context.\n"
            "If an existing voice-summary script is present, you may compress it rather than re-listing every item.\n"
            f"Digest title: {digest.get('title')}\n"
            f"Brief date: {digest.get('brief_date')}\n\n"
            f"Preferred briefing depth: {summary_depth}\n"
            f"{'Additional operator guidance: ' + editorial_guidance + chr(10) + chr(10) if editorial_guidance else ''}"
            f"{format_items('Editorial shortlist', editorial_shortlist)}\n\n"
            f"{format_items('Headlines', headlines)}\n\n"
            f"{format_items('Interesting side signals', side_signals)}\n\n"
            f"{format_items('Remaining reads', remaining_reads)}\n\n"
            f"Existing voice-summary script:\n{audio_script or 'None'}"
        )
        return self._generate_json(
            system_instruction=(
                "Return JSON only. Write a compact editorial note for the top of a daily briefing. "
                "Be specific, restrained, and useful. Prefer synthesis over enumeration."
            ),
            operation_name="compose_editorial_note",
            prompt=prompt,
            schema_name="editorial_note",
            schema=EDITORIAL_NOTE_SCHEMA,
            max_output_tokens=EDITORIAL_NOTE_MAX_OUTPUT_TOKENS,
        )

    def _remote_newsletter_facts(self, newsletter: dict[str, Any]) -> dict[str, Any]:
        outbound_links = newsletter.get("outbound_links") or []
        link_preview = "\n".join(f"- {link}" for link in outbound_links[:12]) if outbound_links else "None"
        prompt = (
            "Split one newsletter email into several compact fast reads.\n"
            "Each fast read should capture one distinct fact, launch, paper, claim, or signal.\n"
            "Return between 2 and 6 facts, ordered by importance.\n"
            "Skip sponsor blocks, intros, sign-offs, social prompts, and inbox housekeeping.\n"
            "Each fact needs a short spoken-style headline, a concise summary, a compact "
            "why_it_matters line, a one-sentence whats_new note, a short caveat, 2 or 3 "
            "follow_up_questions, and relevant_links selected only from the available links list.\n"
            "Use an empty relevant_links array if none of the available links clearly match the fact.\n"
            "Write for a fast-scanning research inbox, not a full article.\n"
            "Do not invent facts that are absent from the email.\n"
            f"Newsletter source: {newsletter.get('source_name') or newsletter.get('sender')}\n"
            f"Email subject: {newsletter.get('subject')}\n"
            f"Sender: {newsletter.get('sender')}\n"
            f"Published at: {newsletter.get('published_at')}\n"
            f"Available links:\n{link_preview}\n\n"
            "Email text:\n"
            f"{str(newsletter.get('text_body') or '')[:16000]}"
        )
        return self._generate_json(
            system_instruction=(
                "Return JSON only. Break the newsletter into clean, distinct, high-signal "
                "fast reads. Keep each fact compact, editorial, and easy to scan."
            ),
            operation_name="split_newsletter_message",
            prompt=prompt,
            schema_name="newsletter_facts",
            schema=NEWSLETTER_FACT_SCHEMA,
            max_output_tokens=NEWSLETTER_FACTS_MAX_OUTPUT_TOKENS,
        )

    def _remote_zotero_tags(
        self,
        item: dict[str, Any],
        text: str,
        allowed_tags: list[str],
        insight: dict[str, Any],
    ) -> dict[str, Any]:
        prompt_parts = [
            "Choose high-signal Zotero tags for one research item.",
            "Pick only tags from the allowed list.",
            "Use as few tags as needed, prefer precision over recall, and avoid status/* or hook/* tags unless the evidence is unusually strong.",
            f"Item type: {item.get('content_type')}",
            f"Title: {item.get('title')}",
            f"Source: {item.get('source_name')}",
            f"Allowed tags: {', '.join(allowed_tags)}",
        ]
        if insight.get("short_summary"):
            prompt_parts.append(f"Summary: {insight['short_summary']}")
        if insight.get("why_it_matters"):
            prompt_parts.append(f"Why it matters: {insight['why_it_matters']}")
        if insight.get("method"):
            prompt_parts.append(f"Method note: {insight['method']}")
        if insight.get("result"):
            prompt_parts.append(f"Result note: {insight['result']}")
        prompt_parts.append(f"Source text:\n{text[:10000]}")
        payload = self._generate_json(
            system_instruction=(
                "Return JSON only. Select only evidence-backed tags from the allowed vocabulary. "
                "Do not invent tags or emit commentary."
            ),
            operation_name="suggest_zotero_tags",
            prompt="\n".join(prompt_parts),
            schema_name="zotero_tags",
            schema=build_tagging_schema(allowed_tags),
            max_output_tokens=ZOTERO_TAGS_MAX_OUTPUT_TOKENS,
        )
        return payload

    def _generate_json(
        self,
        *,
        system_instruction: str,
        operation_name: str,
        prompt: str,
        schema_name: str | None,
        schema: dict[str, Any],
        max_output_tokens: int,
    ) -> dict[str, Any]:
        estimated_cost_usd = self._estimate_request_max_cost_usd(
            system_instruction=system_instruction,
            prompt=prompt,
            schema=schema,
            max_output_tokens=max_output_tokens,
        )
        reservation = self.budget_service.reserve_estimated_cost(
            provider="gemini",
            operation=operation_name,
            estimated_cost_usd=estimated_cost_usd,
            metadata={
                "model": self.settings.gemini_model,
                "max_output_tokens": max_output_tokens,
            },
        )
        started_perf = perf_counter()
        invocation = self.invocation_recorder.begin(
            provider="gemini",
            model=self.settings.gemini_model,
            operation=operation_name,
            system_instruction=system_instruction,
            prompt=prompt,
            schema_name=schema_name,
            schema=schema,
            max_output_tokens=max_output_tokens,
            estimated_cost_usd=estimated_cost_usd,
            started_perf=started_perf,
        )
        usage: dict[str, int] | None = None
        provider_payload: Any = None
        response_text: str | None = None
        parsed_output: dict[str, Any] | None = None
        try:
            response = httpx.post(
                f"{self.settings.gemini_base_url}/models/{self.settings.gemini_model}:generateContent",
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": self.settings.gemini_api_key or "",
                    "x-goog-api-client": "research-center/0.1",
                },
                json={
                    "systemInstruction": {"parts": [{"text": system_instruction}]},
                    "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "responseMimeType": "application/json",
                        "responseJsonSchema": schema,
                        "maxOutputTokens": max_output_tokens,
                    },
                },
                timeout=self.settings.gemini_timeout_seconds,
            )
            response.raise_for_status()
            provider_payload = response.json()
            usage = self._extract_usage(provider_payload)
            parts = (
                provider_payload.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [])
            )
            response_text = "\n".join(part.get("text", "") for part in parts if part.get("text")).strip()
            if not response_text:
                raise RuntimeError("Gemini returned no text content.")
            parsed_output = json.loads(response_text)
            if not isinstance(parsed_output, dict):
                raise RuntimeError("Gemini returned a non-object JSON payload.")
            actual_cost_usd = self._estimate_usage_cost_usd(usage)
            self.budget_service.consume_reservation(
                reservation,
                actual_cost_usd=actual_cost_usd,
            )
            reservation = None
            trace = self.invocation_recorder.complete_success(
                invocation,
                completed_at=datetime.now(UTC),
                duration_ms=self._duration_ms(started_perf),
                usage=usage,
                actual_cost_usd=actual_cost_usd,
                response_text=response_text,
                parsed_output=parsed_output,
                provider_payload=provider_payload,
            )
            return self._attach_trace_metadata(
                parsed_output,
                usage=usage,
                trace=trace.model_dump(mode="json"),
            )
        except Exception as exc:
            actual_cost_usd = self._estimate_usage_cost_usd(usage) if usage else None
            if reservation is not None:
                if actual_cost_usd is not None:
                    self.budget_service.consume_reservation(
                        reservation,
                        actual_cost_usd=actual_cost_usd,
                    )
                else:
                    self.budget_service.release_reservation(reservation)
            trace = self.invocation_recorder.complete_failure(
                invocation,
                completed_at=datetime.now(UTC),
                duration_ms=self._duration_ms(started_perf),
                usage=usage,
                actual_cost_usd=actual_cost_usd,
                response_text=response_text,
                parsed_output=parsed_output,
                provider_payload=provider_payload,
                error=exc,
            )
            exc.ai_trace = trace.model_dump(mode="json")
            raise

    def _extract_usage(self, payload: dict[str, Any]) -> dict[str, int] | None:
        usage_payload = payload.get("usageMetadata")
        if not isinstance(usage_payload, dict):
            return None

        prompt_tokens = self._read_usage_int(usage_payload.get("promptTokenCount"))
        completion_tokens = self._read_usage_int(usage_payload.get("candidatesTokenCount"))
        total_tokens = self._read_usage_int(usage_payload.get("totalTokenCount"))
        if total_tokens == 0:
            total_tokens = prompt_tokens + completion_tokens
        if prompt_tokens == 0 and completion_tokens == 0 and total_tokens == 0:
            return None
        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }

    def _extract_ollama_usage(self, payload: dict[str, Any]) -> dict[str, int] | None:
        prompt_tokens = self._read_usage_int(payload.get("prompt_eval_count"))
        completion_tokens = self._read_usage_int(payload.get("eval_count"))
        total_tokens = prompt_tokens + completion_tokens
        if prompt_tokens == 0 and completion_tokens == 0:
            return None
        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }

    def _estimate_request_max_cost_usd(
        self,
        *,
        system_instruction: str,
        prompt: str,
        schema: dict[str, Any],
        max_output_tokens: int,
    ) -> float:
        prompt_tokens = (
            len(system_instruction.encode("utf-8"))
            + len(prompt.encode("utf-8"))
            + len(
                json.dumps(schema, sort_keys=True, separators=(",", ":")).encode("utf-8")
            )
            + LLM_REQUEST_TOKEN_BUFFER
        )
        return self._estimate_cost_usd(
            prompt_tokens=prompt_tokens,
            completion_tokens=max_output_tokens,
            total_tokens=prompt_tokens + max_output_tokens,
        )

    def _estimate_usage_cost_usd(self, usage: dict[str, int] | None) -> float:
        normalized_usage = usage or {}
        prompt_tokens = self._read_usage_int(normalized_usage.get("prompt_tokens"))
        completion_tokens = self._read_usage_int(normalized_usage.get("completion_tokens"))
        total_tokens = self._read_usage_int(normalized_usage.get("total_tokens"))
        if total_tokens == 0:
            total_tokens = prompt_tokens + completion_tokens
        return self._estimate_cost_usd(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        )

    def _estimate_cost_usd(
        self,
        *,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
    ) -> float:
        cost = 0.0
        accounted_tokens = 0
        if prompt_tokens or completion_tokens:
            cost += (
                prompt_tokens / 1_000_000
            ) * self.settings.llm_input_token_cost_per_million_usd + (
                completion_tokens / 1_000_000
            ) * self.settings.llm_output_token_cost_per_million_usd
            accounted_tokens = prompt_tokens + completion_tokens
        remaining_total_tokens = max(total_tokens - accounted_tokens, 0)
        if remaining_total_tokens:
            cost += (
                remaining_total_tokens / 1_000_000
            ) * self.settings.llm_total_token_cost_per_million_usd
        return round(cost, 6)

    def _log_budget_fallback(self, operation_name: str, exc: AIBudgetExceededError) -> None:
        record_llm_fallback(operation=operation_name, reason="budget_exceeded")
        logger.warning(
            "llm.remote_budget_limited",
            extra={
                "operation": operation_name,
                "model": self.settings.gemini_model,
                "reason": str(exc),
            },
        )

    def _read_usage_int(self, value: Any) -> int:
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return 0

    def _duration_ms(self, started_perf: float) -> int:
        return max(0, int(round((perf_counter() - started_perf) * 1000)))

    def _attach_trace_metadata(
        self,
        payload: dict[str, Any],
        *,
        usage: dict[str, int] | None,
        trace: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized = dict(payload)
        if usage:
            normalized["_usage"] = usage
        if trace:
            normalized["_trace"] = trace
        return normalized

    def _generate_ollama_json(
        self,
        *,
        operation_name: str,
        prompt: str,
        schema_name: str,
        schema: dict[str, Any],
        temperature: float,
        error_prefix: str,
    ) -> tuple[dict[str, Any], dict[str, int] | None, dict[str, Any]]:
        status = self.ollama_status()
        if not status.get("available"):
            raise RuntimeError(str(status.get("detail") or "Ollama is unavailable."))

        request_prompt = (
            f"{prompt}\n\n"
            "<json_schema>\n"
            f"{json.dumps(schema, indent=2, sort_keys=True, ensure_ascii=True)}\n"
            "</json_schema>\n"
        )
        started_perf = perf_counter()
        invocation = self.invocation_recorder.begin(
            provider="ollama",
            model=self.settings.ollama_model,
            operation=operation_name,
            system_instruction=None,
            prompt=request_prompt,
            schema_name=schema_name,
            schema=schema,
            max_output_tokens=None,
            estimated_cost_usd=0.0,
            started_perf=started_perf,
        )
        provider_payload: Any = None
        response_text: str | None = None
        parsed: dict[str, Any] | None = None
        usage: dict[str, int] | None = None
        try:
            response = httpx.post(
                f"{self.settings.ollama_base_url.rstrip('/')}/api/generate",
                json={
                    "model": self.settings.ollama_model,
                    "prompt": request_prompt,
                    "stream": False,
                    "format": schema,
                    "options": {
                        "temperature": temperature,
                    },
                },
                timeout=self.settings.ollama_timeout_seconds,
            )
            response.raise_for_status()
            provider_payload = response.json()
            raw = provider_payload.get("response") if isinstance(provider_payload, dict) else None
            if not isinstance(raw, str) or not raw.strip():
                raise RuntimeError("Ollama returned no JSON payload.")
            response_text = raw
            loaded = json.loads(raw)
            if not isinstance(loaded, dict):
                raise RuntimeError("Ollama returned a JSON payload that was not an object.")
            parsed = loaded
            usage = self._extract_ollama_usage(provider_payload if isinstance(provider_payload, dict) else {})
            trace = self.invocation_recorder.complete_success(
                invocation,
                completed_at=datetime.now(UTC),
                duration_ms=self._duration_ms(started_perf),
                usage=usage,
                actual_cost_usd=0.0,
                response_text=response_text,
                parsed_output=parsed,
                provider_payload=provider_payload,
            )
        except Exception as exc:
            trace = self.invocation_recorder.complete_failure(
                invocation,
                completed_at=datetime.now(UTC),
                duration_ms=self._duration_ms(started_perf),
                usage=usage,
                actual_cost_usd=0.0 if usage is not None else None,
                response_text=response_text,
                parsed_output=parsed,
                provider_payload=provider_payload,
                error=exc,
            )
            wrapped = RuntimeError(f"{error_prefix}: {exc}")
            wrapped.ai_trace = trace.model_dump(mode="json")
            raise wrapped from exc
        return parsed, usage, trace.model_dump(mode="json")

    def _fallback_trace_from_exception(self, exc: Exception) -> dict[str, Any] | None:
        trace = getattr(exc, "ai_trace", None)
        return trace if isinstance(trace, dict) else None

    def _normalize_allowed_tags(self, allowed_tags: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for tag in allowed_tags:
            cleaned = str(tag).strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            normalized.append(cleaned)
        return normalized

    def _normalize_suggested_tags(self, selected_tags: Any, allowed_tags: list[str]) -> list[str]:
        if not isinstance(selected_tags, list):
            return []
        normalized = [tag.strip() for tag in selected_tags if isinstance(tag, str) and tag.strip()]
        return self._prune_zotero_tags(normalized, allowed_tags)

    def _prune_zotero_tags(self, tags: list[str], allowed_tags: list[str]) -> list[str]:
        allowed = set(allowed_tags)
        counts: dict[str, int] = {}
        pruned: list[str] = []
        for tag in tags:
            if tag not in allowed or tag in pruned:
                continue
            prefix = tag.split("/", 1)[0]
            if counts.get(prefix, 0) >= TAG_PREFIX_LIMITS.get(prefix, 2):
                continue
            pruned.append(tag)
            counts[prefix] = counts.get(prefix, 0) + 1
            if len(pruned) >= TAGGING_MAX_TAGS:
                break
        return pruned

    def _heuristic_zotero_tags(
        self,
        item: dict[str, Any],
        text: str,
        allowed_tags: list[str],
        insight: dict[str, Any],
    ) -> list[str]:
        parts = [
            str(item.get("title") or ""),
            str(item.get("source_name") or ""),
            str(item.get("content_type") or ""),
            str(insight.get("short_summary") or ""),
            str(insight.get("why_it_matters") or ""),
            str(insight.get("method") or ""),
            str(insight.get("result") or ""),
            text[:12000],
        ]
        haystack = " ".join(parts).lower()
        selected: list[str] = []
        allowed = set(allowed_tags)

        def add(tag: str) -> None:
            if tag in allowed and tag not in selected:
                selected.append(tag)

        for tag, keywords in TAG_KEYWORDS.items():
            if tag not in allowed:
                continue
            if any(keyword in haystack for keyword in keywords):
                add(tag)

        parameter_scale = self._parameter_scale_tag(haystack)
        if parameter_scale:
            add(parameter_scale)

        if item.get("content_type") == "paper":
            if "type/survey" in allowed and "survey" in haystack:
                add("type/survey")
            elif "type/benchmark" in allowed and "benchmark" in haystack:
                add("type/benchmark")
            elif "type/theory" in allowed and any(term in haystack for term in ("theorem", "proof", "bound", "convergence")):
                add("type/theory")
            elif "type/empirical" in allowed:
                add("type/empirical")

        if "status/speculative" in allowed and any(term in haystack for term in ("hypothesis", "speculative", "open question")):
            add("status/speculative")
        if "status/foundational" in allowed and any(term in haystack for term in ("foundational", "seminal", "canonical")):
            add("status/foundational")
        if "hook/experiment" in allowed and any(term in haystack for term in ("ablation", "experiment", "experimental setup")):
            add("hook/experiment")

        return self._prune_zotero_tags(selected, allowed_tags)

    def _heuristic_newsletter_facts(self, newsletter: dict[str, Any]) -> dict[str, Any]:
        subject = normalize_whitespace(newsletter.get("subject") or "Newsletter update")
        cleaned_lines: list[str] = []
        for raw_line in str(newsletter.get("text_body") or "").replace("\r", "\n").split("\n"):
            line = normalize_whitespace(URL_RE.sub("", raw_line))
            if len(line) < 24 or NEWSLETTER_NOISE_RE.search(line):
                continue
            cleaned_lines.append(line)

        sentence_source = " ".join(cleaned_lines)
        sentences = [
            sentence
            for sentence in SENTENCE_SPLIT_RE.split(sentence_source)
            if len(normalize_whitespace(sentence)) >= 28 and not NEWSLETTER_NOISE_RE.search(sentence)
        ]
        if not sentences:
            fallback = normalize_whitespace(newsletter.get("text_body") or subject)
            sentences = [fallback or subject]

        chunk_size = 2 if len(sentences) <= NEWSLETTER_FACT_LIMIT * 2 else 3
        facts: list[dict[str, str]] = []
        for start in range(0, len(sentences), chunk_size):
            chunk_sentences = sentences[start : start + chunk_size]
            if not chunk_sentences:
                continue
            summary = normalize_whitespace(" ".join(chunk_sentences))
            headline_source = subject if len(sentences) == 1 else chunk_sentences[0]
            headline = self._newsletter_headline_from_text(headline_source, fallback=subject)
            why_it_matters = compact_signal_note(
                "",
                title=headline,
                summary=summary,
                fallback_text=summary,
            ) or summary[:140]
            facts.append(
                self.expand_newsletter_fact(
                    newsletter,
                    {
                        "headline": headline,
                        "summary": summary[:420],
                        "why_it_matters": normalize_whitespace(why_it_matters)[:220],
                    },
                )
            )
            if len(facts) >= NEWSLETTER_FACT_LIMIT:
                break

        if not facts:
            facts.append(
                self.expand_newsletter_fact(
                    newsletter,
                    {
                        "headline": self._newsletter_headline_from_text(
                            subject,
                            fallback="Newsletter update",
                        ),
                        "summary": subject,
                        "why_it_matters": normalize_whitespace(subject)[:220] or "newsletter update",
                    },
                )
            )
        return {"facts": facts}

    def _heuristic_audio_brief(self, digest: dict[str, Any]) -> dict[str, Any]:
        shortlisted_items = digest.get("shortlisted_items", [])
        spoken_date = self._spoken_brief_date(digest.get("brief_date"))
        editorial_note = self._normalize_brief_block(
            digest.get("editorial_note"),
            fallback="The strongest items are already grouped into an ordered shortlist.",
        )
        follow_ups = [
            question
            for question in digest.get("suggested_follow_ups", [])
            if isinstance(question, str) and question.strip()
        ]
        intro = self._normalize_brief_block(
            (
                f"Good morning. Here's your calm research briefing"
                f"{f' for {spoken_date}' if spoken_date else ''}. "
                f"{editorial_note}"
            ),
            fallback="Good morning. Here's your calm research briefing.",
        )
        outro = self._normalize_brief_block(
            (
                "That is the morning briefing. As you get into the day, keep "
                f"this question in mind: {follow_ups[0]}"
                if follow_ups
                else (
                    "That is the morning briefing. Start with the lead item if you "
                    "want the strongest next read."
                )
            ),
            fallback="That is the morning briefing.",
        )
        return {
            "intro": intro,
            "outro": outro,
            "chapters": [self._heuristic_audio_chapter(item) for item in shortlisted_items],
        }

    def _heuristic_editorial_note(self, digest: dict[str, Any]) -> dict[str, Any]:
        fallback_note = self._normalize_editorial_note_text(
            digest.get("fallback_note"),
            fallback="A small cluster of higher-signal items leads this edition.",
        )
        if fallback_note:
            return {"note": fallback_note}

        audio_script = self._truncate_context_block(digest.get("audio_script"), limit=500)
        if audio_script:
            return {
                "note": self._normalize_editorial_note_text(
                    audio_script,
                    fallback="A small cluster of higher-signal items leads this edition.",
                )
            }

        shortlist = digest.get("editorial_shortlist") or []
        if shortlist:
            titles = [
                normalize_item_title(str(item.get("title") or "Untitled item"), content_type=item.get("content_type"))
                for item in shortlist[:2]
            ]
            joined_titles = ", ".join(title for title in titles if title)
            if joined_titles:
                return {
                    "note": self._normalize_editorial_note_text(
                        f"The day centers on {joined_titles}.",
                        fallback="A small cluster of higher-signal items leads this edition.",
                    )
                }

        return {"note": fallback_note}

    def _normalize_audio_brief_chapters(
        self,
        chapters: Any,
        shortlisted_items: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        remote_chapters: dict[str, dict[str, str]] = {}
        if isinstance(chapters, list):
            for chapter in chapters:
                if not isinstance(chapter, dict):
                    continue
                item_id = str(chapter.get("item_id") or "").strip()
                headline = str(chapter.get("headline") or "").strip()
                narration = str(chapter.get("narration") or "").strip()
                if not item_id or not headline or not narration:
                    continue
                remote_chapters[item_id] = {
                    "item_id": item_id,
                    "headline": headline,
                    "narration": narration,
                }
        normalized: list[dict[str, str]] = []
        for item in shortlisted_items:
            item_id = str(item.get("item_id") or "").strip()
            if not item_id:
                continue
            if item_id in remote_chapters:
                normalized.append(remote_chapters[item_id])
            else:
                normalized.append(self._heuristic_audio_chapter(item))
        return normalized

    def _normalize_newsletter_facts(
        self,
        facts: Any,
        *,
        fallback: list[dict[str, Any]],
        newsletter: dict[str, Any],
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        if isinstance(facts, list):
            for fact in facts:
                if not isinstance(fact, dict):
                    continue
                expanded = self.expand_newsletter_fact(newsletter, fact)
                if not expanded.get("headline") or not expanded.get("summary"):
                    continue
                normalized.append(expanded)
                if len(normalized) >= NEWSLETTER_FACT_LIMIT:
                    break
        return normalized or fallback[:NEWSLETTER_FACT_LIMIT]

    def expand_newsletter_fact(self, newsletter: dict[str, Any], fact: dict[str, Any]) -> dict[str, Any]:
        subject = normalize_whitespace(newsletter.get("subject") or "Newsletter update")
        headline = self._newsletter_headline_from_text(
            str(fact.get("headline") or ""),
            fallback=subject or "Newsletter update",
        )
        summary = self._normalize_brief_block(
            str(fact.get("summary") or ""),
            fallback=headline,
        )
        available_links = self._normalize_newsletter_links(
            newsletter.get("outbound_links"),
            limit=50,
        )
        heuristic = self._heuristic_summary(
            {
                "title": headline,
                "source_name": str(newsletter.get("source_name") or newsletter.get("sender") or "Newsletter"),
                "content_type": "newsletter",
            },
            self._newsletter_fact_context_text(
                summary=summary,
                why_it_matters=str(fact.get("why_it_matters") or ""),
                links=available_links,
            ),
        )
        why_it_matters = normalize_whitespace(fact.get("why_it_matters") or "")
        if not why_it_matters:
            why_it_matters = compact_signal_note(
                "",
                title=headline,
                summary=summary,
                fallback_text=summary,
            ) or str(heuristic.get("why_it_matters") or headline)
        relevant_links = self._normalize_newsletter_links(
            fact.get("relevant_links") or fact.get("outbound_links"),
            allowed=available_links,
        )
        if not relevant_links:
            relevant_links = self._select_newsletter_fact_links(
                headline=headline,
                summary=summary,
                available_links=available_links,
            )
        return {
            "headline": headline,
            "summary": summary[:420],
            "why_it_matters": why_it_matters[:220],
            "whats_new": self._normalize_brief_block(
                fact.get("whats_new"),
                fallback=summary,
            ),
            "caveats": self._normalize_brief_block(
                fact.get("caveats"),
                fallback=str(
                    heuristic.get("caveats")
                    or "Check the linked source before acting."
                ),
            ),
            "follow_up_questions": self._normalize_newsletter_questions(
                fact.get("follow_up_questions"),
                fallback=heuristic.get("follow_up_questions"),
            ),
            "relevant_links": relevant_links,
        }

    def _newsletter_fact_context_text(
        self,
        *,
        summary: str,
        why_it_matters: str,
        links: list[str],
    ) -> str:
        parts = [summary]
        normalized_why = normalize_whitespace(why_it_matters)
        if normalized_why:
            parts.append(f"Why it matters: {normalized_why}")
        if links:
            parts.append("Links: " + " ".join(links[:NEWSLETTER_FACT_LINK_LIMIT]))
        return "\n".join(parts)

    def _normalize_newsletter_questions(self, value: Any, *, fallback: Any) -> list[str]:
        raw_questions: list[str] = []
        for candidate in [value, fallback]:
            if isinstance(candidate, str) and candidate.strip():
                raw_questions.append(candidate)
            elif isinstance(candidate, list):
                raw_questions.extend(str(entry) for entry in candidate if str(entry).strip())
            if raw_questions:
                break
        normalized: list[str] = []
        for raw_question in raw_questions:
            cleaned = self._strip_terminal_punctuation(normalize_whitespace(raw_question))
            if not cleaned:
                continue
            question = f"{cleaned[:220]}?"
            if question in normalized:
                continue
            normalized.append(question)
            if len(normalized) >= 3:
                break
        return normalized

    def _normalize_newsletter_links(
        self,
        value: Any,
        *,
        allowed: list[str] | None = None,
        limit: int = NEWSLETTER_FACT_LINK_LIMIT,
    ) -> list[str]:
        allowed_set = set(allowed or [])
        raw_links = value if isinstance(value, list) else [value] if value else []
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_link in raw_links:
            link = str(raw_link or "").strip()
            if not link or not link.startswith(("http://", "https://")):
                continue
            if allowed is not None and link not in allowed_set:
                continue
            if link in seen:
                continue
            seen.add(link)
            normalized.append(link)
            if len(normalized) >= limit:
                break
        return normalized

    def _select_newsletter_fact_links(
        self,
        *,
        headline: str,
        summary: str,
        available_links: list[str],
    ) -> list[str]:
        if not available_links:
            return []
        keywords = self._newsletter_link_keywords(headline=headline, summary=summary)
        if not keywords:
            return available_links[:NEWSLETTER_FACT_LINK_LIMIT]
        scored_links = []
        for index, link in enumerate(available_links):
            haystack = link.lower()
            score = sum(1 for keyword in keywords if keyword in haystack)
            if score <= 0:
                continue
            scored_links.append((-score, index, link))
        if not scored_links:
            return available_links[:NEWSLETTER_FACT_LINK_LIMIT]
        scored_links.sort()
        return [link for _, _, link in scored_links[:NEWSLETTER_FACT_LINK_LIMIT]]

    def _newsletter_link_keywords(self, *, headline: str, summary: str) -> list[str]:
        keywords: list[str] = []
        seen: set[str] = set()
        for token in NEWSLETTER_LINK_TOKEN_RE.findall(f"{headline} {summary}".lower()):
            if token in NEWSLETTER_LINK_STOPWORDS or token.isdigit() or token in seen:
                continue
            seen.add(token)
            keywords.append(token)
        return keywords

    def _heuristic_audio_chapter(self, item: dict[str, Any]) -> dict[str, str]:
        title = normalize_item_title(
            str(item.get("title") or "Untitled item"),
            content_type=item.get("content_type"),
        ).strip()
        source_name = str(item.get("source_name") or "Unknown source").strip()
        note = str(item.get("note") or "").strip()
        short_summary = str(item.get("short_summary") or "").strip()
        why_it_matters = str(item.get("why_it_matters") or "").strip()
        whats_new = str(item.get("whats_new") or "").strip()
        caveats = str(item.get("caveats") or "").strip()
        follow_up_questions = item.get("follow_up_questions") or []
        follow_up = (
            str(follow_up_questions[0]).strip()
            if isinstance(follow_up_questions, list) and follow_up_questions
            else ""
        )
        section = str(item.get("section") or "").strip()
        style = AUDIO_SECTION_BRIEF_STYLE.get(
            section,
            {
                "headline_prefix": "Next up",
                "lead_in": "This is worth your attention today",
            },
        )
        primary = self._normalize_brief_block(
            note or why_it_matters or short_summary or title,
            fallback=title,
        )
        secondary = ""
        if short_summary and (
            (note and short_summary.lower() != note.lower()) or short_summary.lower() != primary.lower()
        ):
            secondary = self._normalize_brief_block(short_summary, fallback="")
        whats_new_block = ""
        if whats_new and whats_new.lower() not in {primary.lower(), secondary.lower()}:
            whats_new_block = self._normalize_brief_block(whats_new, fallback="")
        caveat_block = self._normalize_brief_block(caveats, fallback="") if caveats else ""
        follow_up_block = self._normalize_brief_block(follow_up, fallback="") if follow_up else ""
        narration_parts = [self._normalize_brief_block(style["lead_in"], fallback="")]
        primary_fragment = self._strip_terminal_punctuation(primary)
        if primary_fragment:
            narration_parts.append(f"What matters most is this: {primary_fragment}.")
        secondary_fragment = self._strip_terminal_punctuation(secondary)
        if secondary_fragment:
            narration_parts.append(f"In short: {secondary_fragment}.")
        whats_new_fragment = self._strip_terminal_punctuation(whats_new_block)
        if whats_new_fragment:
            narration_parts.append(f"The clearest new angle is this: {whats_new_fragment}.")
        caveat_fragment = self._strip_terminal_punctuation(caveat_block)
        if caveat_fragment:
            narration_parts.append(f"Keep one caveat in mind: {caveat_fragment}.")
        follow_up_fragment = self._strip_terminal_punctuation(follow_up_block)
        if follow_up_fragment:
            narration_parts.append(f"A useful next question is this: {follow_up_fragment}?")
        if source_name and source_name.lower() != "unknown source":
            narration_parts.append(f"It comes via {source_name}.")
        narration = " ".join(part for part in narration_parts if part).strip()
        return {
            "item_id": str(item.get("item_id") or "").strip(),
            "headline": f"{style['headline_prefix']}: {title}",
            "narration": narration or title,
        }

    def _parameter_scale_tag(self, haystack: str) -> str | None:
        trillion_matches = [float(match.group(1)) for match in SCALE_TRILLION_RE.finditer(haystack)]
        if trillion_matches:
            return "scale/1t"
        billion_matches = [float(match.group(1)) for match in SCALE_BILLION_RE.finditer(haystack)]
        if not billion_matches:
            return None
        maximum = max(billion_matches)
        if maximum >= 100:
            return "scale/100b"
        if maximum >= 10:
            return "scale/10b"
        if maximum >= 1:
            return "scale/1b"
        return None

    def _heuristic_summary(self, item: dict[str, Any], text: str) -> dict[str, Any]:
        lines = [segment.strip() for segment in text.replace("\r", "\n").split("\n") if segment.strip()]
        condensed = " ".join(lines)[:800]
        primary_sentence = condensed.split(". ")[0][:260]
        secondary_sentence = condensed.split(". ")[1][:240] if ". " in condensed else primary_sentence
        is_paper = item.get("content_type") == "paper"
        title = str(item.get("title") or "").strip()

        return {
            "short_summary": primary_sentence or item.get("title"),
            "why_it_matters": compact_signal_note(
                "",
                title=title,
                summary=primary_sentence,
                fallback_text=condensed,
            ),
            "whats_new": secondary_sentence or "The item introduces a fresh signal relative to the rest of the feed.",
            "caveats": "The current summary is abstract-first and should be validated against the source before acting.",
            "follow_up_questions": [
                "What claim here is genuinely new versus reframed from prior work?",
                "Which baseline, comparator, or prior paper should be checked next?",
                "What evidence would change the current interpretation?",
            ],
            "contribution": primary_sentence if is_paper else None,
            "method": "Method details should be verified in the source text or abstract." if is_paper else None,
            "result": "The main reported result needs closer comparison against related work." if is_paper else None,
            "limitation": "The current view may miss limitations because only partial text is available." if is_paper else None,
            "possible_extension": "Translate the core claim into one concrete follow-up experiment or reading thread."
            if is_paper
            else None,
            "generated_at": datetime.now().isoformat(),
        }

    def _normalize_item_context(self, item: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(item)
        normalized["title"] = normalize_item_title(
            str(item.get("title") or "Untitled item"),
            content_type=item.get("content_type"),
        )
        return normalized

    def _normalize_summary_payload(self, item: dict[str, Any], payload: dict[str, Any], text: str) -> dict[str, Any]:
        normalized = dict(payload)
        normalized["why_it_matters"] = compact_signal_note(
            str(payload.get("why_it_matters") or ""),
            title=str(item.get("title") or ""),
            summary=str(payload.get("short_summary") or ""),
            fallback_text=text[:600],
        )
        return normalized

    def _normalize_brief_block(self, value: Any, *, fallback: str) -> str:
        if isinstance(value, str):
            cleaned = re.sub(r"\s+", " ", value).strip()
            if cleaned:
                return cleaned if cleaned.endswith((".", "!", "?")) else f"{cleaned}."
        fallback_cleaned = re.sub(r"\s+", " ", fallback).strip()
        if not fallback_cleaned:
            return ""
        return fallback_cleaned if fallback_cleaned.endswith((".", "!", "?")) else f"{fallback_cleaned}."

    def _normalize_editorial_note_text(self, value: Any, *, fallback: str) -> str:
        cleaned = normalize_whitespace(value if isinstance(value, str) else "")
        if not cleaned:
            cleaned = normalize_whitespace(fallback)
        if not cleaned:
            return ""

        sentences = [segment.strip() for segment in SENTENCE_SPLIT_RE.split(cleaned) if segment.strip()]
        if sentences:
            cleaned = " ".join(sentences[:2])
        words = cleaned.split()
        if len(words) > 55:
            cleaned = " ".join(words[:55]).rstrip(",;:")
        cleaned = cleaned.strip()
        if cleaned and cleaned[-1] not in ".!?":
            cleaned = f"{cleaned}."
        return cleaned

    def _truncate_context_block(self, value: Any, *, limit: int) -> str:
        cleaned = normalize_whitespace(value if isinstance(value, str) else "")
        if not cleaned:
            return ""
        if len(cleaned) <= limit:
            return cleaned
        return cleaned[: max(limit - 3, 1)].rstrip() + "..."

    def _spoken_brief_date(self, value: Any) -> str:
        parsed: date | None = None
        if isinstance(value, datetime):
            parsed = value.date()
        elif isinstance(value, date):
            parsed = value
        elif isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                try:
                    parsed = date.fromisoformat(cleaned)
                except ValueError:
                    try:
                        parsed = datetime.fromisoformat(cleaned).date()
                    except ValueError:
                        return cleaned
        if not parsed:
            return ""
        return f"{parsed.strftime('%B')} {parsed.day}, {parsed.year}"

    def _strip_terminal_punctuation(self, value: str) -> str:
        return re.sub(r"[.!?]+$", "", value).strip()

    def _newsletter_headline_from_text(self, value: str, *, fallback: str) -> str:
        cleaned = self._strip_terminal_punctuation(normalize_whitespace(value))
        if not cleaned:
            cleaned = self._strip_terminal_punctuation(normalize_whitespace(fallback))
        if not cleaned:
            return "Newsletter update"
        words = cleaned.split()
        if len(words) > 10:
            cleaned = " ".join(words[:10]).rstrip(",;:")
            cleaned = f"{cleaned}..."
        return cleaned[:120]
