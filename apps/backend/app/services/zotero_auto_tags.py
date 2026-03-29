from __future__ import annotations

from typing import Any

DEFAULT_ZOTERO_AUTO_TAG_VOCABULARY = [
    "area/llm",
    "area/vision",
    "area/multimodal",
    "area/rl",
    "area/alignment",
    "area/interpretability",
    "area/agents",
    "area/theory",
    "area/efficiency",
    "area/safety",
    "area/evals",
    "area/robotics",
    "area/systems",
    "area/data",
    "method/pretraining",
    "method/posttraining",
    "method/rhf",
    "method/rlaif",
    "method/dpo",
    "method/distillation",
    "method/moe",
    "method/sparsity",
    "method/quantization",
    "method/pruning",
    "method/retrieval",
    "method/tool_use",
    "method/self_improvement",
    "method/synthetic_data",
    "method/curriculum",
    "method/meta_learning",
    "method/contrastive",
    "method/scaling_laws",
    "arch/transformer",
    "arch/attention",
    "arch/moe",
    "arch/state_space",
    "arch/diffusion",
    "arch/gan",
    "arch/rl_policy",
    "arch/world_model",
    "arch/memory",
    "type/theory",
    "type/empirical",
    "type/benchmark",
    "type/survey",
    "type/framework",
    "type/negative_result",
    "type/reproduction",
    "type/scaling_study",
    "scale/1b",
    "scale/10b",
    "scale/100b",
    "scale/1t",
    "scale/compute_efficient",
    "scale/low_resource",
    "scale/edge",
    "data/webscale",
    "data/synthetic",
    "data/human_feedback",
    "data/self_play",
    "data/simulation",
    "data/curated",
    "data/filtered",
    "eval/reasoning",
    "eval/code",
    "eval/math",
    "eval/long_context",
    "eval/safety",
    "eval/robustness",
    "eval/ood",
    "eval/multilingual",
    "eval/efficiency",
    "status/skimming",
    "status/deep_read",
    "status/key",
    "status/foundational",
    "status/speculative",
    "status/high_impact",
    "hook/experiment",
    "hook/theory_gap",
    "hook/contradiction",
    "hook/scaling_question",
    "hook/replication_candidate",
]


def normalize_zotero_auto_tag_vocabulary(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for entry in value:
        if not isinstance(entry, str):
            continue
        tag = entry.strip()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        normalized.append(tag)
    return normalized


def resolve_zotero_auto_tag_vocabulary(metadata: dict[str, Any] | None) -> list[str]:
    metadata = metadata or {}
    if "auto_tag_vocabulary" in metadata:
        return normalize_zotero_auto_tag_vocabulary(metadata.get("auto_tag_vocabulary"))
    return list(DEFAULT_ZOTERO_AUTO_TAG_VOCABULARY)


def merge_zotero_tags(*tag_groups: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in tag_groups:
        for tag in group:
            normalized = tag.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            merged.append(normalized)
    return merged
