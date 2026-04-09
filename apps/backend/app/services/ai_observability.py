from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from app.core.logging import (
    current_log_context,
    current_request_id,
    current_task_id,
    current_task_name,
)
from app.core.metrics import record_llm_invocation
from app.vault.models import AITraceArtifact, AITraceReference, AITraceUsage
from app.vault.store import VaultStore

logger = logging.getLogger(__name__)
_MAX_LOG_ERROR_CHARS = 400


def _normalize_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        normalized = value if value.tzinfo else value.replace(tzinfo=UTC)
        return normalized.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {str(key): _normalize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_normalize_value(item) for item in value]
    return str(value)


def _normalize_context(value: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, Any] = {}
    for key, item in value.items():
        cleaned_key = str(key).strip()
        if not cleaned_key:
            continue
        normalized[cleaned_key] = _normalize_value(item)
    return normalized


def _safe_error_message(error: Exception | str | None) -> str | None:
    if error is None:
        return None
    rendered = str(error).strip()
    if not rendered:
        return None
    if len(rendered) <= _MAX_LOG_ERROR_CHARS:
        return rendered
    return rendered[: _MAX_LOG_ERROR_CHARS - 3].rstrip() + "..."


@dataclass(frozen=True)
class AIInvocation:
    trace_id: str
    provider: str
    model: str
    operation: str
    recorded_at: datetime
    started_perf: float
    system_instruction: str | None
    prompt: str
    schema_name: str | None
    schema: dict[str, Any] | None
    max_output_tokens: int | None
    prompt_sha256: str
    prompt_chars: int
    system_instruction_chars: int
    estimated_cost_usd: float | None
    context: dict[str, Any]


class AIInvocationRecorder:
    def __init__(self, *, store: VaultStore | None = None) -> None:
        self.store = store or VaultStore()

    def begin(
        self,
        *,
        provider: str,
        model: str,
        operation: str,
        system_instruction: str | None,
        prompt: str,
        schema_name: str | None,
        schema: dict[str, Any] | None,
        max_output_tokens: int | None,
        estimated_cost_usd: float | None,
        context: dict[str, Any] | None = None,
        started_perf: float,
        ) -> AIInvocation:
        prompt_hash = hashlib.sha256(
            json.dumps(
                {
                    "system_instruction": system_instruction or "",
                    "prompt": prompt,
                    "schema": schema or {},
                    "max_output_tokens": max_output_tokens,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return AIInvocation(
            trace_id=uuid4().hex,
            provider=provider,
            model=model,
            operation=operation,
            recorded_at=datetime.now(UTC),
            started_perf=started_perf,
            system_instruction=system_instruction,
            prompt=prompt,
            schema_name=schema_name,
            schema=schema,
            max_output_tokens=max_output_tokens,
            prompt_sha256=prompt_hash,
            prompt_chars=len(prompt),
            system_instruction_chars=len(system_instruction or ""),
            estimated_cost_usd=estimated_cost_usd,
            context=_normalize_context(
                {
                    **current_log_context(),
                    **(context or {}),
                }
            ),
        )

    def complete_success(
        self,
        invocation: AIInvocation,
        *,
        completed_at: datetime,
        duration_ms: int,
        usage: dict[str, int] | None,
        actual_cost_usd: float | None,
        response_text: str | None,
        parsed_output: Any,
        provider_payload: Any,
    ) -> AITraceReference:
        return self._complete(
            invocation,
            status="succeeded",
            completed_at=completed_at,
            duration_ms=duration_ms,
            usage=usage,
            actual_cost_usd=actual_cost_usd,
            response_text=response_text,
            parsed_output=parsed_output,
            provider_payload=provider_payload,
            error=None,
        )

    def complete_failure(
        self,
        invocation: AIInvocation,
        *,
        completed_at: datetime,
        duration_ms: int,
        usage: dict[str, int] | None,
        actual_cost_usd: float | None,
        response_text: str | None,
        parsed_output: Any,
        provider_payload: Any,
        error: Exception,
    ) -> AITraceReference:
        return self._complete(
            invocation,
            status="failed",
            completed_at=completed_at,
            duration_ms=duration_ms,
            usage=usage,
            actual_cost_usd=actual_cost_usd,
            response_text=response_text,
            parsed_output=parsed_output,
            provider_payload=provider_payload,
            error=error,
        )

    def _complete(
        self,
        invocation: AIInvocation,
        *,
        status: str,
        completed_at: datetime,
        duration_ms: int,
        usage: dict[str, int] | None,
        actual_cost_usd: float | None,
        response_text: str | None,
        parsed_output: Any,
        provider_payload: Any,
        error: Exception | None,
    ) -> AITraceReference:
        normalized_usage = self._normalize_usage(usage)
        failure_reason = error.__class__.__name__ if error is not None else None
        reference = AITraceReference(
            trace_id=invocation.trace_id,
            provider=invocation.provider,
            model=invocation.model,
            operation=invocation.operation,
            status=status,
            recorded_at=invocation.recorded_at,
            duration_ms=duration_ms,
            prompt_sha256=invocation.prompt_sha256,
            prompt_tokens=normalized_usage.prompt_tokens,
            completion_tokens=normalized_usage.completion_tokens,
            total_tokens=normalized_usage.total_tokens,
            cost_usd=round(float(actual_cost_usd or 0.0), 6),
            context=invocation.context,
            error=_safe_error_message(error),
        )
        try:
            artifact = AITraceArtifact(
                id=invocation.trace_id,
                recorded_at=invocation.recorded_at,
                completed_at=completed_at,
                provider=invocation.provider,
                model=invocation.model,
                operation=invocation.operation,
                status=status,
                duration_ms=duration_ms,
                request_id=current_request_id(),
                task_id=current_task_id(),
                task_name=current_task_name(),
                context=invocation.context,
                prompt_sha256=invocation.prompt_sha256,
                prompt_chars=invocation.prompt_chars,
                system_instruction_chars=invocation.system_instruction_chars,
                schema_name=invocation.schema_name,
                response_schema=invocation.schema,
                max_output_tokens=invocation.max_output_tokens,
                usage=normalized_usage,
                estimated_cost_usd=invocation.estimated_cost_usd,
                actual_cost_usd=round(float(actual_cost_usd or 0.0), 6) if actual_cost_usd is not None else None,
                response_text=response_text,
                parsed_output=parsed_output,
                provider_payload=provider_payload,
                error=str(error) if error is not None else None,
            )
            prompt_path, trace_path = self.store.write_ai_trace_bundle(
                artifact=artifact,
                prompt_markdown=self._render_prompt_markdown(invocation),
            )
            reference = reference.model_copy(
                update={
                    "prompt_path": str(prompt_path),
                    "trace_path": str(trace_path),
                }
            )
        except Exception as trace_exc:
            logger.exception(
                "ai.trace_persist.failed",
                extra={
                    "trace_id": invocation.trace_id,
                    "provider": invocation.provider,
                    "model": invocation.model,
                    "operation": invocation.operation,
                    "status": status,
                    "reason": _safe_error_message(trace_exc),
                },
            )

        duration_seconds = max(duration_ms, 0) / 1000
        record_llm_invocation(
            provider=invocation.provider,
            model=invocation.model,
            operation=invocation.operation,
            status="success" if status == "succeeded" else "error",
            duration_seconds=duration_seconds,
            usage=normalized_usage.model_dump(mode="json"),
            cost_usd=actual_cost_usd,
            failure_reason=failure_reason,
        )

        log_payload = {
            "trace_id": reference.trace_id,
            "provider": reference.provider,
            "model": reference.model,
            "operation": reference.operation,
            "status": reference.status,
            "duration_ms": reference.duration_ms,
            "prompt_sha256": reference.prompt_sha256,
            "prompt_tokens": reference.prompt_tokens,
            "completion_tokens": reference.completion_tokens,
            "total_tokens": reference.total_tokens,
            "cost_usd": reference.cost_usd,
            "prompt_path": reference.prompt_path,
            "trace_path": reference.trace_path,
            "error": reference.error,
        }
        active_context_keys = set(current_log_context())
        for key, value in reference.context.items():
            if key in active_context_keys or key in log_payload:
                continue
            log_payload[key] = value
        if status == "succeeded":
            logger.info("ai.invocation.completed", extra=log_payload)
        else:
            logger.warning("ai.invocation.failed", extra=log_payload)
        return reference

    def _render_prompt_markdown(self, invocation: AIInvocation) -> str:
        header_lines = [
            "# AI Invocation",
            "",
            f"- Trace ID: {invocation.trace_id}",
            f"- Provider: {invocation.provider}",
            f"- Model: {invocation.model}",
            f"- Operation: {invocation.operation}",
            f"- Recorded at: {invocation.recorded_at.isoformat()}",
            f"- Prompt SHA-256: {invocation.prompt_sha256}",
            f"- Max output tokens: {invocation.max_output_tokens if invocation.max_output_tokens is not None else 'n/a'}",
        ]
        if invocation.context:
            header_lines.extend(
                [
                    "",
                    "## Context",
                    "",
                    "```json",
                    json.dumps(invocation.context, indent=2, sort_keys=True, ensure_ascii=True),
                    "```",
                ]
            )
        if invocation.system_instruction:
            header_lines.extend(
                [
                    "",
                    "## System Instruction",
                    "",
                    invocation.system_instruction.rstrip(),
                ]
            )
        header_lines.extend(
            [
                "",
                "## Prompt",
                "",
                invocation.prompt.rstrip(),
            ]
        )
        if invocation.schema is not None:
            header_lines.extend(
                [
                    "",
                    "## Response Schema",
                    "",
                    "```json",
                    json.dumps(invocation.schema, indent=2, sort_keys=True, ensure_ascii=True),
                    "```",
                ]
            )
        return "\n".join(header_lines).rstrip() + "\n"

    @staticmethod
    def _normalize_usage(usage: dict[str, int] | None) -> AITraceUsage:
        normalized = usage or {}
        prompt_tokens = max(0, int(normalized.get("prompt_tokens", 0)))
        completion_tokens = max(0, int(normalized.get("completion_tokens", 0)))
        total_tokens = max(0, int(normalized.get("total_tokens", 0)))
        if total_tokens == 0:
            total_tokens = prompt_tokens + completion_tokens
        return AITraceUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        )
