from __future__ import annotations

import logging
import math
import re
import secrets
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import perf_counter

logger = logging.getLogger(__name__)

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
DEFAULT_HTTP_REQUEST_DURATION_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)
DEFAULT_TASK_DURATION_BUCKETS = (
    0.01,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    300.0,
)
_UUID_SEGMENT_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_DATE_SEGMENT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_INT_SEGMENT_RE = re.compile(r"^\d+$")
_LONG_ID_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_-]{16,}$")
_EXPORTER_LOCK = threading.Lock()
_STARTED_EXPORTERS: dict[tuple[str, int, str], ThreadingHTTPServer] = {}


def normalize_metrics_path(path: str) -> str:
    if not path or path == "/":
        return "/"
    normalized_segments: list[str] = []
    for segment in path.split("/"):
        if not segment:
            continue
        if _UUID_SEGMENT_RE.match(segment):
            normalized_segments.append(":id")
        elif _DATE_SEGMENT_RE.match(segment):
            normalized_segments.append(":date")
        elif _INT_SEGMENT_RE.match(segment) or _LONG_ID_SEGMENT_RE.match(segment):
            normalized_segments.append(":id")
        else:
            normalized_segments.append(segment)
    return "/" + "/".join(normalized_segments)


def _format_help_text(value: str) -> str:
    return value.replace("\\", r"\\").replace("\n", r"\n")


def _format_label_value(value: str) -> str:
    return value.replace("\\", r"\\").replace("\n", r"\n").replace('"', r"\"")


def _format_sample_value(value: float | int) -> str:
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    return format(value, ".16g")


def _coerce_label_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _render_labels(
    label_names: tuple[str, ...],
    label_values: tuple[str, ...],
    *,
    extra_labels: dict[str, str] | None = None,
) -> str:
    labels = list(zip(label_names, label_values, strict=True))
    if extra_labels:
        labels.extend(extra_labels.items())
    if not labels:
        return ""
    rendered = ",".join(
        f'{name}="{_format_label_value(value)}"'
        for name, value in labels
    )
    return "{" + rendered + "}"


class _MetricFamily:
    def __init__(
        self,
        *,
        name: str,
        documentation: str,
        metric_type: str,
        label_names: tuple[str, ...] = (),
    ) -> None:
        self.name = name
        self.documentation = documentation
        self.metric_type = metric_type
        self.label_names = label_names
        self._lock = threading.Lock()

    def _label_values(self, labels: dict[str, object]) -> tuple[str, ...]:
        if set(labels) != set(self.label_names):
            expected = ", ".join(self.label_names) or "<none>"
            received = ", ".join(sorted(labels)) or "<none>"
            raise ValueError(
                f"Metric {self.name} expected labels [{expected}] but received [{received}]."
            )
        return tuple(_coerce_label_value(labels[name]) for name in self.label_names)

    def header_lines(self) -> list[str]:
        return [
            f"# HELP {self.name} {_format_help_text(self.documentation)}",
            f"# TYPE {self.name} {self.metric_type}",
        ]

    def reset(self) -> None:
        raise NotImplementedError

    def render(self) -> list[str]:
        raise NotImplementedError


class Counter(_MetricFamily):
    def __init__(
        self,
        *,
        name: str,
        documentation: str,
        label_names: tuple[str, ...] = (),
    ) -> None:
        super().__init__(
            name=name,
            documentation=documentation,
            metric_type="counter",
            label_names=label_names,
        )
        self._values: dict[tuple[str, ...], float] = {}

    def inc(self, amount: float = 1.0, **labels: object) -> None:
        if amount < 0:
            raise ValueError("Counters can only be incremented by non-negative amounts.")
        label_values = self._label_values(labels)
        with self._lock:
            self._values[label_values] = self._values.get(label_values, 0.0) + amount

    def reset(self) -> None:
        with self._lock:
            self._values.clear()

    def render(self) -> list[str]:
        with self._lock:
            items = sorted(self._values.items())
        lines = self.header_lines()
        for label_values, value in items:
            lines.append(
                f"{self.name}{_render_labels(self.label_names, label_values)} "
                f"{_format_sample_value(value)}"
            )
        return lines


class Gauge(_MetricFamily):
    def __init__(
        self,
        *,
        name: str,
        documentation: str,
        label_names: tuple[str, ...] = (),
    ) -> None:
        super().__init__(
            name=name,
            documentation=documentation,
            metric_type="gauge",
            label_names=label_names,
        )
        self._values: dict[tuple[str, ...], float] = {}

    def inc(self, amount: float = 1.0, **labels: object) -> None:
        label_values = self._label_values(labels)
        with self._lock:
            self._values[label_values] = self._values.get(label_values, 0.0) + amount

    def dec(self, amount: float = 1.0, **labels: object) -> None:
        self.inc(-amount, **labels)

    def set(self, amount: float, **labels: object) -> None:
        label_values = self._label_values(labels)
        with self._lock:
            self._values[label_values] = amount

    def reset(self) -> None:
        with self._lock:
            self._values.clear()

    def render(self) -> list[str]:
        with self._lock:
            items = sorted(self._values.items())
        lines = self.header_lines()
        for label_values, value in items:
            lines.append(
                f"{self.name}{_render_labels(self.label_names, label_values)} "
                f"{_format_sample_value(value)}"
            )
        return lines


@dataclass
class _HistogramState:
    bucket_counts: list[int]
    sample_count: int = 0
    sample_sum: float = 0.0


class Histogram(_MetricFamily):
    def __init__(
        self,
        *,
        name: str,
        documentation: str,
        buckets: tuple[float, ...],
        label_names: tuple[str, ...] = (),
    ) -> None:
        super().__init__(
            name=name,
            documentation=documentation,
            metric_type="histogram",
            label_names=label_names,
        )
        resolved_buckets = tuple(sorted(float(bucket) for bucket in buckets))
        if not resolved_buckets or not math.isinf(resolved_buckets[-1]):
            resolved_buckets = (*resolved_buckets, math.inf)
        self._buckets = resolved_buckets
        self._values: dict[tuple[str, ...], _HistogramState] = {}

    def observe(self, amount: float, **labels: object) -> None:
        label_values = self._label_values(labels)
        with self._lock:
            state = self._values.setdefault(
                label_values,
                _HistogramState(bucket_counts=[0] * len(self._buckets)),
            )
            for index, bucket in enumerate(self._buckets):
                if amount <= bucket:
                    state.bucket_counts[index] += 1
            state.sample_count += 1
            state.sample_sum += amount

    def reset(self) -> None:
        with self._lock:
            self._values.clear()

    def render(self) -> list[str]:
        with self._lock:
            items = sorted(self._values.items())
        lines = self.header_lines()
        for label_values, state in items:
            for bucket, bucket_count in zip(
                self._buckets,
                state.bucket_counts,
                strict=True,
            ):
                lines.append(
                    f"{self.name}_bucket"
                    f"{_render_labels(
                        self.label_names,
                        label_values,
                        extra_labels={'le': _format_sample_value(bucket)},
                    )} "
                    f"{bucket_count}"
                )
            labels = _render_labels(self.label_names, label_values)
            lines.append(f"{self.name}_sum{labels} {_format_sample_value(state.sample_sum)}")
            lines.append(f"{self.name}_count{labels} {state.sample_count}")
        return lines


class MetricsRegistry:
    def __init__(self) -> None:
        self._families: list[_MetricFamily] = []

    def counter(
        self,
        name: str,
        documentation: str,
        *,
        label_names: tuple[str, ...] = (),
    ) -> Counter:
        family = Counter(
            name=name,
            documentation=documentation,
            label_names=label_names,
        )
        self._families.append(family)
        return family

    def gauge(
        self,
        name: str,
        documentation: str,
        *,
        label_names: tuple[str, ...] = (),
    ) -> Gauge:
        family = Gauge(
            name=name,
            documentation=documentation,
            label_names=label_names,
        )
        self._families.append(family)
        return family

    def histogram(
        self,
        name: str,
        documentation: str,
        *,
        buckets: tuple[float, ...],
        label_names: tuple[str, ...] = (),
    ) -> Histogram:
        family = Histogram(
            name=name,
            documentation=documentation,
            buckets=buckets,
            label_names=label_names,
        )
        self._families.append(family)
        return family

    def reset(self) -> None:
        for family in self._families:
            family.reset()

    def render(self) -> str:
        lines: list[str] = []
        for family in self._families:
            rendered = family.render()
            if len(rendered) <= 2:
                continue
            lines.extend(rendered)
        return "\n".join(lines) + ("\n" if lines else "")


_REGISTRY = MetricsRegistry()
HTTP_REQUESTS_IN_PROGRESS = _REGISTRY.gauge(
    "research_center_http_requests_in_progress",
    "Number of API requests currently being handled by the backend.",
    label_names=("method", "path"),
)
HTTP_REQUESTS_TOTAL = _REGISTRY.counter(
    "research_center_http_requests_total",
    "Total number of completed API requests.",
    label_names=("method", "path", "status_code"),
)
HTTP_REQUEST_DURATION_SECONDS = _REGISTRY.histogram(
    "research_center_http_request_duration_seconds",
    "API request duration in seconds.",
    buckets=DEFAULT_HTTP_REQUEST_DURATION_BUCKETS,
    label_names=("method", "path", "status_code"),
)
AUTH_EVENTS_TOTAL = _REGISTRY.counter(
    "research_center_auth_events_total",
    "Authentication event totals.",
    label_names=("event",),
)
OPERATION_EVENTS_TOTAL = _REGISTRY.counter(
    "research_center_operation_events_total",
    "Administrative operation event totals.",
    label_names=("operation", "event", "execution_mode"),
)
TASKS_IN_PROGRESS = _REGISTRY.gauge(
    "research_center_tasks_in_progress",
    "Number of worker tasks currently executing.",
    label_names=("task",),
)
TASK_RUNS_TOTAL = _REGISTRY.counter(
    "research_center_task_runs_total",
    "Worker task execution totals.",
    label_names=("task", "outcome"),
)
TASK_DURATION_SECONDS = _REGISTRY.histogram(
    "research_center_task_duration_seconds",
    "Worker task duration in seconds.",
    buckets=DEFAULT_TASK_DURATION_BUCKETS,
    label_names=("task", "outcome"),
)


def reset_metrics() -> None:
    _REGISTRY.reset()


def render_metrics() -> str:
    return _REGISTRY.render()


def record_http_request_started(method: str, path: str) -> None:
    HTTP_REQUESTS_IN_PROGRESS.inc(method=method.upper(), path=path)


def record_http_request_finished(
    *,
    method: str,
    path: str,
    status_code: int,
    duration_seconds: float,
) -> None:
    normalized_method = method.upper()
    normalized_status = str(status_code)
    HTTP_REQUESTS_TOTAL.inc(
        method=normalized_method,
        path=path,
        status_code=normalized_status,
    )
    HTTP_REQUEST_DURATION_SECONDS.observe(
        duration_seconds,
        method=normalized_method,
        path=path,
        status_code=normalized_status,
    )
    HTTP_REQUESTS_IN_PROGRESS.dec(method=normalized_method, path=path)


def record_auth_event(event: str) -> None:
    AUTH_EVENTS_TOTAL.inc(event=event)


def record_operation_event(
    *,
    operation: str,
    event: str,
    execution_mode: str,
) -> None:
    OPERATION_EVENTS_TOTAL.inc(
        operation=operation,
        event=event,
        execution_mode=execution_mode,
    )


@contextmanager
def track_task_metrics(task_name: str) -> Iterator[Callable[[str], None]]:
    outcome = "success"
    TASKS_IN_PROGRESS.inc(task=task_name)

    def set_outcome(value: str) -> None:
        nonlocal outcome
        outcome = value

    started_at = perf_counter()
    try:
        yield set_outcome
    except Exception:
        outcome = "error"
        raise
    finally:
        duration_seconds = perf_counter() - started_at
        TASK_RUNS_TOTAL.inc(task=task_name, outcome=outcome)
        TASK_DURATION_SECONDS.observe(
            duration_seconds,
            task=task_name,
            outcome=outcome,
        )
        TASKS_IN_PROGRESS.dec(task=task_name)


def _request_token_matches(
    *,
    authorization_header: str | None,
    metrics_token_header: str | None,
    expected_token: str | None,
) -> bool:
    if not expected_token:
        return True
    provided_token = metrics_token_header
    if not provided_token and authorization_header:
        scheme, _, token = authorization_header.partition(" ")
        if scheme.lower() == "bearer":
            provided_token = token.strip() or None
    return secrets.compare_digest(provided_token or "", expected_token)


class _MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        request_path = self.path.split("?", maxsplit=1)[0]
        if request_path != self.server.metrics_path:
            self.send_error(404)
            return
        if not _request_token_matches(
            authorization_header=self.headers.get("Authorization"),
            metrics_token_header=self.headers.get("X-Metrics-Token"),
            expected_token=self.server.metrics_token,
        ):
            self.send_error(403)
            return
        payload = render_metrics().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", PROMETHEUS_CONTENT_TYPE)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        return None


class _MetricsHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        host: str,
        port: int,
        *,
        metrics_path: str,
        metrics_token: str | None,
    ) -> None:
        super().__init__((host, port), _MetricsHandler)
        self.metrics_path = metrics_path
        self.metrics_token = metrics_token


def start_metrics_http_server(
    *,
    host: str,
    port: int,
    path: str,
    token: str | None = None,
) -> bool:
    key = (host, port, path)
    with _EXPORTER_LOCK:
        if key in _STARTED_EXPORTERS:
            return False
        server = _MetricsHTTPServer(
            host,
            port,
            metrics_path=path,
            metrics_token=token,
        )
        thread = threading.Thread(
            target=server.serve_forever,
            name=f"metrics-exporter-{host}:{port}",
            daemon=True,
        )
        thread.start()
        _STARTED_EXPORTERS[key] = server
        return True
