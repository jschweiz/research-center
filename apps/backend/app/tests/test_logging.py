import json
import logging

from app.core.logging import JsonLogFormatter


def test_json_log_formatter_serializes_context_and_extra_fields() -> None:
    formatter = JsonLogFormatter()
    record = logging.makeLogRecord(
        {
            "name": "app.test",
            "levelno": logging.INFO,
            "levelname": "INFO",
            "msg": "request.completed",
            "request_id": "req-123",
            "path": "/api/test",
            "status_code": 200,
        }
    )

    payload = json.loads(formatter.format(record))

    assert payload["message"] == "request.completed"
    assert payload["request_id"] == "req-123"
    assert payload["path"] == "/api/test"
    assert payload["status_code"] == 200
    assert payload["level"] == "INFO"
    assert payload["logger"] == "app.test"


def test_api_request_logs_include_request_id(authenticated_client, caplog) -> None:
    caplog.set_level(logging.INFO)
    caplog.clear()

    response = authenticated_client.get(
        "/api/ops/ingestion-runs",
        headers={"x-request-id": "req-123"},
    )

    assert response.status_code == 200
    assert response.headers["x-request-id"] == "req-123"

    started = [
        record
        for record in caplog.records
        if record.getMessage() == "request.started"
        and getattr(record, "path", None) == "/api/ops/ingestion-runs"
    ]
    completed = [
        record
        for record in caplog.records
        if record.getMessage() == "request.completed"
        and getattr(record, "path", None) == "/api/ops/ingestion-runs"
    ]

    assert started
    assert completed
    assert completed[-1].request_id == "req-123"
    assert completed[-1].method == "GET"
    assert completed[-1].status_code == 200
    assert isinstance(completed[-1].duration_ms, float)


def test_inline_purge_task_logs_completion(client, monkeypatch, caplog) -> None:
    from app.tasks.jobs import purge_raw_email_payloads_task

    monkeypatch.setattr(
        "app.tasks.jobs.IngestionService.purge_old_email_payloads",
        lambda self: 3,
    )
    caplog.set_level(logging.INFO)
    caplog.clear()

    result = purge_raw_email_payloads_task.run()

    assert result == 3
    assert any(
        record.getMessage() == "task.raw_email_payload_purge.started"
        for record in caplog.records
    )
    completed = [
        record
        for record in caplog.records
        if record.getMessage() == "task.raw_email_payload_purge.completed"
    ]
    assert completed
    assert completed[-1].purged_count == 3
