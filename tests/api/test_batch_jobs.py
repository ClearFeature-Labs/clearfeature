"""Tests for POST/GET /v1/batch/jobs (Kafka-first batch job API)."""

from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.fs_core.events.topics import FEATURE_COMPUTE_BATCH


def _client(backend=None) -> TestClient:
    return TestClient(create_app(backend))


def _item(user_id="u1", income=100_000) -> dict:
    return {
        "entity_type": "application",
        "entity_key": {"user_id": user_id, "application_id": "A1"},
        "inline_sources": {
            "credit_report": {
                "report_type": "credit_report",
                "report_ts": "2026-01-01T00:00:00Z",
                "payload": {"declared_income": income, "monthly_obligations": 700_000},
            }
        },
    }


def _body(items=None, **overrides) -> dict:
    body = {
        "view": "user_credit_risk",
        "view_version": 1,
        "requested_features": ["declared_income"],
        "scope": {"type": "inline", "items": items if items is not None else [_item()]},
        "chunk_size": 100,
        "idempotency_key": "batch-1",
    }
    body.update(overrides)
    return body


def test_accepts_valid_job_and_publishes_chunks():
    backend = build_memory_backend()
    items = [_item(f"u{i}") for i in range(5)]
    resp = _client(backend).post("/v1/batch/jobs", json=_body(items=items, chunk_size=2))
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["job_id"] == "batch-1"
    assert data["total_items"] == 5
    assert data["chunk_count"] == 3  # 2 + 2 + 1
    published = [r for r in backend.events.published if r.topic == FEATURE_COMPUTE_BATCH]
    assert len(published) == 3
    # deterministic chunk ids
    assert published[0].event.chunk_id == "batch-1:0"
    assert published[-1].event.chunk_id == "batch-1:2"
    assert published[-1].event.chunk_count == 3
    # total_items carried on every chunk event (for the durable job snapshot)
    assert published[0].event.total_items == 5


def test_requires_idempotency_key():
    body = _body()
    del body["idempotency_key"]
    assert _client().post("/v1/batch/jobs", json=body).status_code == 422


def test_rejects_unknown_view_version():
    backend = build_memory_backend()
    resp = _client(backend).post("/v1/batch/jobs", json=_body(view_version=2))
    assert resp.status_code == 400
    assert backend.events.published == []


def test_rejects_unknown_feature():
    backend = build_memory_backend()
    resp = _client(backend).post(
        "/v1/batch/jobs", json=_body(requested_features=["nope"])
    )
    assert resp.status_code == 400
    assert backend.events.published == []


def test_rejects_empty_scope():
    backend = build_memory_backend()
    resp = _client(backend).post("/v1/batch/jobs", json=_body(items=[]))
    assert resp.status_code == 400
    assert backend.events.published == []


def test_rejects_unsupported_scope_type():
    backend = build_memory_backend()
    body = _body()
    body["scope"]["type"] = "report_refs"
    resp = _client(backend).post("/v1/batch/jobs", json=body)
    assert resp.status_code == 400
    assert backend.events.published == []


def test_rejects_chunk_size_over_cap(monkeypatch):
    monkeypatch.setenv("FSP_BATCH_MAX_CHUNK_SIZE", "2")
    backend = build_memory_backend()
    resp = _client(backend).post("/v1/batch/jobs", json=_body(chunk_size=3))
    assert resp.status_code == 400


def test_rejects_total_items_over_cap(monkeypatch):
    monkeypatch.setenv("FSP_BATCH_MAX_ITEMS", "2")
    backend = build_memory_backend()
    items = [_item(f"u{i}") for i in range(3)]
    resp = _client(backend).post("/v1/batch/jobs", json=_body(items=items))
    assert resp.status_code == 400


def test_partial_publish_failure_marks_publish_failed():
    class _FlakyPublisher:
        def __init__(self):
            self.calls = 0
            self.published = []

        def publish(self, topic, key, event):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("kafka down")
            self.published.append(event)

    import dataclasses

    backend = dataclasses.replace(build_memory_backend(), events=_FlakyPublisher())
    items = [_item(f"u{i}") for i in range(3)]
    resp = _client(backend).post("/v1/batch/jobs", json=_body(items=items, chunk_size=1))
    assert resp.status_code == 503
    job = backend.batch_status.get("batch-1")
    assert job.status == "publish_failed"
    assert job.error_summary["failed_chunk_index"] == 1


def test_get_batch_job_returns_status():
    backend = build_memory_backend()
    _client(backend).post("/v1/batch/jobs", json=_body())
    resp = _client(backend).get("/v1/batch/jobs/batch-1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["job_id"] == "batch-1"
    assert data["status"] == "accepted"
    assert data["chunk_count"] == 1


def test_get_unknown_job_returns_404():
    assert _client().get("/v1/batch/jobs/nope").status_code == 404


def test_status_does_not_expose_values_or_payloads():
    backend = build_memory_backend()
    _client(backend).post("/v1/batch/jobs", json=_body())
    resp = _client(backend).get("/v1/batch/jobs/batch-1")
    assert "100000" not in resp.text  # no payload values in status
    assert "payload" not in resp.text
