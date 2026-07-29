"""Tests for POST /v1/model-scores (Kafka-first model score writeback)."""

from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.fs_core.events.topics import MODEL_SCORE_WRITE

_ENTITY = {"user_id": "1", "application_id": "A1"}


def _client(backend=None) -> TestClient:
    return TestClient(create_app(backend))


def _body(**overrides) -> dict:
    body = {
        "entity_type": "application",
        "entity_key": _ENTITY,
        "view": "user_credit_risk",
        "view_version": 1,
        "idempotency_key": "score-key-1",
        "scores": [
            {
                "feature": "pd_score",
                "value": 0.037,
                "data_ts": "2026-01-01T12:00:00Z",
                "model_name": "pd_model",
                "model_version": "v4",
                "source_request_id": "freq_abc",
            }
        ],
    }
    body.update(overrides)
    return body


def test_accepts_valid_write_and_publishes_event():
    backend = build_memory_backend()
    resp = _client(backend).post("/v1/model-scores", json=_body())
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["score_write_id"] == "score-key-1"
    assert len(backend.events.published) == 1
    record = backend.events.published[0]
    assert record.topic == MODEL_SCORE_WRITE
    event = record.event
    assert event.event_type == "model_score.write.requested"
    assert event.score_write_id == "score-key-1"
    assert event.scores[0].feature == "pd_score"


def test_requires_idempotency_key():
    body = _body()
    del body["idempotency_key"]
    resp = _client().post("/v1/model-scores", json=body)
    assert resp.status_code == 422  # schema-required field


def test_rejects_unknown_view_version():
    backend = build_memory_backend()
    resp = _client(backend).post("/v1/model-scores", json=_body(view_version=2))
    assert resp.status_code == 400
    assert backend.events.published == []


def test_rejects_unknown_feature():
    backend = build_memory_backend()
    body = _body(scores=[{**_body()["scores"][0], "feature": "nope"}])
    resp = _client(backend).post("/v1/model-scores", json=body)
    assert resp.status_code == 400
    assert backend.events.published == []


def test_rejects_writing_to_udf_feature():
    backend = build_memory_backend()
    body = _body(scores=[{**_body()["scores"][0], "feature": "declared_income"}])
    resp = _client(backend).post("/v1/model-scores", json=body)
    assert resp.status_code == 400
    assert "not a model_score feature" in resp.text
    assert backend.events.published == []


def test_rejects_missing_data_ts():
    score = {**_body()["scores"][0]}
    del score["data_ts"]
    resp = _client().post("/v1/model-scores", json=_body(scores=[score]))
    assert resp.status_code == 422


def test_rejects_missing_model_name_or_version():
    score = {**_body()["scores"][0]}
    del score["model_version"]
    resp = _client().post("/v1/model-scores", json=_body(scores=[score]))
    assert resp.status_code == 422


def test_rejects_both_write_flags_false():
    backend = build_memory_backend()
    resp = _client(backend).post(
        "/v1/model-scores", json=_body(write_online=False, write_offline=False)
    )
    assert resp.status_code == 400
    assert backend.events.published == []


def test_does_not_write_stores_synchronously():
    backend = build_memory_backend()
    from fintech_feature_platform.fs_core.models import EntityKey

    _client(backend).post("/v1/model-scores", json=_body())
    entity_key = EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])
    # accepted boundary is Kafka publish only; no online/offline write here
    assert backend.online.get("user_credit_risk", 1, entity_key, "pd_score", 1) is None
    assert backend.offline.get(entity_key) == []


def test_response_does_not_expose_internal_pointers():
    resp = _client().post("/v1/model-scores", json=_body())
    assert "object_key" not in resp.text
    assert "storage_uri" not in resp.text


def test_compute_request_for_model_score_feature_is_rejected():
    # a feature-requests compute targeting a model_score feature -> 400 (planner rejects)
    backend = build_memory_backend()
    body = {
        "entity_type": "application",
        "entity_key": _ENTITY,
        "view": "user_credit_risk",
        "view_version": 1,
        "requested_features": ["pd_score"],
        "reports": [
            {
                "source_name": "credit_report",
                "report_type": "credit_report",
                "report_ts": "2026-06-27T10:00:00Z",
                "payload": {"declared_income": 1},
            }
        ],
    }
    resp = _client(backend).post("/v1/feature-requests", json=body)
    assert resp.status_code == 400
    assert backend.events.published == []
