from datetime import UTC, datetime

from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult

_ENTITY = {"user_id": "1", "application_id": "A1"}


def _client(backend=None) -> TestClient:
    return TestClient(create_app(backend))


def _seed_offline(backend, *, income=4_000_000, data_ts=datetime(2026, 6, 21, 10, tzinfo=UTC)):
    # PIT reads append-only offline history; the record's data_ts drives point-in-time.
    entity_key = EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])
    backend.offline.append(
        "user_credit_risk",
        1,
        FeatureResult(
            ref=FeatureRef("declared_income", 1),
            entity_key=entity_key,
            value=income,
            data_ts=data_ts,
            calc_ts=data_ts,
        ),
    )


def _build_body(**overrides) -> dict:
    body = {
        "view": "user_credit_risk",
        "view_version": 1,
        "features": ["declared_income"],
        "observations": [
            {
                "entity": _ENTITY,
                "observation_ts": "2026-06-21T12:00:00Z",
                "context": {"request_id": "req_1", "segment": "new_client"},
            }
        ],
        "missing_policy": "keep_null",
    }
    body.update(overrides)
    return body


def test_build_after_offline_seed():
    backend = build_memory_backend()
    _seed_offline(backend)
    response = _client(backend).post("/v1/training-datasets/build", json=_build_body())
    assert response.status_code == 200
    data = response.json()
    row = data["rows"][0]
    # DataFrame-friendly: flat features + separate metadata.
    assert row["features"]["declared_income"] == 4_000_000
    assert row["context"]["request_id"] == "req_1"
    assert row["entity"] == _ENTITY
    assert "label" not in row  # PIT builder returns feature data only, no semantic label
    meta = row["feature_metadata"]["declared_income"]
    assert meta["status"] == "ok"
    assert meta["feature_version"] == 1
    assert meta["data_ts"] is not None
    assert data["summary"] == {
        "rows": 1, "features": 1, "missing_values": 0, "future_records_ignored": 0,
        "safety_gap_seconds": 0,
    }
    assert "storage_uri" not in response.text
    assert "label" not in response.text


def test_future_observation_excludes_feature():
    backend = build_memory_backend()
    _seed_offline(backend, data_ts=datetime(2026, 6, 21, 10, tzinfo=UTC))
    # Observation before the only record -> the record is in the future -> excluded.
    body = _build_body()
    body["observations"][0]["observation_ts"] = "2026-06-20T00:00:00Z"
    data = _client(backend).post("/v1/training-datasets/build", json=body).json()
    assert data["rows"][0]["features"]["declared_income"] is None
    assert data["rows"][0]["feature_metadata"]["declared_income"]["status"] == "missing"
    assert data["summary"]["future_records_ignored"] == 1


def test_missing_policy_keep_null_for_never_computed():
    data = _client().post("/v1/training-datasets/build", json=_build_body()).json()
    assert data["rows"][0]["features"]["declared_income"] is None
    assert data["summary"]["missing_values"] == 1


def test_missing_policy_error_returns_400():
    response = _client().post(
        "/v1/training-datasets/build", json=_build_body(missing_policy="error")
    )
    assert response.status_code == 400


def test_unknown_feature_returns_400():
    response = _client().post(
        "/v1/training-datasets/build", json=_build_body(features=["nope"])
    )
    assert response.status_code == 400


def test_unknown_view_returns_400():
    response = _client().post(
        "/v1/training-datasets/build", json=_build_body(view="nope")
    )
    assert response.status_code == 400


def test_missing_entity_key_returns_400():
    body = _build_body()
    body["observations"][0]["entity"] = {"user_id": "1"}
    response = _client().post("/v1/training-datasets/build", json=body)
    assert response.status_code == 400


def test_naive_observation_ts_returns_422():
    body = _build_body()
    body["observations"][0]["observation_ts"] = "2026-06-21T12:00:00"
    response = _client().post("/v1/training-datasets/build", json=body)
    assert response.status_code == 422


def test_safety_gap_seconds_applied_and_echoed():
    backend = build_memory_backend()
    # data_ts 12:00, observation 12:00 -> a 1h (3600s) safety_gap excludes it.
    _seed_offline(backend, data_ts=datetime(2026, 6, 21, 12, tzinfo=UTC))
    body = _build_body(safety_gap_seconds=3600)
    body["observations"][0]["observation_ts"] = "2026-06-21T12:00:00Z"
    data = _client(backend).post("/v1/training-datasets/build", json=body).json()
    assert data["rows"][0]["features"]["declared_income"] is None  # excluded by gap
    assert data["summary"]["safety_gap_seconds"] == 3600


def test_safety_gap_default_zero_preserves_behavior():
    backend = build_memory_backend()
    _seed_offline(backend, data_ts=datetime(2026, 6, 21, 10, tzinfo=UTC))
    data = _client(backend).post(
        "/v1/training-datasets/build", json=_build_body()
    ).json()
    assert data["rows"][0]["features"]["declared_income"] == 4_000_000
    assert data["summary"]["safety_gap_seconds"] == 0


def test_negative_safety_gap_seconds_returns_422():
    response = _client().post(
        "/v1/training-datasets/build", json=_build_body(safety_gap_seconds=-1)
    )
    assert response.status_code == 422


def test_health_endpoint_ok():
    assert _client().get("/health").json() == {"status": "ok"}
