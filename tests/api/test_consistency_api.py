from datetime import UTC, datetime

from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult

_ENTITY = {"user_id": "1", "application_id": "A1"}
_TS = datetime(2026, 6, 23, 10, tzinfo=UTC)


def _client(backend=None) -> TestClient:
    return TestClient(create_app(backend))


def _seed_online_and_offline(backend, name="declared_income", value=4_000_000, version=1):
    entity_key = EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])
    result = FeatureResult(
        ref=FeatureRef(name, version),
        entity_key=entity_key,
        value=value,
        data_ts=_TS,
        calc_ts=_TS,
    )
    backend.online.write("user_credit_risk", 1, result)
    backend.offline.append("user_credit_risk", 1, result)


def _check_body(**overrides) -> dict:
    body = {
        "view": "user_credit_risk",
        "view_version": 1,
        "entity": _ENTITY,
        "features": ["declared_income"],
    }
    body.update(overrides)
    return body


def test_consistency_ok_when_online_and_offline_match():
    backend = build_memory_backend()
    _seed_online_and_offline(backend)
    response = _client(backend).post(
        "/v1/features/consistency-check", json=_check_body()
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    item = data["checks"]["declared_income"]
    assert item["status"] == "ok"
    assert item["offline_value"] == 4_000_000
    assert item["online_value"] == 4_000_000
    assert item["offline_data_ts"] is not None
    assert item["online_data_ts"] is not None
    assert item["offline_feature_version"] == 1
    assert item["online_feature_version"] == 1


def test_consistency_never_computed_is_missing_both():
    response = _client().post("/v1/features/consistency-check", json=_check_body())
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "inconsistent"
    assert data["checks"]["declared_income"]["status"] == "missing_both"


def test_consistency_unknown_feature_returns_400():
    response = _client().post(
        "/v1/features/consistency-check", json=_check_body(features=["nope"])
    )
    assert response.status_code == 400


def test_consistency_missing_entity_key_field_returns_400():
    response = _client().post(
        "/v1/features/consistency-check", json=_check_body(entity={"user_id": "1"})
    )
    assert response.status_code == 400


def test_consistency_unknown_view_returns_400():
    response = _client().post(
        "/v1/features/consistency-check", json=_check_body(view="nope")
    )
    assert response.status_code == 400


def test_health_endpoint_ok():
    assert _client().get("/health").json() == {"status": "ok"}
