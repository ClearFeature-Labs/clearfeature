"""Tests for /health, the read APIs (latest/history), and removed legacy routes.

The legacy report_ref subsystem (POST /v1/raw-reports, POST /v1/features/compute) was
removed in. Read-API tests seed the online/offline stores directly (no compute
route dependency) so they remain valid after /v1/features/compute-direct is removed too.
"""

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult

_ENTITY = {"user_id": "1", "application_id": "A1"}
_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)


def _entity_key() -> EntityKey:
    return EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])


def _client(backend=None) -> TestClient:
    return TestClient(create_app(backend))


def _write_online(backend, name="declared_income", value=3_500_000, version=1, ts=_TS):
    backend.online.write(
        "user_credit_risk",
        1,
        FeatureResult(
            ref=FeatureRef(name, version),
            entity_key=_entity_key(),
            value=value,
            data_ts=ts,
            calc_ts=ts,
        ),
    )


def _append_offline(backend, name="declared_income", value=3_500_000, version=1, ts=_TS):
    backend.offline.append(
        "user_credit_risk",
        1,
        FeatureResult(
            ref=FeatureRef(name, version),
            entity_key=_entity_key(),
            value=value,
            data_ts=ts,
            calc_ts=ts,
        ),
    )


def test_health():
    response = _client().get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# --- removed legacy routes  --------------------------------------

def test_raw_reports_route_removed_returns_404():
    resp = _client().post("/v1/raw-reports", json={"anything": 1})
    assert resp.status_code == 404


def test_features_compute_route_removed_returns_404():
    resp = _client().post("/v1/features/compute", json={"anything": 1})
    assert resp.status_code == 404


def test_features_compute_direct_route_removed_returns_404():
    resp = _client().post("/v1/features/compute-direct", json={"anything": 1})
    assert resp.status_code == 404


# --- /v1/features/latest ----------------------------------------------------

def _latest(**overrides) -> dict:
    body = {
        "view": "user_credit_risk",
        "view_version": 1,
        "entity": _ENTITY,
        "requested_features": ["declared_income"],
    }
    body.update(overrides)
    return body


def test_read_latest_returns_written_value():
    backend = build_memory_backend()
    _write_online(backend)
    response = _client(backend).post("/v1/features/latest", json=_latest())
    assert response.status_code == 200
    data = response.json()
    assert data["entity_key"] == "user_id=1|application_id=A1"
    assert data["missing"] == []
    feature = data["features"]["declared_income"]
    assert feature["feature_version"] == 1
    assert feature["value"] == 3_500_000
    assert feature["data_ts"] is not None
    assert feature["calc_ts"] is not None


def test_read_latest_reports_missing_feature():
    backend = build_memory_backend()
    _write_online(backend)  # only declared_income
    response = _client(backend).post(
        "/v1/features/latest",
        json=_latest(requested_features=["declared_income", "monthly_obligations"]),
    )
    assert response.status_code == 200
    data = response.json()
    assert "declared_income" in data["features"]
    assert "monthly_obligations" not in data["features"]
    assert data["missing"] == ["monthly_obligations"]


def test_read_latest_unknown_feature_returns_400():
    response = _client().post(
        "/v1/features/latest", json=_latest(requested_features=["does_not_exist"])
    )
    assert response.status_code == 400


def test_read_latest_missing_entity_key_field_returns_400():
    response = _client().post("/v1/features/latest", json=_latest(entity={"user_id": "1"}))
    assert response.status_code == 400


def test_read_latest_unknown_view_version_returns_400():
    response = _client().post("/v1/features/latest", json=_latest(view_version=2))
    assert response.status_code == 400


# --- /v1/features/history ---------------------------------------------------

def _history(**overrides) -> dict:
    body = {"view": "user_credit_risk", "view_version": 1, "entity": _ENTITY}
    body.update(overrides)
    return body


def test_read_history_returns_appended_record():
    backend = build_memory_backend()
    _append_offline(backend)
    response = _client(backend).post("/v1/features/history", json=_history())
    assert response.status_code == 200
    data = response.json()
    assert data["entity_key"] == "user_id=1|application_id=A1"
    assert len(data["records"]) == 1
    record = data["records"][0]
    assert record["view"] == "user_credit_risk"
    assert record["view_version"] == 1
    assert record["feature_name"] == "declared_income"
    assert record["feature_version"] == 1
    assert record["value"] == 3_500_000


def test_history_appends_across_repeated_writes():
    backend = build_memory_backend()
    _append_offline(backend, ts=datetime(2024, 8, 26, 10, tzinfo=UTC))
    _append_offline(backend, ts=datetime(2024, 8, 27, 10, tzinfo=UTC))
    response = _client(backend).post(
        "/v1/features/history", json=_history(feature_name="declared_income")
    )
    assert response.status_code == 200
    assert len(response.json()["records"]) == 2


def test_history_filter_by_feature_name():
    backend = build_memory_backend()
    _append_offline(backend, name="declared_income", value=3_500_000)
    _append_offline(backend, name="monthly_obligations", value=700_000)
    response = _client(backend).post(
        "/v1/features/history", json=_history(feature_name="declared_income")
    )
    assert response.status_code == 200
    records = response.json()["records"]
    assert len(records) == 1
    assert records[0]["feature_name"] == "declared_income"


def test_history_filter_by_feature_version():
    backend = build_memory_backend()
    _append_offline(backend, name="declared_income", version=1)
    present = _client(backend).post(
        "/v1/features/history",
        json=_history(feature_name="declared_income", feature_version=1),
    )
    absent = _client(backend).post(
        "/v1/features/history",
        json=_history(feature_name="declared_income", feature_version=2),
    )
    assert len(present.json()["records"]) == 1
    assert absent.json()["records"] == []


def test_history_known_feature_no_records_returns_empty():
    backend = build_memory_backend()
    _append_offline(backend, name="declared_income")
    response = _client(backend).post(
        "/v1/features/history", json=_history(feature_name="monthly_obligations")
    )
    assert response.status_code == 200
    assert response.json()["records"] == []


def test_history_unknown_feature_returns_400():
    response = _client().post(
        "/v1/features/history", json=_history(feature_name="does_not_exist")
    )
    assert response.status_code == 400


def test_history_missing_entity_key_field_returns_400():
    response = _client().post(
        "/v1/features/history", json=_history(entity={"user_id": "1"})
    )
    assert response.status_code == 400


def test_history_unknown_view_returns_400():
    response = _client().post("/v1/features/history", json=_history(view="nope"))
    assert response.status_code == 400


def test_history_empty_returns_200_with_empty_records():
    response = _client().post("/v1/features/history", json=_history())
    assert response.status_code == 200
    assert response.json()["records"] == []
