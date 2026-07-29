"""Observability + lineage HTTP endpoints."""

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult

_TS = datetime(2026, 1, 10, tzinfo=UTC)
_ENTITY = {"user_id": "u1", "application_id": "a1"}


def _key():
    return EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])


def _client_with_value():
    backend = build_memory_backend()
    backend.offline.append("user_credit_risk", 1, FeatureResult(
        ref=FeatureRef("declared_income", 1), entity_key=_key(), value=5000,
        data_ts=_TS, calc_ts=_TS, max_input_data_ts=_TS,
        input_fingerprint="sha256:fp", value_hash="sha256:vh",
        bundle_digest="sha256:bundle",
    ))
    backend.metrics.incr("online_requests_total", {"outcome": "ok"})
    return TestClient(create_app(backend))


def test_metrics_endpoint_returns_bounded_json():
    client = _client_with_value()
    resp = client.get("/v1/observability/metrics")
    assert resp.status_code == 200
    body = resp.json()
    assert "metrics" in body and "generated_at" in body
    assert set(body["metrics"]) == {"counters", "gauges", "histograms"}
    assert body["metrics"]["counters"]["online_requests_total{outcome=ok}"] == 1


def test_lineage_endpoint_returns_values_free_metadata():
    client = _client_with_value()
    resp = client.post("/v1/lineage/feature-value", json={
        "view": "user_credit_risk", "view_version": 1,
        "feature_name": "declared_income", "feature_version": 1,
        "entity": _ENTITY,
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["found"] is True
    assert body["value_hash"] == "sha256:vh"
    assert body["bundle_digest"] == "sha256:bundle"
    assert body["report_refs"] == []
    assert "source_report_refs_not_available" in body["gaps"]
    # No value / storage path leaks.
    assert "5000" not in resp.text
    assert "storage_uri" not in resp.text


def test_lineage_endpoint_missing_value_is_explicit():
    client = _client_with_value()
    resp = client.post("/v1/lineage/feature-value", json={
        "view": "user_credit_risk", "view_version": 1,
        "feature_name": "declared_income", "feature_version": 1,
        "entity": {"user_id": "nobody", "application_id": "x"},
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["found"] is False
    assert "feature_value_not_found" in body["gaps"]
