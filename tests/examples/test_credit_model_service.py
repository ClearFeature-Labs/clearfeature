"""demo-model-service : shared artifact, controlled failures, no store deps.

All Docker-free: the Feature API is stubbed through the service's injectable
``fetch_latest`` seam; feature vectors come from the REAL ComputeCore over the
deterministic seed-7 golden population (same clients as the earlier goldens).
"""

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from examples.credit_decision_demo.features import MODEL_FEATURES, build_registry_and_udfs
from examples.credit_decision_demo.generator import generate_population
from examples.credit_decision_demo.model_lib import (
    artifact_bytes,
    compute_client_features,
    load_artifact,
    predict_proba,
)
from examples.credit_decision_demo.model_runner import DemoPdModelRunner
from examples.credit_decision_demo.model_service import (
    APPROVE_BELOW,
    REVIEW_BELOW,
    create_app,
    decide,
)
from fastapi.testclient import TestClient

from fintech_feature_platform.fs_core.compute.engine import ComputeCore

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "examples" / "credit_decision_demo"
_GOLDENS = json.loads((_DEMO / "fixtures" / "golden_features.json").read_text())

_DATA_TS = "2026-06-01T12:00:00+00:00"


def _model_vector(client) -> dict[str, float]:
    registry, udfs = build_registry_and_udfs()
    core = ComputeCore(registry, udfs)
    return compute_client_features(core, client, list(MODEL_FEATURES))


def _golden_clients():
    population = {c.user_id: c for c in generate_population(150, seed=7)}
    return {user_id: population[user_id] for user_id in _GOLDENS}


def _stub_fetch(vector: dict, missing: list[str] | None = None, data_ts: str = _DATA_TS):
    def fetch(payload: dict) -> dict:
        return {
            "view": payload["view"], "view_version": payload["view_version"],
            "entity_key": "stub", "missing": list(missing or []),
            "features": {
                name: {"feature_version": 1, "value": value,
                       "data_ts": data_ts, "calc_ts": data_ts}
                for name, value in vector.items()
            },
        }

    return fetch


def _client(vector: dict, include_features: bool = False, **kwargs) -> TestClient:
    return TestClient(create_app(fetch_latest=_stub_fetch(vector, **kwargs),
                                 include_features=include_features))


def _decision_payload(**overrides) -> dict:
    payload = {"user_id": "user_000005", "application_id": "app_000005"}
    payload.update(overrides)
    return payload


# --- artifact / scorer identity -------------------------------------------------------


def test_registry_pin_matches_committed_artifact_digest():
    registry, _ = build_registry_and_udfs()
    view = next(v for v in registry.feature_views if v.name == "credit_decision")
    pd = next(f for f in view.features if f.name == "pd_score")
    assert pd.model.digest == DemoPdModelRunner().digest


def test_artifact_feature_order_matches_registry_deps():
    registry, _ = build_registry_and_udfs()
    view = next(v for v in registry.feature_views if v.name == "credit_decision")
    pd = next(f for f in view.features if f.name == "pd_score")
    order = DemoPdModelRunner().feature_order
    assert set(order) == {dep.feature for dep in pd.deps}
    assert order == list(MODEL_FEATURES)  # pinned: stable input ordering


def test_service_score_equals_batch_scorer_for_golden_clients():
    """Same artifact, same code path: endpoint pd == predict_proba, bit-for-bit."""
    artifact = load_artifact()
    for user_id, client_obj in _golden_clients().items():
        vector = _model_vector(client_obj)
        expected = predict_proba(
            artifact, [float(vector[name]) for name in artifact["feature_order"]]
        )
        response = _client(vector).post(
            "/v1/credit/decision",
            json={"user_id": user_id, "application_id": _GOLDENS[user_id]["application_id"]},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["pd_score"] == expected == _GOLDENS[user_id]["pd_score"]
        assert body["model_digest"] == DemoPdModelRunner().digest
        assert body["decision"] == decide(expected)


def test_feature_order_comes_from_artifact_not_response_order():
    """A permuted /latest response must not change the score."""
    client_obj = next(iter(_golden_clients().values()))
    vector = _model_vector(client_obj)
    reversed_vector = dict(reversed(list(vector.items())))
    a = _client(vector).post("/v1/credit/decision", json=_decision_payload())
    b = _client(reversed_vector).post("/v1/credit/decision", json=_decision_payload())
    assert a.json()["pd_score"] == b.json()["pd_score"]


# --- decision policy ------------------------------------------------------------------


def test_decision_thresholds():
    assert decide(0.0) == "approve"
    assert decide(APPROVE_BELOW - 1e-9) == "approve"
    assert decide(APPROVE_BELOW) == "review"
    assert decide(REVIEW_BELOW - 1e-9) == "review"
    assert decide(REVIEW_BELOW) == "decline"
    assert decide(1.0) == "decline"


# --- endpoints ------------------------------------------------------------------------


def test_health_is_minimal():
    """ policy: public health returns only {"status": "ok"}."""
    client_obj = next(iter(_golden_clients().values()))
    response = _client(_model_vector(client_obj)).get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_valid_decision_request_contract():
    client_obj = next(iter(_golden_clients().values()))
    vector = _model_vector(client_obj)
    observation = datetime(2026, 6, 2, tzinfo=UTC)
    response = _client(vector, include_features=True).post(
        "/v1/credit/decision",
        json=_decision_payload(observation_ts=observation.isoformat()),
    )
    assert response.status_code == 200
    body = response.json()
    for field in ("request_id", "user_id", "application_id", "pd_score", "decision",
                  "model_name", "model_version", "model_digest", "feature_view",
                  "feature_view_version", "observation_ts"):
        assert field in body, field
    assert body["model_name"] == "credit_pd_demo"
    assert body["model_version"] == "1"
    assert body["observation_ts"] == observation.isoformat()
    assert set(body["features"]) == set(MODEL_FEATURES)
    # No raw report payloads leak through the response.
    assert "reports" not in body and "payload" not in json.dumps(body)


def test_feature_vector_hidden_by_default():
    """: the synthetic vector is demo-only, off unless explicitly enabled."""
    client_obj = next(iter(_golden_clients().values()))
    response = _client(_model_vector(client_obj)).post(
        "/v1/credit/decision", json=_decision_payload()
    )
    assert response.status_code == 200
    assert "features" not in response.json()


def test_invalid_request_is_controlled_422():
    client_obj = next(iter(_golden_clients().values()))
    response = _client(_model_vector(client_obj)).post(
        "/v1/credit/decision", json={"user_id": "u"}  # application_id missing
    )
    assert response.status_code == 422


def test_naive_observation_ts_rejected():
    client_obj = next(iter(_golden_clients().values()))
    response = _client(_model_vector(client_obj)).post(
        "/v1/credit/decision",
        json=_decision_payload(observation_ts="2026-06-02T00:00:00"),
    )
    assert response.status_code == 422


# --- controlled failure paths ---------------------------------------------------------


def test_missing_required_feature_is_explicit_409():
    client_obj = next(iter(_golden_clients().values()))
    vector = _model_vector(client_obj)
    vector.pop("bureau_score")
    response = _client(vector, missing=["bureau_score"]).post(
        "/v1/credit/decision", json=_decision_payload()
    )
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["status"] == "missing_features"
    assert detail["missing"] == ["bureau_score"]


def test_stale_features_are_explicit_409():
    client_obj = next(iter(_golden_clients().values()))
    vector = _model_vector(client_obj)
    observation = datetime.fromisoformat(_DATA_TS) + timedelta(days=30)
    response = _client(vector).post(
        "/v1/credit/decision",
        json=_decision_payload(observation_ts=observation.isoformat(),
                               max_feature_age_seconds=3600),
    )
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["status"] == "stale_features"
    assert set(detail["stale"]) == set(MODEL_FEATURES)


def test_feature_api_unavailable_is_controlled_502():
    def broken_fetch(payload: dict) -> dict:
        raise OSError("connection refused")

    client = TestClient(create_app(fetch_latest=broken_fetch))
    response = client.post("/v1/credit/decision", json=_decision_payload())
    assert response.status_code == 502
    assert response.json()["detail"]["status"] == "feature_api_unavailable"


def test_artifact_digest_mismatch_fails_startup(tmp_path):
    artifact = load_artifact()
    artifact["bias"] = artifact["bias"] + 0.5  # tampered weights -> different digest
    tampered = tmp_path / "artifact.json"
    tampered.write_bytes(artifact_bytes(artifact))
    with pytest.raises(RuntimeError, match="digest mismatch"):
        create_app(artifact_path=tampered, fetch_latest=lambda payload: {})


# --- dependency boundary --------------------------------------------------------------


def test_model_service_has_no_direct_store_or_broker_imports():
    """The service depends on the Feature API contract only."""
    source = (_DEMO / "model_service.py").read_text(encoding="utf-8")
    forbidden = ("psycopg", "redis", "valkey", "minio", "kafka", "confluent",
                 "fs_core.stores", "fs_core.raw", "api.backend", "api.local_backend")
    imports = [
        node for node in ast.walk(ast.parse(source))
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]
    names = []
    for node in imports:
        if isinstance(node, ast.ImportFrom):
            names.append(node.module or "")
        else:
            names.extend(alias.name for alias in node.names)
    for name in names:
        assert not any(bad in name for bad in forbidden), name
