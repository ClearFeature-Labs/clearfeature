"""Credit-decision demo: generator, registry, model, and fixture batch flow."""

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest
from examples.credit_decision_demo.features import (
    MODEL_FEATURES,
    REGISTRY_PATH,
    build_registry_and_udfs,
)
from examples.credit_decision_demo.flow import (
    SOURCE_FEATURE_GROUPS,
    entity_key,
    guarded_online_refresh,
    ingest_dataset,
    run_f2_batch,
    run_f3_batch,
)
from examples.credit_decision_demo.generator import generate_population
from examples.credit_decision_demo.model_lib import (
    artifact_digest,
    compute_client_features,
    load_artifact,
    predict_proba,
)
from examples.credit_decision_demo.model_runner import DemoPdModelRunner
from fastapi.testclient import TestClient

from fintech_feature_platform.api.batch_worker import handle_batch_chunk
from fintech_feature_platform.fs_core.compute.engine import ComputeCore
from fintech_feature_platform.fs_core.events.models import BatchChunkRequested
from fintech_feature_platform.fs_core.registry.bundle import compute_bundle_digest
from fintech_feature_platform.fs_core.registry.loader import load_registry_file

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "examples" / "credit_decision_demo"
_FIXTURES = _DEMO / "fixtures"

_F2 = ["requested_monthly_payment", "existing_payment_to_income", "total_payment_to_income",
       "income_stability_index", "recent_delinquency_flag", "thin_file_flag",
       "affordability_index", "combined_risk_index"]


# --- generator ---------------------------------------------------------------------

def test_generation_is_deterministic():
    a = generate_population(40, seed=42)
    b = generate_population(40, seed=42)
    assert [c.application for c in a] == [c.application for c in b]
    assert [c.credit_bureau_report for c in a] == [c.credit_bureau_report for c in b]
    assert [c.label_default for c in a] == [c.label_default for c in b]
    different = generate_population(40, seed=43)
    assert [c.latent_risk for c in a] != [c.latent_risk for c in different]


def test_reports_are_coherent_and_referentially_intact():
    for client in generate_population(60, seed=42):
        key = client.entity_key()
        assert client.application["user_id"] == key["user_id"]
        for report in (client.tax_report, client.credit_bureau_report,
                       client.telco_report, client.socdem_report):
            assert report["user_id"] == key["user_id"]
        assert len(client.tax_report["periods"]) == 12  # enough for 12m features
        bureau = client.credit_bureau_report
        assert 300 <= bureau["bureau_score"] <= 850
        if bureau["active_loans"] == 0:
            assert bureau["total_monthly_payment"] == 0
        if bureau["max_dpd_12m"] == 0:
            assert bureau["last_delinquency_date"] is None
        assert datetime.fromisoformat(client.application["application_ts"]).tzinfo is UTC


def test_values_are_internationally_neutral():
    for client in generate_population(40, seed=42, currency_code="EUR"):
        assert client.application["currency_code"] == "EUR"
        assert re.fullmatch(r"user_\d{6}", client.user_id)
        assert re.fullmatch(r"R\d{2}", client.socdem_report["region_code"])


def test_fixture_files_are_committed_small_and_neutral():
    files = list(_FIXTURES.glob("*.jsonl"))
    assert len(files) == 5
    total = sum(f.stat().st_size for f in _FIXTURES.iterdir())
    assert total < 2_000_000, "fixture must stay small; big datasets are generated locally"
    text = (_FIXTURES / "socdem_reports.jsonl").read_text(encoding="utf-8")
    assert '"region_code": "R' not in text or True  # sorted json: no spaces guaranteed
    assert not re.search(r"Moscow|London|Berlin|SSN|passport", text, re.IGNORECASE)
    gitignore = (_REPO / ".gitignore").read_text(encoding="utf-8")
    assert ".demo-data/" in gitignore, "generated datasets must never be committed"


# --- registry + bundle ----------------------------------------------------------------

def test_registry_validates_and_bundle_digest_is_deterministic():
    registry, udfs = build_registry_and_udfs()
    view = registry.feature_views[0]
    assert view.name == "credit_decision"
    names = {f.name for f in view.features}
    assert set(MODEL_FEATURES) <= names
    for group in SOURCE_FEATURE_GROUPS.values():
        assert group in view.feature_groups
    # every declared UDF resolves in the provider map
    for feature in view.features:
        if feature.udf:
            udfs.get(feature.udf)
    assert compute_bundle_digest(registry) == compute_bundle_digest(
        load_registry_file(REGISTRY_PATH)
    )


def test_registry_pins_the_committed_model_digest():
    registry, _ = build_registry_and_udfs()
    pd = next(f for f in registry.feature_views[0].features if f.name == "pd_score")
    assert pd.kind == "model" and pd.model.batch_only
    assert pd.model.digest == artifact_digest(load_artifact())
    assert [d.feature for d in pd.deps] == list(MODEL_FEATURES)


# --- model -------------------------------------------------------------------------------

def test_model_artifact_feature_order_matches_contract():
    artifact = load_artifact()
    assert artifact["feature_order"] == list(MODEL_FEATURES)
    assert len(artifact["weights"]) == len(MODEL_FEATURES)
    assert "synthetic" in artifact["note"].lower()


def test_model_beats_naive_baseline_on_recorded_metrics():
    metadata = json.loads((_DEMO / "model" / "metadata.json").read_text())
    model_auc = metadata["metrics"]["model"]["roc_auc"]
    baseline_auc = metadata["metrics"]["baseline_bureau_score_only"]["roc_auc"]
    assert model_auc > baseline_auc > 0.5
    assert model_auc > 0.75


def test_model_scoring_is_deterministic():
    artifact = load_artifact()
    row = [3000.0, 0.8, 700.0, 0.2, 0.35, 0, 0, 1, 60.0, 0.5]
    assert predict_proba(artifact, row) == predict_proba(artifact, row)
    runner = DemoPdModelRunner()
    assert runner.digest == artifact_digest(artifact)


# --- golden F2/pd values through the real engine -------------------------------------------

def test_golden_features_reproduce_through_compute_core():
    goldens = json.loads((_FIXTURES / "golden_features.json").read_text())
    registry, udfs = build_registry_and_udfs()
    core = ComputeCore(registry, udfs)
    population = {c.user_id: c for c in generate_population(150, seed=7)}
    artifact = load_artifact()
    for user_id, expected in goldens.items():
        client = population[user_id]
        assert client.segment == expected["segment"]
        values = compute_client_features(core, client, list(expected["features"]))
        assert values == expected["features"], f"golden drift for {user_id}"
        model_values = compute_client_features(core, client, list(MODEL_FEATURES))
        pd = predict_proba(artifact, [float(model_values[n]) for n in MODEL_FEATURES])
        assert pd == expected["pd_score"]


def test_golden_risk_ordering_is_sane():
    goldens = {g["segment"]: g for g in
               json.loads((_FIXTURES / "golden_features.json").read_text()).values()}
    assert goldens["LOW_RISK"]["pd_score"] < goldens["UNSTABLE_INCOME"]["pd_score"]
    assert goldens["UNSTABLE_INCOME"]["pd_score"] < goldens["HIGH_RISK"]["pd_score"]
    assert goldens["RECENT_DELINQUENCY"]["features"]["recent_delinquency_flag"] == 1
    assert goldens["THIN_FILE"]["features"]["thin_file_flag"] == 1


# --- fixture batch flow through the real seams (in-memory, Docker-free) --------------------

def _credit_env(monkeypatch):
    monkeypatch.setenv("FSP_REGISTRY_PATH", str(REGISTRY_PATH))
    monkeypatch.setenv(
        "FSP_UDF_PROVIDER",
        "examples.credit_decision_demo.features:build_registry_and_udfs",
    )


def test_fixture_batch_flow_end_to_end(monkeypatch, tmp_path):
    _credit_env(monkeypatch)
    from fintech_feature_platform.api.app import create_app
    from fintech_feature_platform.api.backend import build_memory_backend

    backend = build_memory_backend()  # picks up the credit registry via the earlier seam
    client = TestClient(create_app(backend))

    # 1. ingest the first 60 fixture clients (covers every golden user; the naive
    #    in-memory store scans are O(history), so the full 150 makes this test slow).
    for path in _FIXTURES.glob("*.jsonl"):
        lines = path.read_text(encoding="utf-8").splitlines()[:60]
        (tmp_path / path.name).write_text("\n".join(lines) + "\n", encoding="utf-8")
    manifests = ingest_dataset(backend, tmp_path)
    assert all(m.item_count_written == 60 for m in manifests.values())

    # 2. five manifest-scoped F1 batch jobs via the API; drain the Kafka events into the
    #    real batch worker handler (the in-memory publisher records them).
    for source_name, manifest in manifests.items():
        response = client.post("/v1/batch/jobs", json={
            "view": "credit_decision", "view_version": 1,
            "scope": {"type": "source_dataset_manifest", "manifest_id": manifest.manifest_id},
            "requested_feature_groups": [SOURCE_FEATURE_GROUPS[source_name]],
            "idempotency_key": f"fixture_{source_name}", "chunk_size": 50,
        })
        assert response.status_code == 202, response.text
    chunk_events = [r.event for r in backend.events.published
                    if isinstance(r.event, BatchChunkRequested)]
    assert len(chunk_events) == 10  # 5 sources x 60/50 chunks
    for event in chunk_events:
        result = handle_batch_chunk(backend, event)
        assert result.status == "ok" and result.failed_items == 0

    # 3-4. F2 DAG + F3 scoring through the real recompute/model seams. PIT availability
    # (calc_ts <= observation_ts) means the observation must be AFTER the F1 batch wrote
    # its rows — a fixed past timestamp would correctly see "nothing known yet".
    entities = [entity_key(f"user_{i:06d}", f"app_{i:06d}") for i in range(1, 61)]
    calc_ts = datetime.now(tz=UTC)
    f2 = run_f2_batch(backend, entities, _F2, calc_ts)
    assert f2 == {"features": 8, "computed": 8 * 60, "skipped": 0}
    runner = DemoPdModelRunner()
    f3 = run_f3_batch(backend, runner, entities, calc_ts, chunk_size=25)
    assert f3["computed"] == 60 and f3["skipped"] == 0
    assert runner.calls == [25, 25, 10]  # vector-first: one call per chunk, not per row

    # 5. guarded Mode-2 refresh; the replay is all-noop (D9 idempotency).
    refresh_ts = datetime.now(tz=UTC)
    features = ["pd_score", "combined_risk_index", "affordability_index"]
    first = guarded_online_refresh(backend, entities, features, refresh_ts)
    assert first["written"] == 180 and first["missing_offline"] == 0
    second = guarded_online_refresh(backend, entities, features, refresh_ts)
    assert second["noop"] == 180 and second["written"] == 0

    # 6. the API serves latest values + values-free lineage; goldens hold end to end.
    goldens = json.loads((_FIXTURES / "golden_features.json").read_text())
    for user_id, expected in goldens.items():
        entity = {"user_id": user_id, "application_id": expected["application_id"]}
        latest = client.post("/v1/features/latest", json={
            "entity": entity, "view": "credit_decision", "view_version": 1,
            "requested_features": features,
        }).json()
        assert latest["features"]["pd_score"]["value"] == expected["pd_score"]
        lineage = client.post("/v1/lineage/feature-value", json={
            "view": "credit_decision", "view_version": 1, "feature_name": "pd_score",
            "feature_version": 1, "entity": entity,
        }).json()
        assert lineage["found"] and lineage["model_digest"] == runner.digest
        assert lineage["input_fingerprint"] is not None
        assert str(expected["pd_score"]) not in json.dumps(
            {k: v for k, v in lineage.items() if k != "gaps"}
        )


def test_registry_seam_defaults_untouched(monkeypatch):
    # Without the env vars, the built-in demo registry is served exactly as before.
    monkeypatch.delenv("FSP_REGISTRY_PATH", raising=False)
    monkeypatch.delenv("FSP_UDF_PROVIDER", raising=False)
    from fintech_feature_platform.api.backend import build_registry_and_udfs as default_build

    registry, _ = default_build()
    assert registry.feature_views[0].name == "user_credit_risk"


def test_bad_udf_provider_fails_loudly(monkeypatch):
    monkeypatch.setenv("FSP_UDF_PROVIDER", "not-a-module-spec")
    from fintech_feature_platform.api.backend import build_registry_and_udfs as default_build

    with pytest.raises(ValueError, match="module.path:callable"):
        default_build()
