""" — pipeline, per-feature, worker, D9 and artifact metrics.

Drives the REAL execution paths (memory backend: online compute_write_set, batch
FeatureStore.compute, the runner classification seam) and asserts the new metric
families record with the closed domains — no duplicates, no arbitrary series.
"""

from datetime import UTC, datetime

import pytest
from prometheus_client import generate_latest

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.runner_daemon import classify_result, record_worker_item
from fintech_feature_platform.fs_core.models import EntityKey, SourceStamp
from fintech_feature_platform.fs_core.observability.catalog import (
    WORKER_ROLES,
    fully_qualified_feature_ids,
)
from fintech_feature_platform.fs_core.observability.metrics import MetricLabelError
from fintech_feature_platform.fs_core.raw.meta_repository import RawReportMeta

_TS = datetime(2026, 1, 10, tzinfo=UTC)
_KEY = EntityKey.from_mapping(
    {"user_id": "1", "application_id": "A1", "report_id": "R9"},
    key_order=["user_id", "application_id", "report_id"],
)
_STAMPS = {
    "credit_report": SourceStamp(report_ts=_TS, content_hash="sha256:credit"),
    "tax_report": SourceStamp(report_ts=_TS, content_hash="sha256:tax"),
}
_PAYLOADS = {
    "credit_report": {"declared_income": 4200, "monthly_obligations": 800},
    "tax_report": {"income": 4000},
}


def _text(backend) -> str:
    return generate_latest(backend.metrics.registry).decode("utf-8")


def _seed_reports(backend):
    """Land real payloads + metadata so resolver-based paths (FeatureStore.compute)
    resolve refs like any batch/backfill item."""
    for ref, rtype, payload in (
        ("r1", "credit_report", _PAYLOADS["credit_report"]),
        ("r2", "tax_report", _PAYLOADS["tax_report"]),
    ):
        backend.payloads.put(f"mem://{ref}", payload)
        backend.metas.add(RawReportMeta(
            report_ref=ref, report_type=rtype, entity_type="application",
            entity_key=EntityKey.from_mapping({"user_id": "1"}),
            report_ts=_TS, payload_size_bytes=10, content_hash=f"sha256:{ref}",
            storage_uri=f"mem://{ref}", created_at=_TS,
        ))


def _compute(backend, features, mode="online"):
    return backend.store.compute_write_set(
        view="user_credit_risk", view_version=1, entity_key=_KEY,
        requested_features=features,
        source_refs={"credit_report": "r1", "tax_report": "r2"},
        source_stamps=_STAMPS, calc_ts=_TS,
        source_loader=lambda name: _PAYLOADS[name],
        execution_mode=mode,
    )


# --- A/B: whole-operation + pipeline stages ----------------------------------

def test_online_stages_input_fetch_and_compute_recorded():
    backend = build_memory_backend()
    _compute(backend, ["declared_income"])
    text = _text(backend)
    assert (
        'fsp_pipeline_stage_duration_seconds_count'
        '{execution_mode="online",stage="compute"} 1.0'
    ) in text
    # one lazy load for the one touched source
    assert 'execution_mode="online",stage="input_fetch"} 1.0' in text


def test_input_fetch_observed_once_per_source_not_per_feature():
    backend = build_memory_backend()
    # both features read credit_report; the context caches -> ONE input_fetch observation
    _compute(backend, ["declared_income", "monthly_obligations"])
    text = _text(backend)
    assert 'execution_mode="online",stage="input_fetch"} 1.0' in text


def test_batch_mode_stages_via_feature_store_compute():
    backend = build_memory_backend()
    _seed_reports(backend)
    backend.store.compute(
        view="user_credit_risk", view_version=1, entity_key=_KEY,
        requested_features=["declared_income"],
        source_refs={"credit_report": "r1", "tax_report": "r2"},
        source_stamps=_STAMPS, calc_ts=_TS,
    )
    text = _text(backend)
    assert 'execution_mode="batch",stage="compute"} 1.0' in text
    assert 'execution_mode="batch",stage="offline_write"} 1.0' in text
    assert 'execution_mode="batch",stage="online_write"} 1.0' in text


def test_stage_domain_is_closed():
    backend = build_memory_backend()
    with pytest.raises(MetricLabelError):
        backend.metrics.observe(
            "fsp_pipeline_stage_duration_seconds", 0.1,
            {"execution_mode": "online", "stage": "coffee_break"},
        )


# --- C: per-feature timing ---------------------------------------------------

def test_f1_online_timing_uses_registry_identity():
    backend = build_memory_backend()
    _compute(backend, ["declared_income"])
    text = _text(backend)
    fid = "user_credit_risk:v1:declared_income:v1"
    assert (
        f'fsp_feature_compute_duration_seconds_count'
        f'{{execution_mode="online",feature_id="{fid}"}} 1.0'
    ) in text
    assert (
        f'fsp_feature_compute_items_total'
        f'{{execution_mode="online",feature_id="{fid}"}} 1.0'
    ) in text


def test_f1_batch_timing_same_feature():
    backend = build_memory_backend()
    _compute(backend, ["declared_income"], mode="batch")
    fid = "user_credit_risk:v1:declared_income:v1"
    assert f'execution_mode="batch",feature_id="{fid}"' in _text(backend)


def test_f2_dependency_timing_is_exclusive_one_observation_per_node():
    backend = build_memory_backend()
    # debt_to_income_ratio (F2) depends on monthly_obligations + income_from_tax.
    _compute(backend, ["debt_to_income_ratio"])
    text = _text(backend)
    for name in ("debt_to_income_ratio", "monthly_obligations", "income_from_tax"):
        fid = f"user_credit_risk:v1:{name}:v1"
        # each node's UDF timed EXACTLY once (deps memoized, never re-timed)
        assert (
            f'fsp_feature_compute_duration_seconds_count'
            f'{{execution_mode="online",feature_id="{fid}"}} 1.0'
        ) in text


def test_no_duplicate_timing_when_dep_also_requested():
    backend = build_memory_backend()
    # requesting both the F2 and its dep must not time the dep twice (memoized)
    _compute(backend, ["debt_to_income_ratio", "monthly_obligations"])
    fid = "user_credit_risk:v1:monthly_obligations:v1"
    assert f'feature_id="{fid}"}} 1.0' in _text(backend)


def test_registry_binding_covers_all_registry_features():
    backend = build_memory_backend()
    ids = fully_qualified_feature_ids(backend.registry)
    assert "user_credit_risk:v1:declared_income:v1" in ids
    # every bound id is usable; nothing outside is (proven in domain tests)
    for fid in ids:
        backend.metrics.incr(
            "fsp_feature_compute_items_total",
            {"execution_mode": "online", "feature_id": fid},
        )


# --- D: worker items ---------------------------------------------------------

def test_classify_result_closed_mapping():
    assert classify_result("idle") is None
    assert classify_result("paused") is None
    assert classify_result("rate_limited") is None
    assert classify_result("ok") == "success"
    assert classify_result("observed") == "success"
    assert classify_result("deadline_expired") == "noop"
    assert classify_result("dead_lettered") == "dead_lettered"
    assert classify_result("retry_republished") == "retry"
    for failure in ("consume_error", "unexpected_error", "invalid_event",
                     "online_write_failed", "publish_failed", "result_store_failed",
                     "dlq_publish_failed", "infra_failed", "append_failed",
                     "brand_new_unknown_status"):
        assert classify_result(failure) == "failure"


@pytest.mark.parametrize("role", WORKER_ROLES)
def test_record_worker_item_all_roles(role):
    backend = build_memory_backend()
    record_worker_item(backend, role, "ok", 0.01)
    record_worker_item(backend, role, "idle", 0.0)  # not an item
    text = _text(backend)
    assert f'fsp_worker_items_total{{result="success",worker_role="{role}"}} 1.0' in text
    assert f'fsp_worker_processing_duration_seconds_count{{worker_role="{role}"}} 1.0' in text
    assert f'fsp_worker_last_success_unixtime_seconds{{worker_role="{role}"}}' in text


def test_last_success_only_on_success():
    backend = build_memory_backend()
    record_worker_item(backend, "online-worker", "consume_error", 0.01)
    assert "fsp_worker_last_success_unixtime_seconds{" not in _text(backend)


# --- F: D9 outcomes ----------------------------------------------------------

def test_d9_outcomes_batch_written_and_noop():
    backend = build_memory_backend()
    kwargs = dict(
        view="user_credit_risk", view_version=1, entity_key=_KEY,
        requested_features=["declared_income"],
        source_refs={"credit_report": "r1", "tax_report": "r2"},
        source_stamps=_STAMPS, calc_ts=_TS,
    )
    _seed_reports(backend)
    backend.store.compute(**kwargs)           # first write -> written
    backend.store.compute(**kwargs)           # identical rerun -> noop
    text = _text(backend)
    assert 'fsp_online_write_outcomes_total{execution_mode="batch",outcome="written"} 1.0' in text
    assert 'fsp_online_write_outcomes_total{execution_mode="batch",outcome="noop"} 1.0' in text


def test_d9_domain_closed():
    backend = build_memory_backend()
    with pytest.raises(MetricLabelError):
        backend.metrics.incr(
            "fsp_online_write_outcomes_total",
            {"execution_mode": "online", "outcome": "sideways"},
        )


# --- G: artifact verification ------------------------------------------------

def test_artifact_verification_failure_recorded(tmp_path, monkeypatch):
    from fintech_feature_platform.api.artifact_binding import enforce_artifact_binding
    from fintech_feature_platform.api.settings import load_settings
    from fintech_feature_platform.fs_core.observability.prometheus_recorder import (
        PrometheusMetricsRecorder,
    )
    from fintech_feature_platform.fs_core.runtime.artifact_verifier import (
        ArtifactVerificationError,
    )

    backend = build_memory_backend()  # build BEFORE poisoning the environment
    monkeypatch.setenv("FSP_ENVIRONMENT", "production")
    monkeypatch.setenv("FSP_ARTIFACT_BINDING", "legacy-compatible")
    recorder = PrometheusMetricsRecorder()
    with pytest.raises(ArtifactVerificationError):
        enforce_artifact_binding(backend.registry, load_settings(), metrics=recorder)
    text = generate_latest(recorder.registry).decode()
    assert 'fsp_artifact_verification_total{result="failure"} 1.0' in text
    assert 'category="feature_artifact_required"} 1.0' in text


# --- H: storage boundary -----------------------------------------------------

def test_timed_store_records_ok_and_error():
    from fintech_feature_platform.fs_core.observability.prometheus_recorder import (
        PrometheusMetricsRecorder,
    )
    from fintech_feature_platform.fs_core.observability.timing import TimedStore

    class Fake:
        def get_payload(self, key):
            return {"x": 1}

        def put(self, key, payload):
            raise RuntimeError("boom")

    recorder = PrometheusMetricsRecorder()
    store = TimedStore(Fake(), recorder, "minio", {"get_payload": "read", "put": "write"})
    assert store.get_payload("k") == {"x": 1}
    with pytest.raises(RuntimeError):
        store.put("k", {})
    text = generate_latest(recorder.registry).decode()
    assert 'operation="read",result="ok",store="minio"} 1.0' in text
    assert 'operation="write",result="error",store="minio"} 1.0' in text


# --- I: legacy + isolation stay safe (spot re-checks live in the exposition suites) --

def test_snapshot_schema_still_legacy_after_step2_families():
    backend = build_memory_backend()
    _compute(backend, ["declared_income"])
    snap = backend.metrics.snapshot()
    assert set(snap) == {"counters", "gauges", "histograms"}
