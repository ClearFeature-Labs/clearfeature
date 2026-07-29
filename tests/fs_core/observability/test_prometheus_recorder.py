"""PrometheusMetricsRecorder + central metric catalog.

The recorder implements the existing ``MetricsRecorder`` protocol over a process-local
``CollectorRegistry``; every metric comes from the central catalog (no arbitrary names),
label keys must match the declared set exactly, and closed label domains are enforced
fail-fast — unknown names/labels never silently create time series.
"""

import pytest
from prometheus_client import generate_latest

from fintech_feature_platform.fs_core.observability.catalog import (
    WORKER_ROLES,
    catalog_by_name,
)
from fintech_feature_platform.fs_core.observability.metrics import MetricLabelError
from fintech_feature_platform.fs_core.observability.prometheus_recorder import (
    MetricCatalogError,
    PrometheusMetricsRecorder,
)


def _text(recorder: PrometheusMetricsRecorder) -> str:
    return generate_latest(recorder.registry).decode("utf-8")


# --- recorder semantics ------------------------------------------------------

def test_counter_records_to_exposition():
    r = PrometheusMetricsRecorder()
    r.incr("online_requests_total", {"outcome": "ok"})
    r.incr("online_requests_total", {"outcome": "ok"}, value=2)
    assert 'online_requests_total{outcome="ok"} 3.0' in _text(r)


def test_gauge_records_to_exposition():
    r = PrometheusMetricsRecorder()
    r.gauge("propagation_pending_waves", 7)
    assert "propagation_pending_waves 7.0" in _text(r)


def test_histogram_records_to_exposition():
    r = PrometheusMetricsRecorder()
    r.observe("online_request_latency_ms", 12.5)
    text = _text(r)
    assert "online_request_latency_ms_count 1.0" in text
    assert "online_request_latency_ms_sum 12.5" in text
    assert "online_request_latency_ms_bucket" in text  # real buckets, not a summary


def test_process_info_bounded_role():
    r = PrometheusMetricsRecorder()
    r.gauge("fsp_process_info", 1, {"worker_role": "online-worker"})
    assert 'fsp_process_info{worker_role="online-worker"} 1.0' in _text(r)


def test_snapshot_keeps_legacy_schema():
    # The legacy /v1/observability/metrics schema must not change in Step 1.
    r = PrometheusMetricsRecorder()
    r.incr("online_requests_total", {"outcome": "ok"})
    r.gauge("propagation_pending_waves", 2)
    r.observe("online_request_latency_ms", 5.0)
    snap = r.snapshot()
    assert set(snap) == {"counters", "gauges", "histograms"}
    assert snap["counters"]['online_requests_total{outcome=ok}'] == 1
    assert snap["histograms"]["online_request_latency_ms"]["count"] == 1


# --- catalog enforcement (fail-fast, never silent) ---------------------------

def test_unknown_metric_name_rejected():
    r = PrometheusMetricsRecorder()
    with pytest.raises(MetricCatalogError):
        r.incr("made_up_metric_total")


def test_missing_label_rejected():
    r = PrometheusMetricsRecorder()
    with pytest.raises(MetricLabelError):
        r.incr("online_requests_total")  # requires outcome


def test_extra_label_rejected():
    r = PrometheusMetricsRecorder()
    with pytest.raises(MetricLabelError):
        r.incr("online_requests_total", {"outcome": "ok", "job_id": "j1"})


def test_prohibited_label_value_rejected():
    r = PrometheusMetricsRecorder()
    with pytest.raises(MetricLabelError):
        r.incr("online_requests_total", {"outcome": "payload:{...}"})


def test_unsupported_bounded_value_rejected():
    r = PrometheusMetricsRecorder()
    with pytest.raises(MetricLabelError):
        r.gauge("fsp_process_info", 1, {"worker_role": "not-a-real-role"})


def test_kind_mismatch_rejected():
    r = PrometheusMetricsRecorder()
    with pytest.raises(MetricCatalogError):
        r.incr("propagation_pending_waves")  # declared as a gauge


# --- registry isolation ------------------------------------------------------

def test_two_registries_do_not_conflict():
    a, b = PrometheusMetricsRecorder(), PrometheusMetricsRecorder()
    a.incr("online_requests_total", {"outcome": "ok"})
    b.incr("online_requests_total", {"outcome": "failed"})
    assert 'outcome="ok"' in _text(a) and 'outcome="failed"' not in _text(a)
    assert 'outcome="failed"' in _text(b) and 'outcome="ok"' not in _text(b)


def test_repeated_construction_never_duplicate_registers():
    # Tests construct runtimes repeatedly; each recorder owns a fresh registry.
    for _ in range(25):
        r = PrometheusMetricsRecorder()
        r.incr("online_requests_total", {"outcome": "ok"})
    assert True  # no ValueError('Duplicated timeseries...') was raised


def test_backend_construction_repeats_cleanly():
    from fintech_feature_platform.api.backend import build_memory_backend

    for _ in range(5):
        backend = build_memory_backend()
        backend.metrics.incr("online_requests_total", {"outcome": "ok"})


# --- catalog integrity -------------------------------------------------------

def test_catalog_covers_every_recorded_metric_name():
    # Every name business code records today must be catalogued (grep-derived list).
    recorded_today = {
        "feature_updates_total", "propagation_debounced_total", "propagation_pending_waves",
        "propagation_waves_total", "propagation_wave_items_total", "propagation_lag_seconds",
        "online_requests_total", "online_request_errors_total", "online_request_latency_ms",
        "batch_pause_events_total", "batch_rate_limited_events_total", "dlq_events_total",
        "offline_append_errors_total", "offline_append_rows_total", "batch_chunks_total",
        "batch_items_total", "batch_rows_written_total",
    }
    assert recorded_today <= set(catalog_by_name())


def test_worker_roles_are_the_canonical_bounded_set():
    assert WORKER_ROLES == (
        "api", "online-worker", "batch-worker", "offline-writer",
        "metadata-writer", "propagation-worker", "model-score-writer",
    )
