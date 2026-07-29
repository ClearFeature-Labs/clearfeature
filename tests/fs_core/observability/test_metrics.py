"""In-process metrics recorder + label safety."""

import pytest

from fintech_feature_platform.fs_core.observability.metrics import (
    InMemoryMetricsRecorder,
    MetricLabelError,
    NoopMetricsRecorder,
    recorder_of,
)


def test_incr_observe_gauge_snapshot():
    m = InMemoryMetricsRecorder()
    m.incr("online_requests_total", {"outcome": "ok"})
    m.incr("online_requests_total", {"outcome": "ok"})
    m.incr("online_requests_total", {"outcome": "error"})
    m.gauge("propagation_pending_waves", 3)
    m.observe("online_request_latency_ms", 10)
    m.observe("online_request_latency_ms", 30)

    snap = m.snapshot()
    assert snap["counters"]["online_requests_total{outcome=ok}"] == 2
    assert snap["counters"]["online_requests_total{outcome=error}"] == 1
    assert snap["gauges"]["propagation_pending_waves"] == 3
    hist = snap["histograms"]["online_request_latency_ms"]
    assert hist["count"] == 2 and hist["sum"] == 40
    assert hist["min"] == 10 and hist["max"] == 30


def test_incr_with_value():
    m = InMemoryMetricsRecorder()
    m.incr("batch_rows_written_total", value=50)
    m.incr("batch_rows_written_total", value=25)
    assert m.snapshot()["counters"]["batch_rows_written_total"] == 75


def test_forbidden_label_values_rejected():
    m = InMemoryMetricsRecorder()
    for bad in ("payload_json", "s3://bucket/key", "SELECT * FROM t", "{json:1}"):
        with pytest.raises(MetricLabelError):
            m.incr("x", {"label": bad})


def test_unbounded_label_rejected():
    m = InMemoryMetricsRecorder()
    with pytest.raises(MetricLabelError):
        m.incr("x", {"label": "a" * 200})


def test_noop_records_nothing():
    m = NoopMetricsRecorder()
    m.incr("x")
    m.gauge("y", 1)
    m.observe("z", 2)
    assert m.snapshot() == {"counters": {}, "gauges": {}, "histograms": {}}


def test_recorder_of_falls_back_to_noop():
    class _NoMetrics:
        pass

    rec = recorder_of(_NoMetrics())
    rec.incr("x")  # must not raise
    assert rec.snapshot() == {"counters": {}, "gauges": {}, "histograms": {}}


def test_recorder_of_returns_backend_metrics():
    class _Backend:
        metrics = InMemoryMetricsRecorder()

    backend = _Backend()
    recorder_of(backend).incr("hit")
    assert backend.metrics.snapshot()["counters"]["hit"] == 1
