"""Registry-bound feature_id metric domain.

feature_id is the ONE allowed registry-derived label, valid only for the per-feature
metric family, and only for identities bound from the loaded Registry. Arbitrary strings
(API/Kafka-supplied names) can never create series.
"""

import pytest
from prometheus_client import generate_latest

from fintech_feature_platform.fs_core.observability.metrics import MetricLabelError
from fintech_feature_platform.fs_core.observability.prometheus_recorder import (
    PrometheusMetricsRecorder,
)

FID_A = "activity:v1:event_count_7d:v1"
FID_B = "activity:v1:activity_score:v1"


def _text(r):
    return generate_latest(r.registry).decode("utf-8")


def test_unbound_domain_rejects_all_feature_ids():
    r = PrometheusMetricsRecorder()
    with pytest.raises(MetricLabelError):
        r.observe(
            "fsp_feature_compute_duration_seconds", 0.001,
            {"execution_mode": "online", "feature_id": FID_A},
        )


def test_bound_registry_ids_are_accepted():
    r = PrometheusMetricsRecorder()
    r.bind_dynamic_domain("feature_id", {FID_A, FID_B})
    r.observe(
        "fsp_feature_compute_duration_seconds", 0.002,
        {"execution_mode": "online", "feature_id": FID_A},
    )
    r.incr(
        "fsp_feature_compute_items_total",
        {"execution_mode": "batch", "feature_id": FID_B},
    )
    text = _text(r)
    assert f'feature_id="{FID_A}"' in text
    assert f'feature_id="{FID_B}"' in text


def test_unregistered_feature_string_cannot_create_series():
    r = PrometheusMetricsRecorder()
    r.bind_dynamic_domain("feature_id", {FID_A})
    with pytest.raises(MetricLabelError):
        r.observe(
            "fsp_feature_compute_duration_seconds", 0.001,
            {"execution_mode": "online", "feature_id": "totally_made_up_feature"},
        )
    assert "totally_made_up" not in _text(r)


def test_execution_mode_is_closed():
    r = PrometheusMetricsRecorder()
    r.bind_dynamic_domain("feature_id", {FID_A})
    with pytest.raises(MetricLabelError):
        r.observe(
            "fsp_feature_compute_duration_seconds", 0.001,
            {"execution_mode": "streaming", "feature_id": FID_A},
        )


def test_backend_binds_registry_feature_ids():
    # Backend construction binds the loaded registry's fully-qualified ids.
    from fintech_feature_platform.api.backend import build_memory_backend

    backend = build_memory_backend()
    # The demo registry's known feature: user_credit_risk view, declared_income v1.
    backend.metrics.observe(
        "fsp_feature_compute_duration_seconds", 0.001,
        {"execution_mode": "online",
         "feature_id": "user_credit_risk:v1:declared_income:v1"},
    )
    with pytest.raises(MetricLabelError):
        backend.metrics.observe(
            "fsp_feature_compute_duration_seconds", 0.001,
            {"execution_mode": "online", "feature_id": "not_in_registry:v1:x:v1"},
        )
