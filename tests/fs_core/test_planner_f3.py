"""Planner rejects F3 for the UDF/online compute path.

The dedicated F3 batch path and the offline recompute wave bypass ``plan_features``, so
these rejections gate online + raw-source chunks without blocking batch F3.
"""

import pytest

from fintech_feature_platform.fs_core.planner import PlannerError, plan_features
from fintech_feature_platform.fs_core.registry.loader import build_registry


def _registry():
    data = {
        "registry_version": "test-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"},
        },
        "feature_views": {
            "v": {"entity": "e", "key_fields": ["id"], "view_version": 1,
                  "owner": "o", "status": "active", "features": {
                      "base": {"kind": "udf", "feature_version": 1, "udf": "udf.base",
                               "dtype": "float", "status": "active", "inputs": ["src"]},
                      "pd_score": {"kind": "model", "feature_version": 1, "dtype": "float",
                                   "status": "active",
                                   "deps": [{"feature": "base", "version": 1}],
                                   "model": {"uri": "mlflow://m/1", "digest": "sha256:d",
                                             "output_name": "score"}},
                      # F2 that consumes the F3 output.
                      "risk_band": {"kind": "udf", "feature_version": 1, "udf": "udf.band",
                                    "dtype": "float", "status": "active",
                                    "deps": [{"feature": "pd_score", "version": 1}]},
                  }}
        },
    }
    return build_registry(data)


def _view():
    return _registry().feature_views[0]


def test_online_planner_rejects_direct_f3_request():
    with pytest.raises(PlannerError, match="batch-only model feature"):
        plan_features(_view(), ["pd_score"], [])


def test_online_planner_rejects_f2_depending_on_f3():
    with pytest.raises(PlannerError, match="depends on batch-only model feature"):
        plan_features(_view(), ["risk_band"], [])


def test_planner_still_plans_plain_udf():
    plan = plan_features(_view(), ["base"], [])
    assert plan.compute_features == ("base",)
