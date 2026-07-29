"""Planner lifecycle gating: online serves live only; shadow needs allow_shadow."""

import pytest

from fintech_feature_platform.fs_core.planner import PlannerError, plan_features
from fintech_feature_platform.fs_core.registry.loader import build_registry


def _view(status):
    # A single leaf feature with the given lifecycle status.
    data = {
        "registry_version": "test-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"},
        },
        "feature_views": {
            "v": {"entity": "e", "key_fields": ["id"], "view_version": 1,
                  "owner": "o", "status": "active", "features": {
                      "f": {"kind": "udf", "feature_version": 1, "udf": "udf.f",
                            "dtype": "float", "status": status, "inputs": ["src"]},
                  }}
        },
    }
    return build_registry(data).feature_views[0]


def test_online_planner_allows_live():
    plan = plan_features(_view("live"), ["f"], [])
    assert plan.compute_features == ("f",)
    assert plan.shadow_features == ()


def test_active_is_planned_as_live():
    plan = plan_features(_view("active"), ["f"], [])
    assert plan.compute_features == ("f",)


def test_online_planner_rejects_draft():
    with pytest.raises(PlannerError, match="draft feature"):
        plan_features(_view("draft"), ["f"], [])


def test_online_planner_rejects_shadow_by_default():
    with pytest.raises(PlannerError, match="shadow feature"):
        plan_features(_view("shadow"), ["f"], [])


def test_online_planner_rejects_deprecated():
    with pytest.raises(PlannerError, match="lifecycle 'deprecated'"):
        plan_features(_view("deprecated"), ["f"], [])


def test_allow_shadow_permits_shadow_and_marks_it():
    plan = plan_features(_view("shadow"), ["f"], [], allow_shadow=True)
    assert plan.compute_features == ("f",)
    assert plan.shadow_features == ("f",)


def test_allow_shadow_still_rejects_draft_and_deprecated():
    with pytest.raises(PlannerError, match="draft feature"):
        plan_features(_view("draft"), ["f"], [], allow_shadow=True)
    with pytest.raises(PlannerError, match="lifecycle 'deprecated'"):
        plan_features(_view("deprecated"), ["f"], [], allow_shadow=True)
