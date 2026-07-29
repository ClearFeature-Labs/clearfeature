"""Unit tests for the pure Feature Planner (group expansion + validation)."""

import pytest

from fintech_feature_platform.fs_core.models import FeatureRef
from fintech_feature_platform.fs_core.planner import (
    FeaturePlan,
    PlannerError,
    plan_features,
)
from fintech_feature_platform.fs_core.registry.models import FeatureDef, FeatureViewDef


def _feat(name, *, deps=(), inputs=("credit_report",)) -> FeatureDef:
    return FeatureDef(
        name=name,
        kind="udf",
        feature_version=1,
        udf="udf.x",
        dtype="decimal",
        status="active",
        inputs=inputs,
        deps=deps,
    )


def _view(features, groups=None) -> FeatureViewDef:
    return FeatureViewDef(
        name="v",
        entity="e",
        key_fields=("user_id",),
        view_version=1,
        owner="o",
        status="active",
        features=tuple(features),
        feature_groups=groups or {},
    )


def test_explicit_only_is_identity():
    view = _view([_feat("a"), _feat("b")])
    plan = plan_features(view, ["a"], [])
    assert isinstance(plan, FeaturePlan)
    assert plan.compute_features == ("a",)
    assert plan.requested_outputs == (FeatureRef("a", 1),)
    assert plan.expanded_groups == {}


def test_group_only_expands_to_output_features():
    view = _view([_feat("a"), _feat("b")], {"g": ("a", "b")})
    plan = plan_features(view, [], ["g"])
    assert plan.compute_features == ("a", "b")
    assert plan.expanded_groups == {"g": ("a", "b")}


def test_explicit_plus_group_dedupes_deterministically():
    view = _view([_feat("a"), _feat("b"), _feat("c")], {"g": ("a", "b")})
    # explicit "c" first, then group members a,b (dedupe skips nothing new here)
    plan = plan_features(view, ["c"], ["g"])
    assert plan.compute_features == ("c", "a", "b")


def test_dedupe_overlap_keeps_explicit_first():
    view = _view([_feat("a"), _feat("b")], {"g": ("a", "b")})
    plan = plan_features(view, ["a"], ["g"])
    assert plan.compute_features == ("a", "b")  # a not duplicated


def test_unknown_explicit_feature_raises():
    view = _view([_feat("a")])
    with pytest.raises(PlannerError):
        plan_features(view, ["nope"], [])


def test_unknown_group_raises():
    view = _view([_feat("a")], {"g": ("a",)})
    with pytest.raises(PlannerError):
        plan_features(view, [], ["missing_group"])


def test_group_with_unknown_feature_raises():
    # planner is defensive even if the registry validator was bypassed
    view = _view([_feat("a")], {"g": ("a", "z")})
    with pytest.raises(PlannerError):
        plan_features(view, [], ["g"])


def test_empty_after_expansion_raises():
    view = _view([_feat("a")])
    with pytest.raises(PlannerError):
        plan_features(view, [], [])


def test_dependency_cycle_raises():
    view = _view([_feat("a", deps=("b",)), _feat("b", deps=("a",))])
    with pytest.raises(PlannerError, match="cycle"):
        plan_features(view, ["a"], [])


def test_unknown_dependency_raises():
    view = _view([_feat("a", deps=("missing",))])
    with pytest.raises(PlannerError):
        plan_features(view, ["a"], [])


def test_dependencies_are_not_in_output_set():
    # a depends on b; requesting a yields only a as output (b is ephemeral)
    view = _view([_feat("a", deps=("b",)), _feat("b")])
    plan = plan_features(view, ["a"], [])
    assert plan.compute_features == ("a",)


def _model_score_feat(name) -> FeatureDef:
    return FeatureDef(
        name=name, kind="model_score", feature_version=1, udf="", dtype="float",
        status="active",
    )


def test_model_score_feature_cannot_be_computed():
    view = _view([_feat("a"), _model_score_feat("pd_score")])
    with pytest.raises(PlannerError, match="model_score"):
        plan_features(view, ["pd_score"], [])


def test_model_score_feature_in_group_cannot_be_computed():
    view = _view(
        [_feat("a"), _model_score_feat("pd_score")], {"g": ("a", "pd_score")}
    )
    with pytest.raises(PlannerError, match="model_score"):
        plan_features(view, [], ["g"])
