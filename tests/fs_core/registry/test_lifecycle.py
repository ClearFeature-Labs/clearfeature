"""Feature lifecycle states + lifecycle-dependency rules."""

import pytest

from fintech_feature_platform.fs_core.registry.loader import build_registry
from fintech_feature_platform.fs_core.registry.models import (
    LIFECYCLE_DEPRECATED,
    LIFECYCLE_LIVE,
    normalize_lifecycle,
)


def _registry(features):
    data = {
        "registry_version": "test-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"},
        },
        "feature_views": {
            "v": {"entity": "e", "key_fields": ["id"], "view_version": 1,
                  "owner": "o", "status": "active", "features": features}
        },
    }
    return build_registry(data)


def _udf(status, *, deps=None, inputs=("src",)):
    body = {"kind": "udf", "feature_version": 1, "udf": "udf.x",
            "dtype": "float", "status": status}
    if deps is not None:
        body["deps"] = deps
    else:
        body["inputs"] = list(inputs)
    return body


# --- normalization / backward compat -----------------------------------------

def test_active_maps_to_live():
    assert normalize_lifecycle("active") == LIFECYCLE_LIVE
    registry = _registry({"a": _udf("active")})
    assert registry.feature_views[0].features[0].lifecycle == LIFECYCLE_LIVE


def test_inactive_and_disabled_map_to_deprecated():
    assert normalize_lifecycle("inactive") == LIFECYCLE_DEPRECATED
    assert normalize_lifecycle("disabled") == LIFECYCLE_DEPRECATED


def test_canonical_states_map_to_themselves():
    for state in ("draft", "shadow", "live", "deprecated"):
        assert normalize_lifecycle(state) == state
        registry = _registry({"a": _udf(state)})
        assert registry.feature_views[0].features[0].lifecycle == state


def test_unknown_lifecycle_rejected():
    with pytest.raises(ValueError, match="unknown lifecycle/status"):
        _registry({"a": _udf("archived")})


# --- lifecycle dependency rules ----------------------------------------------

def _pair(source_status, target_status):
    return {
        "base": _udf(target_status),
        "dep": _udf(source_status, deps=[{"feature": "base", "version": 1}]),
    }


def test_live_cannot_depend_on_shadow():
    with pytest.raises(ValueError, match="may not depend on"):
        _registry(_pair("live", "shadow"))


def test_live_cannot_depend_on_draft():
    with pytest.raises(ValueError, match="may not depend on"):
        _registry(_pair("live", "draft"))


def test_live_cannot_depend_on_deprecated():
    with pytest.raises(ValueError, match="may not depend on"):
        _registry(_pair("live", "deprecated"))


def test_shadow_can_depend_on_live():
    _registry(_pair("shadow", "live"))  # no raise


def test_shadow_can_depend_on_shadow():
    _registry(_pair("shadow", "shadow"))  # no raise


def test_draft_can_depend_on_live_shadow_draft():
    _registry(_pair("draft", "live"))
    _registry(_pair("draft", "shadow"))
    _registry(_pair("draft", "draft"))


def test_deprecated_source_rejected_as_dependency_for_non_deprecated():
    # No non-deprecated feature may depend on a deprecated one (no new dependents).
    for source in ("live", "shadow", "draft"):
        with pytest.raises(ValueError, match="may not depend on"):
            _registry(_pair(source, "deprecated"))
    # A deprecated feature may still depend on a deprecated one (historical graph).
    _registry(_pair("deprecated", "deprecated"))


def test_f3_model_feature_follows_lifecycle_rules():
    # A live F3 model depending on a draft input is rejected like any live feature.
    features = {
        "base": _udf("draft"),
        "pd": {"kind": "model", "feature_version": 1, "dtype": "float", "status": "live",
               "deps": [{"feature": "base", "version": 1}],
               "model": {"uri": "mlflow://m/1", "digest": "sha256:d",
                         "output_name": "score"}},
    }
    with pytest.raises(ValueError, match="may not depend on"):
        _registry(features)
