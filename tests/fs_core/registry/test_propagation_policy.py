"""Per-edge propagation policy on F2 dependency edges."""

import pytest

from fintech_feature_platform.fs_core.registry.loader import build_registry
from fintech_feature_platform.fs_core.registry.models import (
    DEFAULT_PROPAGATION,
    PROPAGATION_REACTIVE,
    FeatureDependency,
)


def _registry(propagation):
    dep = {"feature": "base", "version": 1}
    if propagation is not None:
        dep["propagation"] = propagation
    data = {
        "registry_version": "test-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"},
        },
        "feature_views": {
            "v": {
                "entity": "e", "key_fields": ["id"], "view_version": 1,
                "owner": "o", "status": "active",
                "features": {
                    "base": {"kind": "udf", "feature_version": 1, "udf": "udf.base",
                             "dtype": "float", "status": "active", "inputs": ["src"]},
                    "derived": {"kind": "udf", "feature_version": 1, "udf": "udf.derived",
                                "dtype": "float", "status": "active", "deps": [dep]},
                },
            }
        },
    }
    return build_registry(data)


def test_default_propagation_is_lazy():
    dep = FeatureDependency("base", version=1)
    assert dep.propagation == DEFAULT_PROPAGATION == "lazy"


def test_accepts_reactive_policy_on_edge():
    registry = _registry(PROPAGATION_REACTIVE)
    derived = registry.feature_views[0].features[1]
    assert derived.deps[0].propagation == "reactive"


def test_accepts_all_known_policies():
    for policy in ("lazy", "reactive", "scheduled", "none"):
        registry = _registry(policy)
        assert registry.feature_views[0].features[1].deps[0].propagation == policy


def test_rejects_unknown_policy_in_loader():
    with pytest.raises(ValueError, match="unknown propagation policy"):
        _registry("eager")


def test_rejects_unknown_policy_on_dataclass():
    with pytest.raises(ValueError, match="unknown propagation policy"):
        FeatureDependency("base", version=1, propagation="whenever")


def test_bare_string_dep_defaults_to_lazy():
    # A legacy bare-string dep still normalizes with the default (lazy) policy.
    data = {
        "registry_version": "test-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"},
        },
        "feature_views": {
            "v": {
                "entity": "e", "key_fields": ["id"], "view_version": 1,
                "owner": "o", "status": "active",
                "features": {
                    "base": {"kind": "udf", "feature_version": 1, "udf": "udf.base",
                             "dtype": "float", "status": "active", "inputs": ["src"]},
                    "derived": {"kind": "udf", "feature_version": 1, "udf": "udf.derived",
                                "dtype": "float", "status": "active", "deps": ["base"]},
                },
            }
        },
    }
    registry = build_registry(data)
    assert registry.feature_views[0].features[1].deps[0].propagation == "lazy"
