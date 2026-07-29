"""Registry feature_groups: loading + cross-reference validation."""

import pytest

from fintech_feature_platform.fs_core.registry.loader import build_registry


def _registry(feature_groups: dict) -> dict:
    return {
        "registry_version": "test-v1",
        "entities": {"application": {"key_fields": ["user_id"]}},
        "sources": {
            "credit_report": {
                "type": "raw_report",
                "report_type": "credit_report",
                "ts_field": "report_ts",
            }
        },
        "feature_views": {
            "v": {
                "entity": "application",
                "key_fields": ["user_id"],
                "view_version": 1,
                "owner": "o",
                "status": "active",
                "features": {
                    "a": {
                        "kind": "udf",
                        "feature_version": 1,
                        "udf": "udf.a",
                        "inputs": ["credit_report"],
                        "dtype": "decimal",
                        "status": "active",
                    },
                    "b": {
                        "kind": "udf",
                        "feature_version": 1,
                        "udf": "udf.b",
                        "inputs": ["credit_report"],
                        "dtype": "decimal",
                        "status": "active",
                    },
                },
                "feature_groups": feature_groups,
            }
        },
    }


def test_loads_feature_groups():
    registry = build_registry(_registry({"g": ["a", "b"]}))
    view = registry.feature_views[0]
    assert view.feature_groups == {"g": ("a", "b")}


def test_view_without_feature_groups_is_empty():
    data = _registry({})
    del data["feature_views"]["v"]["feature_groups"]
    registry = build_registry(data)
    assert registry.feature_views[0].feature_groups == {}


def test_group_referencing_unknown_feature_is_rejected():
    with pytest.raises(ValueError, match="unknown feature"):
        build_registry(_registry({"g": ["a", "z"]}))


def test_empty_group_list_is_rejected():
    with pytest.raises(ValueError, match="non-empty"):
        build_registry(_registry({"g": []}))


def test_duplicate_feature_in_group_is_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        build_registry(_registry({"g": ["a", "a"]}))


# --- model_score features  ---------------------------------------

def _registry_with_feature(feature_body: dict) -> dict:
    data = _registry({})
    del data["feature_views"]["v"]["feature_groups"]
    data["feature_views"]["v"]["features"]["pd_score"] = feature_body
    return data


def test_loads_model_score_feature_without_udf_or_inputs():
    registry = build_registry(
        _registry_with_feature(
            {"kind": "model_score", "feature_version": 1, "dtype": "float", "status": "active"}
        )
    )
    features = {f.name: f for f in registry.feature_views[0].features}
    assert features["pd_score"].kind == "model_score"
    assert features["pd_score"].udf == ""
    assert features["pd_score"].inputs == ()


def test_model_score_feature_with_inputs_is_rejected():
    with pytest.raises(ValueError, match="must not have inputs or deps"):
        build_registry(
            _registry_with_feature(
                {
                    "kind": "model_score",
                    "feature_version": 1,
                    "dtype": "float",
                    "status": "active",
                    "inputs": ["credit_report"],
                }
            )
        )


def test_udf_feature_without_udf_still_rejected():
    with pytest.raises(ValueError, match="must reference a udf"):
        build_registry(
            _registry_with_feature(
                {"kind": "udf", "feature_version": 1, "dtype": "float", "status": "active"}
            )
        )
