"""F3 model-as-feature registry validation  (kind: model + model spec)."""

import pytest

from fintech_feature_platform.fs_core.registry.loader import build_registry
from fintech_feature_platform.fs_core.registry.models import ModelSpec


def _registry(model=None, *, deps=None, inputs=None, extra_features=None):
    if model is None:
        model = {
            "uri": "mlflow://pd_model/17", "digest": "sha256:abc",
            "output_name": "score", "batch_only": True,
        }
    f3 = {
        "kind": "model", "feature_version": 1, "dtype": "float", "status": "active",
        "deps": deps if deps is not None else [{"feature": "base", "version": 1}],
    }
    if model != "OMIT":
        f3["model"] = model
    if inputs is not None:
        f3["inputs"] = inputs
    features = {
        "base": {"kind": "udf", "feature_version": 1, "udf": "udf.base",
                 "dtype": "float", "status": "active", "inputs": ["src"]},
        "pd_model_score": f3,
    }
    if extra_features:
        features.update(extra_features)
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


def test_accepts_valid_f3_model_feature():
    registry = _registry()
    f3 = registry.feature_views[0].features[1]
    assert f3.kind == "model"
    assert isinstance(f3.model, ModelSpec)
    assert f3.model.uri == "mlflow://pd_model/17"
    assert f3.model.digest == "sha256:abc"
    assert f3.model.output_name == "score"
    assert f3.model.batch_only is True
    # F3 inputs are version-pinned feature refs (deps), reusing F2 validation.
    assert f3.deps[0].feature == "base"
    assert f3.deps[0].version == 1


def test_rejects_f3_without_model_uri():
    with pytest.raises(ValueError, match="uri"):
        _registry({"digest": "sha256:abc", "output_name": "score"})


def test_rejects_f3_without_model_digest():
    with pytest.raises(ValueError, match="digest"):
        _registry({"uri": "mlflow://m/1", "output_name": "score"})


def test_rejects_f3_without_output_name():
    with pytest.raises(ValueError, match="output_name"):
        _registry({"uri": "mlflow://m/1", "digest": "sha256:abc"})


def test_model_spec_dataclass_requires_uri_digest_output():
    with pytest.raises(ValueError, match="model uri"):
        ModelSpec(uri="", digest="sha256:abc", output_name="score")
    with pytest.raises(ValueError, match="model digest"):
        ModelSpec(uri="mlflow://m/1", digest="", output_name="score")
    with pytest.raises(ValueError, match="output_name"):
        ModelSpec(uri="mlflow://m/1", digest="sha256:abc", output_name="")
    with pytest.raises(ValueError, match="batch_only"):
        ModelSpec(uri="mlflow://m/1", digest="sha256:abc", output_name="s",
                  batch_only=False)


def test_rejects_f3_missing_model_block():
    with pytest.raises(ValueError, match="must declare a model spec"):
        _registry("OMIT")


def test_rejects_online_capable_f3():
    with pytest.raises(ValueError, match="batch_only"):
        _registry({"uri": "mlflow://m/1", "digest": "sha256:abc",
                   "output_name": "score", "batch_only": False})


def test_rejects_f3_with_unpinned_input_version():
    # A dep version that disagrees with the target's registered version is rejected.
    with pytest.raises(ValueError, match="pins 'base' v2"):
        _registry(deps=[{"feature": "base", "version": 2}])


def test_rejects_f3_with_raw_source_inputs():
    with pytest.raises(ValueError, match="must not read raw sources"):
        _registry(inputs=["src"])


def test_rejects_f3_without_inputs():
    with pytest.raises(ValueError, match="at least one input feature"):
        _registry(deps=[])


def test_rejects_model_spec_on_udf_feature():
    with pytest.raises(ValueError, match="must not declare a model spec"):
        _registry(extra_features={
            "bad": {"kind": "udf", "feature_version": 1, "udf": "udf.bad",
                    "dtype": "float", "status": "active", "inputs": ["src"],
                    "model": {"uri": "mlflow://m/1", "digest": "sha256:x",
                              "output_name": "y"}},
        })


def test_f3_depth_and_cycle_validation_reused():
    # F3 participates in the F2 dependency graph: a cycle through it is rejected.
    with pytest.raises(ValueError, match="cycle"):
        _registry(
            deps=[{"feature": "loop", "version": 1}],
            extra_features={
                "loop": {"kind": "udf", "feature_version": 1, "udf": "udf.loop",
                         "dtype": "float", "status": "active",
                         "deps": [{"feature": "pd_model_score", "version": 1}]},
            },
        )
