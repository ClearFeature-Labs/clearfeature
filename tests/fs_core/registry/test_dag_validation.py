"""F2 DAG registry validation  (inputs, version pins, cycles, depth cap)."""

import pytest

from fintech_feature_platform.fs_core.registry.models import (
    DEFAULT_DEPENDENCY_DEPTH_CAP,
    EntityDef,
    FeatureDef,
    FeatureDependency,
    FeatureViewDef,
    Registry,
    SourceDef,
)
from fintech_feature_platform.fs_core.registry.validator import validate_registry


def _udf(name, *, inputs=(), deps=(), version=1):
    return FeatureDef(
        name=name, kind="udf", feature_version=version, udf=f"udf.{name}",
        dtype="float", status="active", inputs=tuple(inputs), deps=tuple(deps),
    )


def _registry(features, *, max_dependency_depth=None):
    view = FeatureViewDef(
        name="v", entity="e", key_fields=("id",), view_version=1, owner="o",
        status="active", features=tuple(features), max_dependency_depth=max_dependency_depth,
    )
    return Registry(
        registry_version="v1",
        entities=(EntityDef("e", ("id",)),),
        sources=(SourceDef("src", "raw_report", "rep", "report_ts"),),
        feature_views=(view,),
    )


# --- valid ---

def test_accepts_valid_f2_inputs():
    features = [
        _udf("base", inputs=("src",)),
        _udf("derived", deps=(FeatureDependency("base", version=1, required=True,
                                                max_input_age_seconds=86400),)),
    ]
    validate_registry(_registry(features))  # no raise


def test_accepts_legacy_bare_string_dep():
    features = [_udf("base", inputs=("src",)), _udf("d", deps=("base",))]
    validate_registry(_registry(features))
    # Bare string normalized to a FeatureDependency with an implicit pin.
    assert features[1].deps[0] == FeatureDependency("base")


# --- unknown / cycle ---

def test_rejects_unknown_input_feature():
    features = [_udf("d", deps=(FeatureDependency("nope", version=1),))]
    with pytest.raises(ValueError, match="unknown feature 'nope'"):
        validate_registry(_registry(features))


def test_rejects_cycle():
    features = [
        _udf("a", deps=(FeatureDependency("b", version=1),)),
        _udf("b", deps=(FeatureDependency("a", version=1),)),
    ]
    with pytest.raises(ValueError, match="cycle"):
        validate_registry(_registry(features))


# --- version pin ---

def test_requires_matching_version_pin():
    # base is v1; a dependent that pins v2 must be rejected (no implicit latest).
    features = [
        _udf("base", inputs=("src",), version=1),
        _udf("d", deps=(FeatureDependency("base", version=2),)),
    ]
    with pytest.raises(ValueError, match="pins 'base' v2"):
        validate_registry(_registry(features))


def test_dependency_version_must_be_explicit_positive():
    with pytest.raises(ValueError, match="explicit version"):
        FeatureDependency("base", version=0)


# --- depth cap ---

def _chain(n):
    """Build a chain f0 <- f1 <- ... <- f(n): f0 reads src, each fi deps on f(i-1)."""
    feats = [_udf("f0", inputs=("src",))]
    for i in range(1, n + 1):
        feats.append(_udf(f"f{i}", deps=(FeatureDependency(f"f{i-1}", version=1),)))
    return feats


def test_accepts_chain_at_default_cap():
    # depth(f3) = 3 == default cap 3 -> allowed.
    assert DEFAULT_DEPENDENCY_DEPTH_CAP == 3
    validate_registry(_registry(_chain(3)))


def test_rejects_chain_over_default_cap():
    # depth(f4) = 4 > default cap 3 -> rejected.
    with pytest.raises(ValueError, match="dependency depth 4 exceeding cap 3"):
        validate_registry(_registry(_chain(4)))


def test_accepts_chain_up_to_override_cap():
    # depth(f5) = 5 allowed when the view raises the cap to 5.
    validate_registry(_registry(_chain(5), max_dependency_depth=5))


def test_rejects_chain_over_override_cap():
    with pytest.raises(ValueError, match="exceeding cap 5"):
        validate_registry(_registry(_chain(6), max_dependency_depth=5))


def test_override_cap_cannot_exceed_max():
    with pytest.raises(ValueError, match="must be 1..5"):
        FeatureViewDef(
            name="v", entity="e", key_fields=("id",), view_version=1, owner="o",
            status="active", features=(_udf("f0", inputs=("src",)),),
            max_dependency_depth=6,
        )
