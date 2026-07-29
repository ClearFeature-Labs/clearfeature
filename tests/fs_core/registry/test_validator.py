from pathlib import Path

import pytest

from fintech_feature_platform.fs_core.registry.loader import load_registry_file
from fintech_feature_platform.fs_core.registry.models import (
    EntityDef,
    FeatureDef,
    FeatureViewDef,
    Registry,
    SourceDef,
)
from fintech_feature_platform.fs_core.registry.validator import validate_registry

_EXAMPLE = (
    Path(__file__).resolve().parents[3]
    / "examples"
    / "registry"
    / "minimal_credit_risk.yaml"
)


def _feature(name: str, *, inputs=(), deps=()) -> FeatureDef:
    return FeatureDef(
        name=name,
        kind="udf",
        feature_version=1,
        udf=f"udf.{name}",
        dtype="float",
        status="active",
        inputs=tuple(inputs),
        deps=tuple(deps),
    )


def _registry(
    *,
    entities=None,
    sources=None,
    features=None,
    view_entity="application",
    view_key_fields=("user_id", "application_id"),
):
    if entities is None:
        entities = (EntityDef("application", ("user_id", "application_id")),)
    if sources is None:
        sources = (SourceDef("credit_report", "raw_report", "credit_report", "report_ts"),)
    if features is None:
        features = (_feature("declared_income", inputs=("credit_report",)),)
    view = FeatureViewDef(
        name="user_credit_risk",
        entity=view_entity,
        key_fields=view_key_fields,
        view_version=1,
        owner="risk_team",
        status="active",
        features=features,
    )
    return Registry(
        registry_version="v1",
        entities=entities,
        sources=sources,
        feature_views=(view,),
    )


def test_valid_registry_passes():
    validate_registry(_registry())  # no raise


def test_unknown_entity_rejected():
    with pytest.raises(ValueError):
        validate_registry(_registry(view_entity="missing"))


def test_unknown_source_input_rejected():
    features = (_feature("declared_income", inputs=("ghost_source",)),)
    with pytest.raises(ValueError):
        validate_registry(_registry(features=features))


def test_unknown_dependency_rejected():
    features = (
        _feature("a", inputs=("credit_report",)),
        _feature("b", deps=("does_not_exist",)),
    )
    with pytest.raises(ValueError):
        validate_registry(_registry(features=features))


def test_known_dependency_passes():
    features = (
        _feature("a", inputs=("credit_report",)),
        _feature("b", deps=("a",)),
    )
    validate_registry(_registry(features=features))  # no raise


def test_duplicate_entity_names_rejected():
    entities = (
        EntityDef("application", ("user_id",)),
        EntityDef("application", ("user_id", "application_id")),
    )
    with pytest.raises(ValueError):
        validate_registry(_registry(entities=entities))


def test_duplicate_source_names_rejected():
    sources = (
        SourceDef("credit_report", "raw_report", "credit_report", "report_ts"),
        SourceDef("credit_report", "raw_report", "credit_report", "report_ts"),
    )
    with pytest.raises(ValueError):
        validate_registry(_registry(sources=sources))


def test_duplicate_feature_names_rejected():
    features = (
        _feature("declared_income", inputs=("credit_report",)),
        _feature("declared_income", inputs=("credit_report",)),
    )
    with pytest.raises(ValueError):
        validate_registry(_registry(features=features))


def test_view_key_fields_matching_entity_passes():
    validate_registry(_registry(view_key_fields=("user_id", "application_id")))


def test_view_extra_key_field_rejected():
    with pytest.raises(ValueError):
        validate_registry(
            _registry(view_key_fields=("user_id", "application_id", "report_id"))
        )


def test_view_key_field_order_mismatch_rejected():
    # Same fields as the entity, but different order: order matters.
    with pytest.raises(ValueError):
        validate_registry(_registry(view_key_fields=("application_id", "user_id")))


def test_example_registry_passes_validation():
    # load_registry_file runs build_registry -> validate_registry; must not raise.
    load_registry_file(_EXAMPLE)
