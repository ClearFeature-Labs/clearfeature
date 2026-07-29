import pytest

from fintech_feature_platform.fs_core.registry.loader import build_registry


def _valid_data() -> dict:
    """A UDF-only subset of the reference registry, as a parsed mapping."""
    return {
        "registry_version": "test-v1",
        "entities": {
            "application": {"key_fields": ["user_id", "application_id"]},
        },
        "sources": {
            "credit_report": {
                "type": "raw_report",
                "report_type": "credit_report",
                "ts_field": "report_ts",
            },
            "tax_report": {
                "type": "raw_report",
                "report_type": "tax_report",
                "ts_field": "report_ts",
            },
        },
        "feature_views": {
            "user_credit_risk": {
                "entity": "application",
                "key_fields": ["user_id", "application_id"],
                "view_version": 1,
                "owner": "risk_team",
                "status": "active",
                "features": {
                    "declared_income": {
                        "kind": "udf",
                        "feature_version": 1,
                        "udf": "udf.credit.declared_income",
                        "inputs": ["credit_report"],
                        "dtype": "decimal",
                        "status": "active",
                    },
                    "monthly_obligations": {
                        "kind": "udf",
                        "feature_version": 1,
                        "udf": "udf.credit.monthly_obligations",
                        "inputs": ["credit_report"],
                        "dtype": "decimal",
                        "status": "active",
                    },
                    "income_from_tax": {
                        "kind": "udf",
                        "feature_version": 1,
                        "udf": "udf.tax.income",
                        "inputs": ["tax_report"],
                        "dtype": "decimal",
                        "status": "active",
                    },
                    "debt_to_income_ratio": {
                        "kind": "udf",
                        "feature_version": 1,
                        "udf": "udf.common.safe_ratio",
                        "deps": ["monthly_obligations", "income_from_tax"],
                        "dtype": "float",
                        "status": "active",
                    },
                },
            },
        },
    }


def test_build_valid_registry():
    reg = build_registry(_valid_data())
    assert reg.registry_version == "test-v1"
    assert len(reg.entities) == 1
    assert len(reg.sources) == 2
    assert len(reg.feature_views) == 1

    view = reg.feature_views[0]
    assert view.ref().encode() == "user_credit_risk:v1"
    assert isinstance(view.key_fields, tuple)

    features = {f.name: f for f in view.features}
    assert features["declared_income"].ref().encode() == "declared_income:v1"
    assert features["debt_to_income_ratio"].dep_names() == (
        "monthly_obligations",
        "income_from_tax",
    )


def test_unknown_top_level_key_rejected():
    data = _valid_data()
    data["extra"] = 1
    with pytest.raises(ValueError):
        build_registry(data)


def test_unknown_feature_key_rejected():
    data = _valid_data()
    view = data["feature_views"]["user_credit_risk"]
    view["features"]["declared_income"]["typo"] = 1
    with pytest.raises(ValueError):
        build_registry(data)


def test_missing_required_feature_field_rejected():
    data = _valid_data()
    view = data["feature_views"]["user_credit_risk"]
    del view["features"]["declared_income"]["udf"]
    with pytest.raises(ValueError):
        build_registry(data)


def test_missing_entity_key_fields_rejected():
    data = _valid_data()
    del data["entities"]["application"]["key_fields"]
    with pytest.raises(ValueError):
        build_registry(data)


def test_missing_view_version_rejected():
    data = _valid_data()
    del data["feature_views"]["user_credit_risk"]["view_version"]
    with pytest.raises(ValueError):
        build_registry(data)


def test_unsupported_feature_kind_rejected():
    data = _valid_data()
    view = data["feature_views"]["user_credit_risk"]
    view["features"]["declared_income"]["kind"] = "model"
    with pytest.raises(ValueError):
        build_registry(data)


def test_feature_without_inputs_or_deps_rejected():
    data = _valid_data()
    view = data["feature_views"]["user_credit_risk"]
    del view["features"]["declared_income"]["inputs"]
    with pytest.raises(ValueError):
        build_registry(data)


def test_unsupported_source_type_rejected():
    data = _valid_data()
    data["sources"]["credit_report"]["type"] = "dwh_table"
    with pytest.raises(ValueError):
        build_registry(data)


def test_unknown_source_input_rejected():
    data = _valid_data()
    view = data["feature_views"]["user_credit_risk"]
    view["features"]["declared_income"]["inputs"] = ["does_not_exist"]
    with pytest.raises(ValueError):
        build_registry(data)


def test_unknown_dependency_rejected():
    data = _valid_data()
    view = data["feature_views"]["user_credit_risk"]
    view["features"]["debt_to_income_ratio"]["deps"] = ["ghost"]
    with pytest.raises(ValueError):
        build_registry(data)


def test_unknown_entity_in_view_rejected():
    data = _valid_data()
    data["feature_views"]["user_credit_risk"]["entity"] = "missing"
    with pytest.raises(ValueError):
        build_registry(data)
