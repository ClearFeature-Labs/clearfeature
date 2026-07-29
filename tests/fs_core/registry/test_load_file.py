from pathlib import Path

import pytest

from fintech_feature_platform.fs_core.registry.loader import load_registry_file

_EXAMPLE = (
    Path(__file__).resolve().parents[3]
    / "examples"
    / "registry"
    / "minimal_credit_risk.yaml"
)

_VALID_YAML = """
registry_version: "test-v1"
entities:
  application:
    key_fields: ["user_id", "application_id"]
sources:
  credit_report:
    type: "raw_report"
    report_type: "credit_report"
    ts_field: "report_ts"
feature_views:
  user_credit_risk:
    entity: "application"
    key_fields: ["user_id", "application_id"]
    view_version: 1
    owner: "risk_team"
    status: "active"
    features:
      declared_income:
        kind: "udf"
        feature_version: 1
        udf: "udf.credit.declared_income"
        inputs: ["credit_report"]
        dtype: "decimal"
        status: "active"
"""


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "registry.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_valid_yaml_loads(tmp_path):
    reg = load_registry_file(_write(tmp_path, _VALID_YAML))
    assert reg.registry_version == "test-v1"
    assert reg.feature_views[0].ref().encode() == "user_credit_risk:v1"


def test_malformed_yaml_raises_value_error(tmp_path):
    with pytest.raises(ValueError):
        load_registry_file(_write(tmp_path, "key: : : not valid"))


def test_non_mapping_yaml_raises_value_error(tmp_path):
    with pytest.raises(ValueError):
        load_registry_file(_write(tmp_path, "- a\n- b\n"))


def test_empty_yaml_raises_value_error(tmp_path):
    with pytest.raises(ValueError):
        load_registry_file(_write(tmp_path, ""))


def test_missing_file_raises_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_registry_file(tmp_path / "does_not_exist.yaml")


def test_shipped_example_loads():
    reg = load_registry_file(_EXAMPLE)
    assert reg.registry_version == "example-v1"
    view = reg.feature_views[0]
    assert view.ref().encode() == "user_credit_risk:v1"
    features = {f.name for f in view.features}
    assert "debt_to_income_ratio" in features
