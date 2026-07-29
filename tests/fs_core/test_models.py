from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureRef,
    FeatureResult,
    FeatureViewRef,
    RawReportMeta,
)


def _aware(hour: int = 10) -> datetime:
    return datetime(2024, 8, 26, hour, tzinfo=UTC)


def test_entity_key_from_mapping_alphabetical():
    key = EntityKey.from_mapping({"user_id": "123", "application_id": "A1"})
    assert key.encode() == "application_id=A1|user_id=123"


def test_entity_key_from_mapping_explicit_order():
    key = EntityKey.from_mapping(
        {"user_id": "123", "application_id": "A1", "report_id": "R9"},
        key_order=["user_id", "application_id", "report_id"],
    )
    assert key.encode() == "user_id=123|application_id=A1|report_id=R9"


def test_entity_key_is_immutable():
    key = EntityKey.from_mapping({"user_id": "123"})
    assert isinstance(key.parts, tuple)
    with pytest.raises(FrozenInstanceError):
        key.parts = ()


def test_entity_key_empty_rejected():
    with pytest.raises(ValueError):
        EntityKey.from_mapping({})
    with pytest.raises(ValueError):
        EntityKey(())  # direct construction is validated too


def test_feature_ref_encode_and_validation():
    assert FeatureRef("declared_income", 1).encode() == "declared_income:v1"
    with pytest.raises(ValueError):
        FeatureRef("declared_income", 0)
    with pytest.raises(ValueError):
        FeatureRef("", 1)


def test_feature_view_ref_encode_and_validation():
    assert FeatureViewRef("user_credit_risk", 2).encode() == "user_credit_risk:v2"
    with pytest.raises(ValueError):
        FeatureViewRef("user_credit_risk", 0)
    with pytest.raises(ValueError):
        FeatureViewRef("", 1)


def test_feature_result_holds_value_and_timestamps():
    result = FeatureResult(
        ref=FeatureRef("declared_income", 1),
        entity_key=EntityKey.from_mapping({"user_id": "123"}),
        value=3500000,
        data_ts=_aware(hour=10),
        calc_ts=_aware(hour=12),
    )
    assert result.value == 3500000
    assert result.data_ts != result.calc_ts


@pytest.mark.parametrize("naive_field", ["data_ts", "calc_ts"])
def test_feature_result_rejects_naive_datetimes(naive_field):
    kwargs = {
        "ref": FeatureRef("declared_income", 1),
        "entity_key": EntityKey.from_mapping({"user_id": "123"}),
        "value": 1,
        "data_ts": _aware(),
        "calc_ts": _aware(),
    }
    kwargs[naive_field] = datetime(2024, 8, 26, 10)
    with pytest.raises(ValueError):
        FeatureResult(**kwargs)


def _meta(**overrides):
    kwargs = {
        "report_ref": "rep_1",
        "report_type": "credit_report",
        "entity_type": "application",
        "entity_key": EntityKey.from_mapping({"user_id": "123", "application_id": "A1"}),
        "report_ts": _aware(),
        "payload_size_bytes": 123,
        "content_hash": "sha256:abc",
        "storage_uri": "s3://raw/credit_report/rep_1.json.gz",
        "created_at": _aware(hour=11),
    }
    kwargs.update(overrides)
    return RawReportMeta(**kwargs)


def test_raw_report_meta_minimal_defaults():
    meta = _meta()
    assert meta.format == "json"
    assert meta.compression == "gzip"
    assert meta.entity_key.encode() == "application_id=A1|user_id=123"


def test_raw_report_meta_rejects_empty_ref():
    with pytest.raises(ValueError):
        _meta(report_ref="")


def test_raw_report_meta_rejects_negative_size():
    with pytest.raises(ValueError):
        _meta(payload_size_bytes=-1)


@pytest.mark.parametrize("naive_field", ["report_ts", "created_at"])
def test_raw_report_meta_rejects_naive_datetimes(naive_field):
    with pytest.raises(ValueError):
        _meta(**{naive_field: datetime(2024, 8, 26, 10)})
