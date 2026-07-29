"""FeatureUpdated event: values-free serialization round-trip."""

import json
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.events.models import (
    EVENT_TYPE_FEATURE_UPDATED,
    EntityRef,
    FeatureUpdated,
)

_DATA_TS = datetime(2026, 1, 10, tzinfo=UTC)
_CALC_TS = datetime(2026, 1, 11, tzinfo=UTC)


def _event(**kw):
    base = dict(
        update_id="upd_1",
        entity=EntityRef("application", {"user_id": "u1", "application_id": "a1"}),
        view="user_credit_risk",
        view_version=1,
        feature_name="income_from_tax",
        feature_version=1,
        data_ts=_DATA_TS,
        calc_ts=_CALC_TS,
        source="offline_writer",
        occurred_at=_CALC_TS,
        max_input_data_ts=_DATA_TS,
        input_fingerprint="fp_abc",
        value_hash="vh_xyz",
        run_id="run_1",
    )
    base.update(kw)
    return FeatureUpdated(**base)


def test_round_trip_carries_refs_timestamps_and_hashes():
    event = _event()
    restored = FeatureUpdated.from_json(event.to_json())
    assert restored == event
    assert restored.event_type == EVENT_TYPE_FEATURE_UPDATED
    assert restored.feature_name == "income_from_tax"
    assert restored.feature_version == 1
    assert restored.input_fingerprint == "fp_abc"
    assert restored.value_hash == "vh_xyz"
    assert restored.max_input_data_ts == _DATA_TS


def test_serialization_carries_no_value_or_payload():
    data = json.loads(_event().to_json())
    forbidden = {
        "value", "value_json", "payload", "payload_json", "object_key",
        "storage_uri", "source_payload_b64", "sql", "row",
    }
    assert forbidden.isdisjoint(data.keys())
    # Only hash ids / refs / timestamps are present — never a raw feature value.
    assert "value_hash" in data and "input_fingerprint" in data


def test_partition_key_is_entity_encoded():
    event = _event()
    assert event.entity.encoded() == event.entity.encoded()  # deterministic
    assert ":" in event.entity.encoded() or "=" in event.entity.encoded()


def test_rejects_unknown_source():
    with pytest.raises(ValueError, match="unknown source"):
        _event(source="mystery")


def test_rejects_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        _event(data_ts=datetime(2026, 1, 10))  # noqa: DTZ001 - intentional


def test_optional_fields_absent_round_trip():
    event = _event(
        max_input_data_ts=None, input_fingerprint=None, value_hash=None,
        run_id=None, job_id=None, manifest_id=None,
    )
    restored = FeatureUpdated.from_json(event.to_json())
    assert restored == event
    assert restored.max_input_data_ts is None
