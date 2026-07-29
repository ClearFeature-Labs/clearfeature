"""Tests for the request-scoped result store (hybrid values,)."""

from datetime import UTC, datetime

from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureRef,
    FeatureResult,
    FeatureWriteSet,
)
from fintech_feature_platform.fs_core.stores.request_result import (
    InMemoryRequestResultStore,
    RequestResult,
    ValkeyRequestResultStore,
    result_key,
)

_TS = datetime(2026, 6, 27, 10, tzinfo=UTC)
_MAX = datetime(2026, 6, 27, 12, tzinfo=UTC)


def _write_set() -> FeatureWriteSet:
    entity_key = EntityKey.from_mapping({"user_id": "1"})
    result = FeatureResult(
        ref=FeatureRef("declared_income", 1),
        entity_key=entity_key,
        value=4_200_000,
        data_ts=_TS,
        calc_ts=_TS,
        max_input_data_ts=_MAX,
        input_fingerprint="sha256:fp",
        value_hash="sha256:vh",
    )
    return FeatureWriteSet(
        view="user_credit_risk",
        view_version=1,
        entity_key=entity_key,
        results={"declared_income": result},
        source_refs={"credit_report": "rep_1"},
        request_id="freq_1",
    )


def test_from_write_set_carries_values_and_outcomes():
    result = RequestResult.from_write_set(
        "freq_1", _write_set(), {"declared_income:v1": "skipped_stale"}
    )
    item = result.features["declared_income"]
    assert item["value"] == 4_200_000
    assert item["feature_version"] == 1
    assert item["data_ts"] == _TS.isoformat()
    assert item["max_input_data_ts"] == _MAX.isoformat()
    assert item["input_fingerprint"] == "sha256:fp"
    assert item["value_hash"] == "sha256:vh"
    assert item["online_write_status"] == "skipped_stale"
    assert result.view == "user_credit_risk"
    assert result.entity_key == {"user_id": "1"}


def test_json_round_trip():
    result = RequestResult.from_write_set(
        "freq_1", _write_set(), {"declared_income:v1": "written"}
    )
    restored = RequestResult.from_json(result.to_json())
    assert restored == result


def test_stores_no_payload_pointers():
    result = RequestResult.from_write_set("freq_1", _write_set(), {})
    raw = result.to_json().decode()
    assert "object_key" not in raw
    assert "storage_uri" not in raw
    assert "source_refs" not in raw  # values only, no report pointers


def test_in_memory_put_get():
    store = InMemoryRequestResultStore()
    result = RequestResult.from_write_set("freq_1", _write_set(), {})
    store.put(result)
    assert store.get("freq_1") == result
    assert store.get("freq_missing") is None


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.ttls: dict[str, int] = {}

    def set(self, key, value, ex=None):
        self.values[key] = value
        self.ttls[key] = ex

    def get(self, key):
        return self.values.get(key)


def test_valkey_store_round_trip_with_ttl():
    client = _FakeRedis()
    store = ValkeyRequestResultStore(client, ttl_s=900)
    result = RequestResult.from_write_set("freq_1", _write_set(), {})
    store.put(result)
    assert client.ttls[result_key("freq_1")] == 900
    assert store.get("freq_1") == result
    assert store.get("freq_missing") is None
