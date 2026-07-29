import json
import os
import sys
import uuid
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.stores.valkey_online import (
    ValkeyOnlineStore,
    _field,
    _key,
)
from fintech_feature_platform.fs_core.write_guard import (
    NOOP,
    SKIPPED_STALE,
    WRITTEN,
    WRITTEN_RECOMPUTE,
    decide_write,
)

_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)
_LATER = datetime(2024, 8, 26, 12, tzinfo=UTC)


def _entity_key(user_id: str = "1") -> EntityKey:
    return EntityKey.from_mapping(
        {"user_id": user_id, "application_id": "A1"},
        key_order=["user_id", "application_id"],
    )


def _result(
    name="declared_income",
    version=1,
    *,
    key=None,
    value=3_500_000,
    data_ts=_TS,
    max_input_data_ts=None,
    input_fingerprint=None,
):
    return FeatureResult(
        ref=FeatureRef(name, version),
        entity_key=key or _entity_key(),
        value=value,
        data_ts=data_ts,
        calc_ts=_TS,
        max_input_data_ts=max_input_data_ts,
        input_fingerprint=input_fingerprint,
    )


class _FakeValkeyClient:
    """Emulates the atomic D9 Lua script against an in-memory hash.

    The decision itself is delegated to the shared pure helper
    (``fs_core.write_guard.decide_write``), so the fake cannot drift from the
    InMemory store; the real Lua is covered by the gated integration test below.
    """

    _CODES = {SKIPPED_STALE: 0, WRITTEN: 1, NOOP: 2, WRITTEN_RECOMPUTE: 3}

    def __init__(self) -> None:
        self.store: dict[str, dict[str, str]] = {}
        self.eval_calls = 0

    def eval(self, script, numkeys, *args):
        self.eval_calls += 1
        key, field, value_json, epoch_us, max_epoch_us, fingerprint, available = args
        fields = self.store.setdefault(key, {})
        current = fields.get(field)
        current_tuple = None
        current_fp = None
        current_trusted_available = None
        if current is not None:
            stored = json.loads(current)
            cur_dts = int(stored["data_ts_epoch_us"])
            # Legacy-record fallback: missing max degenerates to data_ts.
            cur_max = stored.get("max_input_data_ts_epoch_us")
            current_tuple = (cur_dts, int(cur_max) if cur_max is not None else cur_dts)
            current_fp = stored.get("input_fingerprint")
            # Only a trusted (source_provided) stored availability joins the replay
            # identity — mirrors the Lua guard.
            if stored.get("availability_source") == "source_provided":
                current_trusted_available = stored.get("available_at")
        outcome = decide_write(
            (int(epoch_us), int(max_epoch_us)),
            str(fingerprint) or None,
            current_tuple,
            current_fp,
            incoming_available_at=str(available) or None,
            current_available_at=current_trusted_available,
        )
        if outcome in (WRITTEN, WRITTEN_RECOMPUTE):
            fields[field] = value_json
        return self._CODES[outcome]

    def hget(self, key, field):
        return self.store.get(key, {}).get(field)


def _store() -> tuple[ValkeyOnlineStore, _FakeValkeyClient]:
    client = _FakeValkeyClient()
    return ValkeyOnlineStore(client), client


def _stored_record(client, field: str = "declared_income:v1") -> dict:
    key = _key("v", 1, "user_id=1|application_id=A1")
    return json.loads(client.store[key][field])


def test_module_imports_without_redis():
    assert "redis" not in sys.modules


def test_key_builder():
    assert _key("user_credit_risk", 1, "user_id=1|application_id=A1") == (
        "fs:online:user_credit_risk:v1:user_id=1|application_id=A1"
    )


def test_field_builder():
    assert _field("declared_income", 1) == "declared_income:v1"


def test_first_write_succeeds_and_get_returns_result():
    store, _ = _store()
    assert store.write("v", 1, _result(value=10)) == WRITTEN
    got = store.get("v", 1, _entity_key(), "declared_income", 1)
    assert got == _result(value=10)


def test_newer_data_ts_overwrites():
    store, _ = _store()
    store.write("v", 1, _result(value=1, data_ts=_TS))
    assert store.write("v", 1, _result(value=2, data_ts=_LATER)) == WRITTEN
    assert store.get("v", 1, _entity_key(), "declared_income", 1).value == 2


def test_older_data_ts_does_not_overwrite():
    store, _ = _store()
    store.write("v", 1, _result(value=2, data_ts=_LATER))
    assert store.write("v", 1, _result(value=1, data_ts=_TS)) == SKIPPED_STALE
    assert store.get("v", 1, _entity_key(), "declared_income", 1).value == 2


def test_equal_data_ts_without_fingerprints_is_noop():
    store, _ = _store()
    store.write("v", 1, _result(value=1, data_ts=_TS))
    assert store.write("v", 1, _result(value=99, data_ts=_TS)) == NOOP
    assert store.get("v", 1, _entity_key(), "declared_income", 1).value == 1


def test_write_many_keyed_by_feature_ref():
    store, _ = _store()
    written = store.write_many(
        "v", 1, [_result("declared_income"), _result("monthly_obligations", value=700_000)]
    )
    assert written == {"declared_income:v1": WRITTEN, "monthly_obligations:v1": WRITTEN}


def test_get_missing_returns_none():
    store, _ = _store()
    assert store.get("v", 1, _entity_key(), "declared_income", 1) is None


def test_non_json_value_raises_value_error():
    store, _ = _store()
    with pytest.raises(ValueError):
        store.write("v", 1, _result(value=object()))


def test_none_value_round_trips_as_none():
    store, client = _store()
    store.write("v", 1, _result(value=None))
    stored = _stored_record(client)
    assert stored["value"] is None  # JSON null, not a stringified value
    assert store.get("v", 1, _entity_key(), "declared_income", 1).value is None


def test_malformed_stored_json_on_get_raises_value_error():
    store, client = _store()
    client.store[_key("v", 1, "user_id=1|application_id=A1")] = {"declared_income:v1": "not json{"}
    with pytest.raises(ValueError):
        store.get("v", 1, _entity_key(), "declared_income", 1)


def test_write_uses_atomic_eval():
    store, client = _store()
    store.write("v", 1, _result())
    assert client.eval_calls == 1


def test_epoch_compare_uses_integer_microseconds():
    store, client = _store()
    store.write("v", 1, _result())
    stored = _stored_record(client)
    assert isinstance(stored["data_ts_epoch_us"], int)


@pytest.mark.skipif(
    os.getenv("FSP_VALKEY_INTEGRATION") != "1",
    reason="set FSP_VALKEY_INTEGRATION=1 (and run local Valkey) to enable",
)
def test_live_valkey_cas():
    pytest.importorskip("redis")
    from fintech_feature_platform.fs_core.stores.valkey_online import connect_valkey

    client = connect_valkey()
    store = ValkeyOnlineStore(client)
    key = _entity_key(user_id=uuid.uuid4().hex)
    assert store.write("v", 1, _result(key=key, value=1, data_ts=_TS)) == WRITTEN
    assert store.write("v", 1, _result(key=key, value=2, data_ts=_LATER)) == WRITTEN
    assert store.write("v", 1, _result(key=key, value=3, data_ts=_TS)) == SKIPPED_STALE
    # D9: equal tuple, changed fingerprint -> same-freshness recompute (real Lua).
    assert (
        store.write(
            "v", 1,
            _result(key=key, value=4, data_ts=_LATER, input_fingerprint="fp-live"),
        )
        == WRITTEN_RECOMPUTE
    )
    assert store.get("v", 1, key, "declared_income", 1).value == 4
