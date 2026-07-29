from datetime import UTC, datetime, timedelta

from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore

_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)
_T12 = datetime(2024, 8, 26, 12, tzinfo=UTC)
_T14 = datetime(2024, 8, 26, 14, tzinfo=UTC)


def _key(user_id: str = "1") -> EntityKey:
    return EntityKey.from_mapping({"user_id": user_id})


def _result(name, version=1, *, key=None, value=1, data_ts=_TS, calc_ts=_TS):
    return FeatureResult(
        ref=FeatureRef(name, version),
        entity_key=key or _key(),
        value=value,
        data_ts=data_ts,
        calc_ts=calc_ts,
    )


def test_append_multiple_results():
    store = InMemoryOfflineStore()
    store.append("v", 1, _result("a"))
    store.append("v", 1, _result("b"))
    assert len(store.get(_key())) == 2


def test_append_many_records_view_and_version():
    store = InMemoryOfflineStore()
    store.append_many("v", 1, [_result("a"), _result("b")])
    records = store.get(_key())
    assert {r.result.ref.name for r in records} == {"a", "b"}
    assert all(r.view == "v" and r.view_version == 1 for r in records)


def test_preserves_history_for_recomputed_feature():
    store = InMemoryOfflineStore()
    store.append("v", 1, _result("a", value=1, data_ts=_TS))
    store.append("v", 1, _result("a", value=2, data_ts=_TS.replace(hour=12)))
    records = store.get(_key(), feature_name="a")
    # Both rows kept, in append order; nothing overwritten.
    assert [r.result.value for r in records] == [1, 2]


def test_get_filters_by_entity_view_feature():
    store = InMemoryOfflineStore()
    store.append("credit", 1, _result("a", key=_key("1")))
    store.append("credit", 1, _result("a", key=_key("2")))
    store.append("other", 1, _result("a", key=_key("1")))

    assert len(store.get(_key("1"))) == 2
    assert len(store.get(_key("2"))) == 1
    assert len(store.get(_key("1"), view="credit")) == 1
    assert len(store.get(_key("1"), view="other")) == 1
    assert len(store.get(_key("1"), feature_name="a")) == 2
    assert store.get(_key("1"), feature_name="missing") == []


def test_feature_versions_stored_separately():
    store = InMemoryOfflineStore()
    store.append("v", 1, _result("a", 1))
    store.append("v", 1, _result("a", 2))
    assert len(store.get(_key(), feature_name="a")) == 2
    assert len(store.get(_key(), feature_name="a", feature_version=1)) == 1
    assert len(store.get(_key(), feature_name="a", feature_version=2)) == 1


def test_view_versions_stored_separately():
    store = InMemoryOfflineStore()
    store.append("v", 1, _result("a"))
    store.append("v", 2, _result("a"))
    assert len(store.get(_key(), view="v", view_version=1)) == 1
    assert len(store.get(_key(), view="v", view_version=2)) == 1
    assert len(store.get(_key(), view="v")) == 2


# --- get_pit  ------------------------------------------------------

def _get_pit(store, observation_ts, safety_gap=timedelta(0)):
    return store.get_pit(
        _key(),
        feature_name="a",
        feature_version=1,
        view="v",
        view_version=1,
        observation_ts=observation_ts,
        safety_gap=safety_gap,
    )


def test_get_pit_selects_latest_eligible():
    store = InMemoryOfflineStore()
    store.append("v", 1, _result("a", value=100, data_ts=_TS))
    store.append("v", 1, _result("a", value=200, data_ts=_T12))
    store.append("v", 1, _result("a", value=999, data_ts=_T14))  # future -> excluded
    selected = _get_pit(store, datetime(2024, 8, 26, 13, tzinfo=UTC))
    assert selected.result.value == 200


def test_get_pit_applies_safety_gap():
    store = InMemoryOfflineStore()
    store.append("v", 1, _result("a", value=200, data_ts=_T12))
    # obs 13:00 with a 2h gap -> cutoff 11:00 excludes the 12:00 row.
    selected = _get_pit(
        store, datetime(2024, 8, 26, 13, tzinfo=UTC), safety_gap=timedelta(hours=2)
    )
    assert selected is None


def test_get_pit_applies_calc_ts_availability():
    store = InMemoryOfflineStore()
    store.append("v", 1, _result("a", value=999, data_ts=_TS, calc_ts=_T14))
    # obs 13:00: data_ts old enough but calc_ts 14:00 is after -> excluded.
    assert _get_pit(store, datetime(2024, 8, 26, 13, tzinfo=UTC)) is None
