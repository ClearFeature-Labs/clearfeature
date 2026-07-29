from datetime import UTC, datetime
from pathlib import Path

from fintech_feature_platform.fs_core.compute.context import RequestContext
from fintech_feature_platform.fs_core.compute.engine import ComputeCore
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureRef,
    FeatureResult,
    SourceStamp,
)
from fintech_feature_platform.fs_core.registry.loader import load_registry_file
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore
from fintech_feature_platform.fs_core.write_guard import (
    NOOP,
    SKIPPED_STALE,
    WRITTEN,
    WRITTEN_RECOMPUTE,
)

_EXAMPLE = (
    Path(__file__).resolve().parents[3]
    / "examples"
    / "registry"
    / "minimal_credit_risk.yaml"
)
_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)
_LATER = _TS.replace(hour=12)


def _key(user_id: str = "1") -> EntityKey:
    return EntityKey.from_mapping({"user_id": user_id})


def _result(
    name,
    version=1,
    *,
    key=None,
    value=1,
    data_ts=_TS,
    max_input_data_ts=None,
    input_fingerprint=None,
):
    return FeatureResult(
        ref=FeatureRef(name, version),
        entity_key=key or _key(),
        value=value,
        data_ts=data_ts,
        calc_ts=_TS,
        max_input_data_ts=max_input_data_ts,
        input_fingerprint=input_fingerprint,
    )


def test_write_new_result():
    store = InMemoryOnlineStore()
    assert store.write("v", 1, _result("a", value=10)) == WRITTEN
    got = store.get("v", 1, _key(), "a", 1)
    assert got is not None and got.value == 10


def test_cas_update_when_newer():
    store = InMemoryOnlineStore()
    store.write("v", 1, _result("a", value=1, data_ts=_TS))
    assert store.write("v", 1, _result("a", value=2, data_ts=_LATER)) == WRITTEN
    assert store.get("v", 1, _key(), "a", 1).value == 2


def test_cas_skip_when_older():
    store = InMemoryOnlineStore()
    store.write("v", 1, _result("a", value=2, data_ts=_LATER))
    assert store.write("v", 1, _result("a", value=1, data_ts=_TS)) == SKIPPED_STALE
    assert store.get("v", 1, _key(), "a", 1).value == 2


def test_cas_skip_when_equal():
    store = InMemoryOnlineStore()
    store.write("v", 1, _result("a", value=1, data_ts=_TS))
    assert store.write("v", 1, _result("a", value=99, data_ts=_TS)) == NOOP
    assert store.get("v", 1, _key(), "a", 1).value == 1


def test_feature_versions_separate():
    store = InMemoryOnlineStore()
    store.write("v", 1, _result("a", 1, value=11))
    store.write("v", 1, _result("a", 2, value=22))
    assert store.get("v", 1, _key(), "a", 1).value == 11
    assert store.get("v", 1, _key(), "a", 2).value == 22
    # Updating v1 must not touch v2.
    store.write("v", 1, _result("a", 1, value=111, data_ts=_LATER))
    assert store.get("v", 1, _key(), "a", 2).value == 22


def test_view_versions_separate():
    store = InMemoryOnlineStore()
    store.write("v", 1, _result("a", value=1))
    store.write("v", 2, _result("a", value=2))
    assert store.get("v", 1, _key(), "a", 1).value == 1
    assert store.get("v", 2, _key(), "a", 1).value == 2
    # A newer write under view_version 2 must not affect view_version 1.
    store.write("v", 2, _result("a", value=22, data_ts=_LATER))
    assert store.get("v", 1, _key(), "a", 1).value == 1


def _declared_income(sources, deps):
    return sources["credit_report"]["declared_income"]


def test_compute_output_written_to_both_stores():
    registry = load_registry_file(_EXAMPLE)
    core = ComputeCore(
        registry, UdfRegistry({"udf.credit.declared_income": _declared_income})
    )
    ctx = RequestContext(
        lambda name: {"credit_report": {"declared_income": 3_500_000}}[name]
    )
    key = EntityKey.from_mapping(
        {"user_id": "1", "application_id": "A1", "report_id": "R9"},
        key_order=["user_id", "application_id", "report_id"],
    )
    results = core.compute(
        view="user_credit_risk",
        view_version=1,
        entity_key=key,
        requested_features=["declared_income"],
        context=ctx,
        source_stamps={
            "credit_report": SourceStamp(report_ts=_TS, content_hash="sha256:credit")
        },
        calc_ts=_TS,
    )

    offline = InMemoryOfflineStore()
    online = InMemoryOnlineStore()
    offline.append_many("user_credit_risk", 1, results.values())
    written = online.write_many("user_credit_risk", 1, results.values())

    assert len(offline.get(key, view="user_credit_risk", view_version=1)) == 1
    assert written["declared_income:v1"] == WRITTEN
    assert online.get("user_credit_risk", 1, key, "declared_income", 1).value == 3_500_000


# --- D9 write guard  ----------------------

_JAN1 = datetime(2026, 1, 1, tzinfo=UTC)
_JAN5 = datetime(2026, 1, 5, tzinfo=UTC)
_JAN10 = datetime(2026, 1, 10, tzinfo=UTC)
_JAN20 = datetime(2026, 1, 20, tzinfo=UTC)


def test_d9_case_a_non_min_input_update_is_written():
    store = InMemoryOnlineStore()
    store.write(
        "v", 1,
        _result("c", value=1, data_ts=_JAN1, max_input_data_ts=_JAN10,
                input_fingerprint="fp1"),
    )
    # B updates Jan10 -> Jan20: min unchanged, max fresher -> write (the bug bare
    # CAS-on-data_ts would skip).
    outcome = store.write(
        "v", 1,
        _result("c", value=2, data_ts=_JAN1, max_input_data_ts=_JAN20,
                input_fingerprint="fp2"),
    )
    assert outcome == WRITTEN
    assert store.get("v", 1, _key(), "c", 1).value == 2


def test_d9_case_b_historical_wave_is_skipped_stale():
    store = InMemoryOnlineStore()
    store.write(
        "v", 1,
        _result("c", value=2, data_ts=_JAN1, max_input_data_ts=_JAN20,
                input_fingerprint="fp1"),
    )
    outcome = store.write(
        "v", 1,
        _result("c", value=1, data_ts=_JAN1, max_input_data_ts=_JAN5,
                input_fingerprint="fp0"),
    )
    assert outcome == SKIPPED_STALE
    assert store.get("v", 1, _key(), "c", 1).value == 2


def test_d9_case_c_equal_tuple_changed_fingerprint_is_written_recompute():
    store = InMemoryOnlineStore()
    store.write(
        "v", 1,
        _result("c", value=1, data_ts=_JAN1, max_input_data_ts=_JAN20,
                input_fingerprint="fp1"),
    )
    outcome = store.write(
        "v", 1,
        _result("c", value=9, data_ts=_JAN1, max_input_data_ts=_JAN20,
                input_fingerprint="fp2"),
    )
    assert outcome == WRITTEN_RECOMPUTE
    assert store.get("v", 1, _key(), "c", 1).value == 9


def test_d9_identical_replay_is_noop():
    store = InMemoryOnlineStore()
    result = _result("c", value=1, data_ts=_JAN1, max_input_data_ts=_JAN20,
                     input_fingerprint="fp1")
    assert store.write("v", 1, result) == WRITTEN
    assert store.write("v", 1, result) == NOOP
    assert store.get("v", 1, _key(), "c", 1).value == 1


def test_d9_f1_degenerates_to_data_ts_cas():
    store = InMemoryOnlineStore()
    # No max_input_data_ts -> tuple degenerates to (data_ts, data_ts).
    assert store.write("v", 1, _result("a", value=1, data_ts=_TS)) == WRITTEN
    assert store.write("v", 1, _result("a", value=2, data_ts=_LATER)) == WRITTEN
    assert store.write("v", 1, _result("a", value=0, data_ts=_TS)) == SKIPPED_STALE
    # Equal ts + changed fingerprint: same-freshness correction is accepted.
    assert (
        store.write("v", 1, _result("a", value=3, data_ts=_LATER,
                                    input_fingerprint="fp-new"))
        == WRITTEN_RECOMPUTE
    )
    assert store.get("v", 1, _key(), "a", 1).value == 3
