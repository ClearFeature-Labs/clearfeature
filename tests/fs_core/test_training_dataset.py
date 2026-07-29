from datetime import UTC, datetime, timedelta

import pytest

from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.registry.loader import build_registry
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.training import (
    TrainingCellStatus,
    TrainingObservation,
    build_training_dataset,
)

_VIEW = "user_credit_risk"
_T10 = datetime(2026, 6, 21, 10, tzinfo=UTC)
_T12 = datetime(2026, 6, 21, 12, tzinfo=UTC)
_T14 = datetime(2026, 6, 21, 14, tzinfo=UTC)
_OBS = datetime(2026, 6, 21, 13, tzinfo=UTC)
_KEY_ORDER = ["user_id", "application_id"]


def _registry():
    data = {
        "registry_version": "test-v1",
        "entities": {"application": {"key_fields": ["user_id", "application_id"]}},
        "sources": {
            "credit_report": {
                "type": "raw_report",
                "report_type": "credit_report",
                "ts_field": "report_ts",
            }
        },
        "feature_views": {
            _VIEW: {
                "entity": "application",
                "key_fields": ["user_id", "application_id"],
                "view_version": 1,
                "owner": "t",
                "status": "active",
                "features": {
                    "declared_income": {
                        "kind": "udf",
                        "feature_version": 2,  # registry declares v2
                        "udf": "udf.credit.declared_income",
                        "inputs": ["credit_report"],
                        "dtype": "int",
                        "status": "active",
                    }
                },
            }
        },
    }
    return build_registry(data)


def _view():
    return _registry().feature_views[0]


def _key(user_id="1"):
    return EntityKey.from_mapping(
        {"user_id": user_id, "application_id": "A1"}, key_order=_KEY_ORDER
    )


def _record(value, *, data_ts, calc_ts=None, version=2, user_id="1"):
    return FeatureResult(
        ref=FeatureRef("declared_income", version),
        entity_key=_key(user_id),
        value=value,
        data_ts=data_ts,
        calc_ts=calc_ts if calc_ts is not None else data_ts,
    )


def _obs(user_id="1", *, observation_ts=_OBS, context=None):
    return TrainingObservation(
        entity={"user_id": user_id, "application_id": "A1"},
        observation_ts=observation_ts,
        context=context,
    )


def _build(
    offline, observations, *, features=None, missing_policy="keep_null", safety_gap=None
):
    kwargs = {}
    if safety_gap is not None:
        kwargs["safety_gap"] = safety_gap
    return build_training_dataset(
        offline=offline,
        view=_view(),
        view_version=1,
        feature_names=features or ["declared_income"],
        observations=observations,
        missing_policy=missing_policy,
        **kwargs,
    )


def test_single_observation_ok():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(1000, data_ts=_T12))
    dataset = _build(offline, [_obs()])
    row = dataset.rows[0]
    assert row.features["declared_income"] == 1000
    cell = row.feature_metadata["declared_income"]
    assert cell.status == TrainingCellStatus.OK
    assert cell.feature_version == 2
    assert cell.data_ts == _T12
    assert dataset.summary == {
        "rows": 1, "features": 1, "missing_values": 0, "future_records_ignored": 0,
        "safety_gap_seconds": 0,
    }


def test_pit_chooses_latest_at_or_before_and_ignores_future():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(100, data_ts=_T10))
    offline.append(_VIEW, 1, _record(200, data_ts=_T12))  # latest <= obs(13:00)
    offline.append(_VIEW, 1, _record(999, data_ts=_T14))  # future -> ignored
    dataset = _build(offline, [_obs()])
    assert dataset.rows[0].features["declared_income"] == 200
    assert dataset.summary["future_records_ignored"] == 1


def test_inclusive_boundary_selects_equal_data_ts():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(500, data_ts=_OBS))  # data_ts == observation_ts
    dataset = _build(offline, [_obs()])
    assert dataset.rows[0].features["declared_income"] == 500
    assert dataset.summary["future_records_ignored"] == 0


def test_late_arriving_older_record_does_not_break_pit():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(200, data_ts=_T12))  # newer, appended first
    offline.append(_VIEW, 1, _record(100, data_ts=_T10))  # older, appended later
    dataset = _build(offline, [_obs()])
    assert dataset.rows[0].features["declared_income"] == 200  # max data_ts wins


def test_tie_break_last_append_for_same_data_ts():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(100, data_ts=_T12))
    offline.append(_VIEW, 1, _record(300, data_ts=_T12))  # same data_ts, last appended
    dataset = _build(offline, [_obs()])
    assert dataset.rows[0].features["declared_income"] == 300


def test_multiple_observations():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(100, data_ts=_T12, user_id="1"))
    offline.append(_VIEW, 1, _record(200, data_ts=_T12, user_id="2"))
    dataset = _build(offline, [_obs("1"), _obs("2")])
    assert [r.features["declared_income"] for r in dataset.rows] == [100, 200]
    assert dataset.summary["rows"] == 2


def test_missing_keep_null():
    dataset = _build(InMemoryOfflineStore(), [_obs()])
    row = dataset.rows[0]
    assert row.features["declared_income"] is None
    assert row.feature_metadata["declared_income"].status == TrainingCellStatus.MISSING
    assert row.feature_metadata["declared_income"].data_ts is None
    assert dataset.summary["missing_values"] == 1


def test_missing_error_policy_raises():
    with pytest.raises(ValueError):
        _build(InMemoryOfflineStore(), [_obs()], missing_policy="error")


def test_unknown_feature_raises():
    with pytest.raises(ValueError):
        _build(InMemoryOfflineStore(), [_obs()], features=["nope"])


def test_invalid_missing_policy_raises():
    with pytest.raises(ValueError):
        _build(InMemoryOfflineStore(), [_obs()], missing_policy="bogus")


def test_context_preserved():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(1000, data_ts=_T12))
    dataset = _build(
        offline, [_obs(context={"request_id": "req_1", "segment": "new"})]
    )
    row = dataset.rows[0]
    assert row.context == {"request_id": "req_1", "segment": "new"}
    assert not hasattr(row, "label")


def test_older_feature_version_not_silently_used():
    offline = InMemoryOfflineStore()
    # Only a v1 record exists, but the registry declares v2 -> must be missing.
    offline.append(_VIEW, 1, _record(1000, data_ts=_T12, version=1))
    dataset = _build(offline, [_obs()])
    assert dataset.rows[0].features["declared_income"] is None
    assert dataset.rows[0].feature_metadata["declared_income"].status == (
        TrainingCellStatus.MISSING
    )


# --- safety_gap + strict availability  -----------------------------

def test_safety_gap_excludes_data_too_close_to_observation():
    offline = InMemoryOfflineStore()
    # data_ts 12:00, observation 13:00 -> only 1h old. A 2h safety_gap excludes it.
    offline.append(_VIEW, 1, _record(200, data_ts=_T12))
    dataset = _build(offline, [_obs()], safety_gap=timedelta(hours=2))
    assert dataset.rows[0].features["declared_income"] is None


def test_safety_gap_keeps_data_old_enough():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(100, data_ts=_T10))  # 3h before obs
    offline.append(_VIEW, 1, _record(200, data_ts=_T12))  # 1h before obs
    # 2h gap: cutoff = 11:00 -> only the 10:00 row qualifies.
    dataset = _build(offline, [_obs()], safety_gap=timedelta(hours=2))
    assert dataset.rows[0].features["declared_income"] == 100
    assert dataset.summary["safety_gap_seconds"] == 7200


def test_calc_ts_after_observation_is_excluded_even_if_data_ts_old_enough():
    # leakage example: data_ts old enough, but computed AFTER the observation.
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(999, data_ts=_T10, calc_ts=_T14))  # calc after obs
    dataset = _build(offline, [_obs()])
    assert dataset.rows[0].features["declared_income"] is None
    assert dataset.rows[0].feature_metadata["declared_income"].status == (
        TrainingCellStatus.MISSING
    )


def test_calc_ts_leakage_row_ignored_but_older_eligible_row_used():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(100, data_ts=_T10, calc_ts=_T10))  # eligible
    offline.append(_VIEW, 1, _record(999, data_ts=_T12, calc_ts=_T14))  # calc after obs
    dataset = _build(offline, [_obs()])
    # The fresher-data_ts row was computed after the observation -> use the older one.
    assert dataset.rows[0].features["declared_income"] == 100


def test_negative_safety_gap_rejected():
    with pytest.raises(ValueError, match="safety_gap"):
        _build(InMemoryOfflineStore(), [_obs()], safety_gap=timedelta(seconds=-1))


def test_naive_observation_ts_rejected():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(100, data_ts=_T12))
    naive = TrainingObservation(
        entity={"user_id": "1", "application_id": "A1"},
        observation_ts=datetime(2026, 6, 21, 13),  # noqa: DTZ001 - naive on purpose
    )
    with pytest.raises(ValueError, match="observation_ts"):
        _build(offline, [naive])


def test_default_safety_gap_zero_preserves_behavior():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _record(200, data_ts=_T12))
    dataset = _build(offline, [_obs()])  # no safety_gap
    assert dataset.rows[0].features["declared_income"] == 200
    assert dataset.summary["safety_gap_seconds"] == 0
