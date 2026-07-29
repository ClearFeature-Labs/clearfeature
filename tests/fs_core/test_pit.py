"""Unit tests for the shared PIT eligibility rule (fs_core.pit)."""

from datetime import UTC, datetime, timedelta

import pytest

from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.pit import (
    effective_data_cutoff,
    is_pit_eligible,
    select_pit,
)
from fintech_feature_platform.fs_core.stores.offline import OfflineFeatureRecord

_OBS = datetime(2026, 7, 10, tzinfo=UTC)
_KEY = EntityKey.from_mapping({"user_id": "1"})


def _record(value, *, data_ts, calc_ts):
    return OfflineFeatureRecord(
        "v", 1,
        FeatureResult(
            ref=FeatureRef("f", 1), entity_key=_KEY, value=value,
            data_ts=data_ts, calc_ts=calc_ts,
        ),
    )


def test_effective_cutoff_subtracts_safety_gap():
    assert effective_data_cutoff(_OBS, timedelta(days=2)) == datetime(
        2026, 7, 8, tzinfo=UTC
    )


def test_effective_cutoff_rejects_naive_and_negative():
    with pytest.raises(ValueError, match="observation_ts"):
        effective_data_cutoff(datetime(2026, 7, 10), timedelta(0))  # noqa: DTZ001
    with pytest.raises(ValueError, match="safety_gap"):
        effective_data_cutoff(_OBS, timedelta(seconds=-1))


def test_eligible_when_both_clocks_clear():
    rec = _record(1, data_ts=datetime(2026, 7, 5, tzinfo=UTC),
                  calc_ts=datetime(2026, 7, 6, tzinfo=UTC))
    assert is_pit_eligible(rec, _OBS, timedelta(days=2)) is True


def test_ineligible_when_data_ts_within_safety_gap():
    # data_ts 2026-07-09 is < 2 days before obs -> excluded by the gap.
    rec = _record(1, data_ts=datetime(2026, 7, 9, tzinfo=UTC),
                  calc_ts=datetime(2026, 7, 9, tzinfo=UTC))
    assert is_pit_eligible(rec, _OBS, timedelta(days=2)) is False


def test_ineligible_when_calc_ts_after_observation():
    # leakage example: data old enough, but computed after the observation.
    rec = _record(1, data_ts=datetime(2026, 7, 5, tzinfo=UTC),
                  calc_ts=datetime(2026, 7, 11, tzinfo=UTC))
    assert is_pit_eligible(rec, _OBS, timedelta(days=2)) is False


def test_select_pit_picks_latest_eligible_by_data_ts_then_calc_ts():
    older = _record(100, data_ts=datetime(2026, 7, 1, tzinfo=UTC),
                    calc_ts=datetime(2026, 7, 1, tzinfo=UTC))
    newer = _record(200, data_ts=datetime(2026, 7, 5, tzinfo=UTC),
                    calc_ts=datetime(2026, 7, 5, tzinfo=UTC))
    future = _record(999, data_ts=datetime(2026, 7, 20, tzinfo=UTC),
                     calc_ts=datetime(2026, 7, 20, tzinfo=UTC))
    selected, ignored = select_pit([older, newer, future], _OBS, timedelta(0))
    assert selected.result.value == 200
    assert ignored == 1  # the future data_ts row


def test_select_pit_tie_break_prefers_last_in_order():
    a = _record(1, data_ts=datetime(2026, 7, 5, tzinfo=UTC),
                calc_ts=datetime(2026, 7, 5, tzinfo=UTC))
    b = _record(2, data_ts=datetime(2026, 7, 5, tzinfo=UTC),
                calc_ts=datetime(2026, 7, 5, tzinfo=UTC))
    selected, _ = select_pit([a, b], _OBS, timedelta(0))
    assert selected.result.value == 2


def test_select_pit_none_when_all_ineligible():
    calc_late = _record(1, data_ts=datetime(2026, 7, 1, tzinfo=UTC),
                        calc_ts=datetime(2026, 7, 15, tzinfo=UTC))
    selected, ignored = select_pit([calc_late], _OBS, timedelta(0))
    assert selected is None
    assert ignored == 0  # excluded by calc_ts, not counted as a future data_ts
