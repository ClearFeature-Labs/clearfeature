"""Pure D9 write-guard decision tests."""

from datetime import UTC, datetime

from fintech_feature_platform.fs_core.write_guard import (
    NOOP,
    SKIPPED_STALE,
    WRITTEN,
    WRITTEN_RECOMPUTE,
    aggregate_online_status,
    decide_write,
    guard_tuple,
)

_JAN1 = datetime(2026, 1, 1, tzinfo=UTC)
_JAN5 = datetime(2026, 1, 5, tzinfo=UTC)
_JAN10 = datetime(2026, 1, 10, tzinfo=UTC)
_JAN20 = datetime(2026, 1, 20, tzinfo=UTC)


def test_guard_tuple_degenerates_for_f1():
    assert guard_tuple(_JAN1, None) == (_JAN1, _JAN1)
    assert guard_tuple(_JAN1, _JAN10) == (_JAN1, _JAN10)


def test_no_current_value_writes():
    assert decide_write((_JAN1, _JAN10), "fp", None, None) == WRITTEN


def test_case_a_non_min_input_update_writes():
    # C=(Jan1, Jan10); B updates Jan10 -> Jan20 => (Jan1, Jan20) > current -> write.
    assert (
        decide_write((_JAN1, _JAN20), "fp2", (_JAN1, _JAN10), "fp1") == WRITTEN
    )


def test_case_b_historical_wave_rejected():
    # Current (Jan1, Jan20); wave resolves B at Jan5 => (Jan1, Jan5) < current.
    assert (
        decide_write((_JAN1, _JAN5), "fp0", (_JAN1, _JAN20), "fp1") == SKIPPED_STALE
    )


def test_case_c_equal_tuple_changed_fingerprint_writes_recompute():
    assert (
        decide_write((_JAN1, _JAN20), "fp2", (_JAN1, _JAN20), "fp1")
        == WRITTEN_RECOMPUTE
    )


def test_identical_replay_is_noop():
    assert decide_write((_JAN1, _JAN20), "fp1", (_JAN1, _JAN20), "fp1") == NOOP


def test_equal_tuple_missing_fingerprints_compare_as_empty():
    # Legacy records / scores without fingerprints: equal tuple -> noop (pre-D9 skip).
    assert decide_write((_JAN1, _JAN1), None, (_JAN1, _JAN1), None) == NOOP
    assert decide_write((_JAN1, _JAN1), "", (_JAN1, _JAN1), None) == NOOP
    # Incoming fingerprint against a legacy fingerprint-less record: recompute write.
    assert (
        decide_write((_JAN1, _JAN1), "fp", (_JAN1, _JAN1), None) == WRITTEN_RECOMPUTE
    )


def test_lexicographic_data_ts_dominates_max():
    # Fresher min wins even if max moves backwards (lexicographic, data_ts first).
    assert decide_write((_JAN5, _JAN10), "f", (_JAN1, _JAN20), "g") == WRITTEN
    assert decide_write((_JAN1, _JAN10), "f", (_JAN5, _JAN5), "g") == SKIPPED_STALE


def test_aggregate_online_status():
    assert aggregate_online_status([WRITTEN, SKIPPED_STALE, NOOP]) == WRITTEN
    assert aggregate_online_status([WRITTEN_RECOMPUTE, NOOP]) == WRITTEN
    assert aggregate_online_status([SKIPPED_STALE, NOOP]) == SKIPPED_STALE
    assert aggregate_online_status([NOOP, NOOP]) == NOOP
    assert aggregate_online_status([]) == NOOP
