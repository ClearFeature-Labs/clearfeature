from datetime import UTC, datetime

from fintech_feature_platform.fs_core.dedup import partition_new_results
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore

_VIEW = "v"
_T1 = datetime(2026, 1, 1, tzinfo=UTC)
_T2 = datetime(2026, 1, 2, tzinfo=UTC)


def _key():
    return EntityKey.from_mapping({"user_id": "1"})


def _res(value, ts, fingerprint=None):
    return FeatureResult(
        ref=FeatureRef("f", 1),
        entity_key=_key(),
        value=value,
        data_ts=ts,
        calc_ts=ts,
        input_fingerprint=fingerprint,
    )


def test_exact_duplicate_skipped():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _res(10, _T1))
    new, dup = partition_new_results(offline, _VIEW, 1, [_res(10, _T1)])
    assert new == [] and dup == 1


def test_same_data_ts_different_value_kept_as_correction():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _res(10, _T1))
    new, dup = partition_new_results(offline, _VIEW, 1, [_res(99, _T1)])
    assert len(new) == 1 and dup == 0


def test_same_value_different_data_ts_kept():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _res(10, _T1))
    new, dup = partition_new_results(offline, _VIEW, 1, [_res(10, _T2)])
    assert len(new) == 1 and dup == 0


def test_in_run_duplicate_skipped():
    offline = InMemoryOfflineStore()  # empty base; both candidates identical
    new, dup = partition_new_results(offline, _VIEW, 1, [_res(10, _T1), _res(10, _T1)])
    assert len(new) == 1 and dup == 1


# --- fingerprint-aware dedup (D9 Case C auditability,) ---------------

def test_same_value_same_ts_different_fingerprint_kept_as_recompute():
    # D9 Case C: equal freshness, equal output value, different inputs -> the
    # recompute must stay auditable in offline history, not be dedup-skipped.
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _res(10, _T1, fingerprint="fp1"))
    new, dup = partition_new_results(
        offline, _VIEW, 1, [_res(10, _T1, fingerprint="fp2")]
    )
    assert len(new) == 1 and dup == 0


def test_same_fingerprint_exact_replay_still_skipped():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _res(10, _T1, fingerprint="fp1"))
    new, dup = partition_new_results(
        offline, _VIEW, 1, [_res(10, _T1, fingerprint="fp1")]
    )
    assert new == [] and dup == 1


def test_missing_fingerprint_falls_back_to_pre_d9_rule():
    # Legacy offline rows have no fingerprint: an incoming identical value/ts is
    # still an exact duplicate (either side None -> old rule).
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _res(10, _T1))
    new, dup = partition_new_results(
        offline, _VIEW, 1, [_res(10, _T1, fingerprint="fp-new")]
    )
    assert new == [] and dup == 1
