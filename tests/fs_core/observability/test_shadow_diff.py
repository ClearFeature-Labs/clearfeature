"""Shadow-vs-live diff by hashes/counts only."""

import json
from datetime import UTC, datetime

from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.observability.shadow_diff import diff_shadow_vs_live
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore

_TS = datetime(2026, 1, 10, tzinfo=UTC)


def _key(id_):
    return EntityKey.from_mapping({"id": id_}, key_order=["id"])


def _seed(offline, feature, id_, value_hash):
    offline.append("v", 1, FeatureResult(
        ref=FeatureRef(feature, 1), entity_key=_key(id_), value=1.0,
        data_ts=_TS, calc_ts=_TS, value_hash=value_hash,
    ))


def _diff(offline, ids):
    return diff_shadow_vs_live(
        offline, [_key(i) for i in ids], view="v", view_version=1,
        live_feature="ratio", shadow_feature="ratio_v2",
    )


def test_same_and_different_hashes_counted():
    offline = InMemoryOfflineStore()
    _seed(offline, "ratio", "1", "sha256:a")
    _seed(offline, "ratio_v2", "1", "sha256:a")   # same
    _seed(offline, "ratio", "2", "sha256:a")
    _seed(offline, "ratio_v2", "2", "sha256:b")   # different
    diff = _diff(offline, ["1", "2"])
    assert diff.total_compared == 2
    assert diff.same_hash == 1
    assert diff.different_hash == 1
    assert diff.sample_different == (_key("2").encode(),)


def test_missing_live_and_shadow_counted():
    offline = InMemoryOfflineStore()
    _seed(offline, "ratio", "1", "sha256:a")       # shadow missing
    _seed(offline, "ratio_v2", "2", "sha256:b")    # live missing
    diff = _diff(offline, ["1", "2"])
    assert diff.missing_shadow == 1
    assert diff.missing_live == 1
    assert diff.total_compared == 0


def test_output_has_no_feature_values():
    offline = InMemoryOfflineStore()
    _seed(offline, "ratio", "1", "sha256:a")
    _seed(offline, "ratio_v2", "1", "sha256:b")
    diff = _diff(offline, ["1"])
    blob = json.dumps(diff.to_dict())
    assert "1.0" not in blob  # the feature value never appears
    assert "value" not in blob
    assert diff.different_hash == 1
