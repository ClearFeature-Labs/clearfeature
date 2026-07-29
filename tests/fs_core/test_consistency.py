from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.consistency import (
    ConsistencyStatus,
    check_online_offline_consistency,
)
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.registry.loader import build_registry
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore

_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)
_LATER = datetime(2024, 8, 26, 12, tzinfo=UTC)
_VIEW = "user_credit_risk"


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
                        "feature_version": 1,
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


def _key():
    return EntityKey.from_mapping(
        {"user_id": "1", "application_id": "A1"},
        key_order=["user_id", "application_id"],
    )


def _result(value=100, *, data_ts=_TS):
    return FeatureResult(
        ref=FeatureRef("declared_income", 1),
        entity_key=_key(),
        value=value,
        data_ts=data_ts,
        calc_ts=_TS,
    )


def _check(offline, online):
    return check_online_offline_consistency(
        offline=offline,
        online=online,
        view=_view(),
        view_version=1,
        entity_key=_key(),
        feature_names=["declared_income"],
    ).items["declared_income"]


def test_ok_after_compute():
    offline, online = InMemoryOfflineStore(), InMemoryOnlineStore()
    offline.append(_VIEW, 1, _result(value=100))
    online.write(_VIEW, 1, _result(value=100))
    item = _check(offline, online)
    assert item.status == ConsistencyStatus.OK
    assert item.offline_value == 100 and item.online_value == 100
    assert item.offline_feature_version == 1 and item.online_feature_version == 1


def test_missing_both():
    item = _check(InMemoryOfflineStore(), InMemoryOnlineStore())
    assert item.status == ConsistencyStatus.MISSING_BOTH


def test_missing_online():
    offline = InMemoryOfflineStore()
    offline.append(_VIEW, 1, _result(value=100))
    item = _check(offline, InMemoryOnlineStore())
    assert item.status == ConsistencyStatus.MISSING_ONLINE


def test_missing_offline():
    online = InMemoryOnlineStore()
    online.write(_VIEW, 1, _result(value=100))
    item = _check(InMemoryOfflineStore(), online)
    assert item.status == ConsistencyStatus.MISSING_OFFLINE


def test_stale_online():
    offline, online = InMemoryOfflineStore(), InMemoryOnlineStore()
    online.write(_VIEW, 1, _result(value=100, data_ts=_TS))
    offline.append(_VIEW, 1, _result(value=200, data_ts=_LATER))  # newer offline
    item = _check(offline, online)
    assert item.status == ConsistencyStatus.STALE_ONLINE


def test_online_newer_than_offline():
    offline, online = InMemoryOfflineStore(), InMemoryOnlineStore()
    offline.append(_VIEW, 1, _result(value=100, data_ts=_TS))
    online.write(_VIEW, 1, _result(value=200, data_ts=_LATER))  # newer online
    item = _check(offline, online)
    assert item.status == ConsistencyStatus.ONLINE_NEWER_THAN_OFFLINE


def test_value_mismatch():
    offline, online = InMemoryOfflineStore(), InMemoryOnlineStore()
    offline.append(_VIEW, 1, _result(value=100, data_ts=_TS))
    online.write(_VIEW, 1, _result(value=999, data_ts=_TS))  # same data_ts, diff value
    item = _check(offline, online)
    assert item.status == ConsistencyStatus.VALUE_MISMATCH


def test_latest_offline_is_max_data_ts():
    offline, online = InMemoryOfflineStore(), InMemoryOnlineStore()
    # Append a newer record first, then an older one (out of data_ts order).
    offline.append(_VIEW, 1, _result(value=200, data_ts=_LATER))
    offline.append(_VIEW, 1, _result(value=100, data_ts=_TS))
    online.write(_VIEW, 1, _result(value=200, data_ts=_LATER))
    item = _check(offline, online)
    assert item.status == ConsistencyStatus.OK  # compared against max-data_ts (200)
    assert item.offline_value == 200


def test_latest_offline_tie_break_is_last_appended():
    offline, online = InMemoryOfflineStore(), InMemoryOnlineStore()
    # Two records with the SAME data_ts; the last appended should win.
    offline.append(_VIEW, 1, _result(value=100, data_ts=_TS))
    offline.append(_VIEW, 1, _result(value=300, data_ts=_TS))
    online.write(_VIEW, 1, _result(value=300, data_ts=_TS))  # value won't CAS-update here
    item = _check(offline, online)
    assert item.offline_value == 300
    assert item.status == ConsistencyStatus.OK


def test_unknown_feature_raises_value_error():
    with pytest.raises(ValueError):
        check_online_offline_consistency(
            offline=InMemoryOfflineStore(),
            online=InMemoryOnlineStore(),
            view=_view(),
            view_version=1,
            entity_key=_key(),
            feature_names=["does_not_exist"],
        )
