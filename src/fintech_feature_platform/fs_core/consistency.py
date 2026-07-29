"""Read-only online/offline consistency check.

Answers a single trust question for an entity/view/feature: does the online latest
value agree with the latest auditable offline value? It only **reads** persisted state
from the offline and online stores and compares — it never recomputes, never calls
ComputeCore/FeatureStore, and never mutates the stores.

This is not monitoring/alerting; it is a point-in-time comparison.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.registry.models import FeatureViewDef


class ConsistencyStatus:
    OK = "ok"
    MISSING_ONLINE = "missing_online"
    MISSING_OFFLINE = "missing_offline"
    MISSING_BOTH = "missing_both"
    STALE_ONLINE = "stale_online"
    ONLINE_NEWER_THAN_OFFLINE = "online_newer_than_offline"
    VALUE_MISMATCH = "value_mismatch"


@dataclass(frozen=True)
class FeatureConsistencyItem:
    feature_name: str
    status: str
    offline_value: Any
    online_value: Any
    offline_data_ts: datetime | None
    online_data_ts: datetime | None
    offline_feature_version: int | None
    online_feature_version: int | None


@dataclass(frozen=True)
class FeatureConsistencyReport:
    status: str  # "ok" if every item is ok, else "inconsistent"
    items: dict[str, FeatureConsistencyItem]


def check_online_offline_consistency(
    *,
    offline,
    online,
    view: FeatureViewDef,
    view_version: int,
    entity_key: EntityKey,
    feature_names: list[str],
) -> FeatureConsistencyReport:
    """Compare persisted offline history vs online latest for each feature name.

    Raises ``ValueError`` for a feature name not declared in the view.
    """
    features_by_name = {feature.name: feature for feature in view.features}

    items: dict[str, FeatureConsistencyItem] = {}
    for name in feature_names:
        feature = features_by_name.get(name)
        if feature is None:
            raise ValueError(f"unknown feature {name!r}")
        version = feature.feature_version

        records = offline.get(
            entity_key,
            feature_name=name,
            feature_version=version,
            view=view.name,
            view_version=view_version,
        )
        latest_offline = _latest_offline(records)
        online_result = online.get(view.name, view_version, entity_key, name, version)
        items[name] = _classify(name, latest_offline, online_result)

    overall = (
        ConsistencyStatus.OK
        if all(item.status == ConsistencyStatus.OK for item in items.values())
        else "inconsistent"
    )
    return FeatureConsistencyReport(status=overall, items=items)


def _latest_offline(records: Sequence):
    # Latest = max data_ts; ties broken by last in append/row order (offline is
    # append-only and may contain late-arriving older reports, so last-appended is not
    # necessarily newest by data_ts).
    latest = None
    for record in records:
        if latest is None or record.result.data_ts >= latest.result.data_ts:
            latest = record
    return latest


def _classify(name: str, latest_offline, online_result) -> FeatureConsistencyItem:
    offline_present = latest_offline is not None
    online_present = online_result is not None

    offline_value = latest_offline.result.value if offline_present else None
    online_value = online_result.value if online_present else None
    offline_data_ts = latest_offline.result.data_ts if offline_present else None
    online_data_ts = online_result.data_ts if online_present else None
    offline_version = latest_offline.result.ref.version if offline_present else None
    online_version = online_result.ref.version if online_present else None

    if not offline_present and not online_present:
        status = ConsistencyStatus.MISSING_BOTH
    elif not online_present:
        status = ConsistencyStatus.MISSING_ONLINE
    elif not offline_present:
        status = ConsistencyStatus.MISSING_OFFLINE
    elif online_data_ts < offline_data_ts:
        status = ConsistencyStatus.STALE_ONLINE
    elif online_data_ts > offline_data_ts:
        status = ConsistencyStatus.ONLINE_NEWER_THAN_OFFLINE
    elif offline_value != online_value:
        status = ConsistencyStatus.VALUE_MISMATCH
    else:
        status = ConsistencyStatus.OK

    return FeatureConsistencyItem(
        feature_name=name,
        status=status,
        offline_value=offline_value,
        online_value=online_value,
        offline_data_ts=offline_data_ts,
        online_data_ts=online_data_ts,
        offline_feature_version=offline_version,
        online_feature_version=online_version,
    )
