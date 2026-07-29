"""Point-in-time (PIT) training dataset builder v1.

For each observation row and requested feature, select the latest offline feature
record that is PIT-eligible (``fs_core.pit.is_pit_eligible``): both
``data_ts <= observation_ts - safety_gap`` and ``calc_ts <= observation_ts`` must hold.
A value that describes old data but was only computed after the observation is never
used — this is the anti-leakage guarantee.

Read-only: it reads offline history only and never recomputes, never calls
ComputeCore/FeatureStore, and never mutates stores. It reads all matching rows via
``offline.get`` and applies the shared eligibility rule in Python, so in-memory and
Postgres behavior are identical. Correct but not optimized for large datasets (one
``offline.get`` per observation/feature); the SQL-pushdown ``offline.get_pit`` applies
the same rule for the scaled path.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.pit import select_pit
from fintech_feature_platform.fs_core.registry.models import FeatureViewDef

_KEEP_NULL = "keep_null"
_ERROR = "error"
_MISSING_POLICIES = (_KEEP_NULL, _ERROR)


class TrainingCellStatus:
    OK = "ok"
    MISSING = "missing"


@dataclass(frozen=True)
class TrainingObservation:
    entity: dict[str, str]
    observation_ts: datetime
    context: dict[str, Any] | None = None


@dataclass(frozen=True)
class TrainingFeatureCell:
    status: str
    value: Any
    feature_version: int | None
    data_ts: datetime | None


@dataclass(frozen=True)
class TrainingDatasetRow:
    entity: dict[str, str]
    observation_ts: datetime
    context: dict[str, Any] | None
    features: dict[str, Any]
    feature_metadata: dict[str, TrainingFeatureCell]


@dataclass(frozen=True)
class TrainingDataset:
    rows: list[TrainingDatasetRow]
    summary: dict[str, int]


def build_training_dataset(
    *,
    offline,
    view: FeatureViewDef,
    view_version: int,
    feature_names: list[str],
    observations: list[TrainingObservation],
    missing_policy: str = _KEEP_NULL,
    safety_gap: timedelta = timedelta(0),
) -> TrainingDataset:
    """Build a leakage-safe PIT training dataset from offline history.

    ``safety_gap`` (default zero) is a conservative delay before the observation: a row
    is eligible only when ``data_ts <= observation_ts - safety_gap`` and
    ``calc_ts <= observation_ts``.

    Raises ``ValueError`` for an unknown feature, an invalid ``missing_policy``, a
    negative ``safety_gap``, a naive ``observation_ts``, or (when
    ``missing_policy="error"``) a feature with no eligible record for some observation.
    Raises ``KeyError`` if an observation's entity is missing a view key field.
    """
    if missing_policy not in _MISSING_POLICIES:
        raise ValueError(f"unknown missing_policy {missing_policy!r}")
    if safety_gap < timedelta(0):
        raise ValueError("safety_gap must be non-negative")

    features_by_name = {feature.name: feature for feature in view.features}
    versions: dict[str, int] = {}
    for name in feature_names:
        feature = features_by_name.get(name)
        if feature is None:
            raise ValueError(f"unknown feature {name!r}")
        versions[name] = feature.feature_version

    rows: list[TrainingDatasetRow] = []
    missing_values = 0
    future_records_ignored = 0

    for observation in observations:
        _require_aware(observation.observation_ts, "observation_ts")
        entity_key = EntityKey.from_mapping(
            {field: observation.entity[field] for field in view.key_fields},
            key_order=view.key_fields,
        )

        features: dict[str, Any] = {}
        metadata: dict[str, TrainingFeatureCell] = {}
        for name in feature_names:
            version = versions[name]
            records = offline.get(
                entity_key,
                feature_name=name,
                feature_version=version,
                view=view.name,
                view_version=view_version,
            )
            selected, ignored = select_pit(
                records, observation.observation_ts, safety_gap
            )
            future_records_ignored += ignored

            if selected is None:
                if missing_policy == _ERROR:
                    raise ValueError(
                        f"missing feature {name!r} for observation "
                        f"entity={observation.entity} "
                        f"observation_ts={observation.observation_ts.isoformat()}"
                    )
                features[name] = None
                metadata[name] = TrainingFeatureCell(
                    status=TrainingCellStatus.MISSING,
                    value=None,
                    feature_version=None,
                    data_ts=None,
                )
                missing_values += 1
            else:
                features[name] = selected.result.value
                metadata[name] = TrainingFeatureCell(
                    status=TrainingCellStatus.OK,
                    value=selected.result.value,
                    feature_version=selected.result.ref.version,
                    data_ts=selected.result.data_ts,
                )

        rows.append(
            TrainingDatasetRow(
                entity=observation.entity,
                observation_ts=observation.observation_ts,
                context=observation.context,
                features=features,
                feature_metadata=metadata,
            )
        )

    summary = {
        "rows": len(rows),
        "features": len(feature_names),
        "missing_values": missing_values,
        "future_records_ignored": future_records_ignored,
        "safety_gap_seconds": int(safety_gap.total_seconds()),
    }
    return TrainingDataset(rows=rows, summary=summary)


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{name} must be timezone-aware")
