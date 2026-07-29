"""Point-in-time (PIT) eligibility: the one leakage-safe rule, shared everywhere.

PIT correctness has two clocks :

- ``data_ts`` — the time the underlying data describes (freshness);
- ``available_at`` — when the value's inputs became available to the bank
  (trusted, optional); falls back to
- ``calc_ts`` — the time the platform actually computed/observed/stored the value.

A feature row is eligible for an observation only when BOTH hold::

    data_ts <= observation_ts - safety_gap             # data not too close to the label
    (available_at or calc_ts) <= observation_ts        # value was AVAILABLE by then:
                                                       # trusted availability wins,
                                                       # legacy rows stay conservative

``safety_gap`` is an extra conservative delay before the observation point. This module
is the single source of the rule; the training builder, the in-memory offline store, and
the Postgres offline query all apply exactly it, so their behavior cannot diverge.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{name} must be timezone-aware")


def effective_data_cutoff(observation_ts: datetime, safety_gap: timedelta) -> datetime:
    """The latest ``data_ts`` still allowed: ``observation_ts - safety_gap``."""
    _require_aware(observation_ts, "observation_ts")
    if safety_gap < timedelta(0):
        raise ValueError("safety_gap must be non-negative")
    return observation_ts - safety_gap


def is_pit_eligible(
    record, observation_ts: datetime, safety_gap: timedelta = timedelta(0)
) -> bool:
    """True iff ``record`` may be used for ``observation_ts`` (no lookahead leakage).

    ``record`` is an ``OfflineFeatureRecord`` (carries ``record.result.data_ts``,
    ``result.calc_ts`` and optionally ``result.available_at``). The availability
    bound uses the effective clock: trusted ``available_at`` when present, else the
    conservative ``calc_ts``.
    """
    cutoff = effective_data_cutoff(observation_ts, safety_gap)
    result = record.result
    effective_available = (
        result.available_at
        if getattr(result, "available_at", None) is not None
        else result.calc_ts
    )
    return result.data_ts <= cutoff and effective_available <= observation_ts


def select_pit(
    records: Sequence,
    observation_ts: datetime,
    safety_gap: timedelta = timedelta(0),
) -> tuple[object | None, int]:
    """Pick the latest eligible record; return ``(selected, future_records_ignored)``.

    Latest = highest ``(data_ts, calc_ts)``; exact ties break to the last record in
    input order (append order for the in-memory store), matching the legacy behavior.
    ``future_records_ignored`` counts records whose ``data_ts`` is strictly after
    ``observation_ts`` (genuine lookahead) — records excluded only by ``safety_gap`` or
    the ``calc_ts`` availability bound are simply not selected, not counted here.
    """
    selected = None
    future_records_ignored = 0
    for record in records:
        if record.result.data_ts > observation_ts:
            future_records_ignored += 1
        if not is_pit_eligible(record, observation_ts, safety_gap):
            continue
        if selected is None or (
            record.result.data_ts,
            record.result.calc_ts,
        ) >= (selected.result.data_ts, selected.result.calc_ts):
            selected = record
    return selected, future_records_ignored
