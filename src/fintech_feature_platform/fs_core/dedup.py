"""Exact-duplicate detection for offline feature rows (idempotent reruns).

A candidate ``FeatureResult`` is an *exact duplicate* of an existing offline record when
they match on entity_key, view, view_version, feature_name, feature_version, data_ts,
value, **and** ``input_fingerprint`` when both sides carry one. ``calc_ts`` is excluded
(it changes every run). Same data_ts + different value is a *correction* (kept); same
value + different data_ts is a new historical state (kept); same value + different
fingerprint is a same-freshness recompute from different inputs (D9 Case C — kept, so
the recompute stays auditable). When either side lacks a fingerprint (legacy rows,
model scores, imports) the pre-D9 rule applies unchanged.

``partition_new_results`` skips exact duplicates against the offline store **and** against
results already accepted earlier in the same run (in-memory seen set). It is read-only and
uses the existing ``offline.get(...)`` lookup — no schema change. Cost is
O(results × offline.get); correct but not optimized for very large runs.
"""

from __future__ import annotations

import json
from collections.abc import Sequence

from fintech_feature_platform.fs_core.models import FeatureResult, trusted_available_at


def _value_token(value: object) -> str:
    # Stable comparable token for the in-run seen set (value may be non-hashable).
    return json.dumps(value, sort_keys=True, default=str)


def _signature(result: FeatureResult) -> tuple:
    return (
        result.entity_key.encode(),
        result.ref.name,
        result.ref.version,
        result.data_ts,
        _value_token(result.value),
        result.input_fingerprint,
        trusted_available_at(result),
    )


def _same_fingerprint(existing: FeatureResult, candidate: FeatureResult) -> bool:
    # Fingerprints only disambiguate when both sides carry one; otherwise keep the
    # pre-D9 exact-duplicate rule (legacy rows / scores / imports have None).
    if existing.input_fingerprint is None or candidate.input_fingerprint is None:
        return True
    return existing.input_fingerprint == candidate.input_fingerprint


def _same_availability(existing: FeatureResult, candidate: FeatureResult) -> bool:
    # Availability is part of the replay identity : a corrected trusted
    # available_at must NOT be silently dropped as a duplicate. Like fingerprints, it
    # only disambiguates when BOTH sides carry one — a legacy row (None) matched by a
    # new availability-carrying recompute stays an exact duplicate, so legacy
    # replays remain all-noop and history is never re-written by an upgrade.
    existing_at = trusted_available_at(existing)
    candidate_at = trusted_available_at(candidate)
    if existing_at is None or candidate_at is None:
        return True
    return existing_at == candidate_at


def _exists_in_offline(offline, view: str, view_version: int, result: FeatureResult) -> bool:
    existing = offline.get(
        result.entity_key,
        feature_name=result.ref.name,
        feature_version=result.ref.version,
        view=view,
        view_version=view_version,
    )
    for record in existing:
        if (
            record.result.data_ts == result.data_ts
            and record.result.value == result.value
            and _same_fingerprint(record.result, result)
            and _same_availability(record.result, result)
        ):
            return True
    return False


def partition_new_results(
    offline,
    view: str,
    view_version: int,
    results: Sequence[FeatureResult],
) -> tuple[list[FeatureResult], int]:
    """Split results into (new, duplicates_skipped) by exact-duplicate match.

    Skips a result if it exactly matches an existing offline record or a result already
    accepted in this call.
    """
    new_results: list[FeatureResult] = []
    duplicates_skipped = 0
    seen: set[tuple] = set()
    for result in results:
        signature = _signature(result)
        if signature in seen:
            duplicates_skipped += 1
            continue
        if _exists_in_offline(offline, view, view_version, result):
            duplicates_skipped += 1
            continue
        seen.add(signature)
        new_results.append(result)
    return new_results, duplicates_skipped
