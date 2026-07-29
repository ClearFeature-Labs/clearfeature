"""Canonical hashing for D9 write-guard metadata.

``value_hash`` identifies a feature value; ``input_fingerprint`` identifies the exact
set of inputs (id, freshness, value identity) a feature was computed from. Both must be
deterministic across replays and runtimes, so serialization is strict canonical JSON
(sorted keys, compact separators, no NaN/Infinity, no lossy ``default=`` fallback) and
timestamps are normalized to UTC epoch microseconds before hashing.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def canonical_value_json(value: Any) -> str:
    """Serialize a JSON-compatible value canonically; fail loudly otherwise."""
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"value is not canonically JSON-serializable: {exc}") from exc


def value_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_value_json(value).encode("utf-8")).hexdigest()


def to_epoch_us(value: datetime) -> int:
    """UTC epoch microseconds; rejects naive datetimes (comparison must be absolute)."""
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError("timestamp must be timezone-aware")
    delta = value.astimezone(UTC) - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def input_fingerprint(inputs: Iterable[tuple[str, datetime, str]]) -> str:
    """Hash ``(input_id, input_data_ts, input_value_hash)`` tuples order-independently.

    Tuples are sorted by ``input_id`` and timestamps normalized to epoch microseconds,
    so the fingerprint is stable across dict ordering and timezone representations.
    """
    normalized = sorted(
        [input_id, to_epoch_us(input_data_ts), input_value_hash]
        for input_id, input_data_ts, input_value_hash in inputs
    )
    return "sha256:" + hashlib.sha256(
        canonical_value_json(normalized).encode("utf-8")
    ).hexdigest()
