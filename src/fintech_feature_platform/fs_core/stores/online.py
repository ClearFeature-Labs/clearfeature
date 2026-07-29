"""In-memory online feature store: latest value per serving key with the D9 guard.

Stores the latest ``FeatureResult`` per
``(entity_key, view, view_version, feature_name, feature_version)``. Writes go through
the shared D9 write guard (``fs_core.write_guard``): ordering compares
``(data_ts, max_input_data_ts)`` lexicographically, and equal tuples are disambiguated
by ``input_fingerprint`` (noop for identical replays, write for same-freshness
recomputes). Results without D9 metadata degenerate to the plain CAS on ``data_ts``.
"""

from __future__ import annotations

from collections.abc import Iterable

from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureResult,
    trusted_available_at,
)
from fintech_feature_platform.fs_core.write_guard import (
    WRITE_OUTCOMES,
    decide_write,
    guard_tuple,
)

_Key = tuple[str, str, int, str, int]


class InMemoryOnlineStore:
    def __init__(self) -> None:
        self._values: dict[_Key, FeatureResult] = {}

    def write(self, view: str, view_version: int, result: FeatureResult) -> str:
        """Apply the D9 write guard; return the outcome string (never a bool)."""
        key = self._key(
            view, view_version, result.entity_key, result.ref.name, result.ref.version
        )
        existing = self._values.get(key)
        outcome = decide_write(
            guard_tuple(result.data_ts, result.max_input_data_ts),
            result.input_fingerprint,
            guard_tuple(existing.data_ts, existing.max_input_data_ts)
            if existing is not None
            else None,
            existing.input_fingerprint if existing is not None else None,
            incoming_available_at=trusted_available_at(result),
            current_available_at=(
                trusted_available_at(existing) if existing is not None else None
            ),
        )
        if outcome in WRITE_OUTCOMES:
            self._values[key] = result
        return outcome

    def write_many(
        self, view: str, view_version: int, results: Iterable[FeatureResult]
    ) -> dict[str, str]:
        return {
            result.ref.encode(): self.write(view, view_version, result)
            for result in results
        }

    def get(
        self,
        view: str,
        view_version: int,
        entity_key: EntityKey,
        feature_name: str,
        feature_version: int,
    ) -> FeatureResult | None:
        return self._values.get(
            self._key(view, view_version, entity_key, feature_name, feature_version)
        )

    @staticmethod
    def _key(
        view: str,
        view_version: int,
        entity_key: EntityKey,
        feature_name: str,
        feature_version: int,
    ) -> _Key:
        return (entity_key.encode(), view, view_version, feature_name, feature_version)
