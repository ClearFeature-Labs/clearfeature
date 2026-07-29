"""In-memory offline feature store: append-only tall history.

Each computed ``FeatureResult`` is stored once per ``(view, view_version)`` as an
``OfflineFeatureRecord`` and never overwritten. This is the audit/training
history that a Postgres tall table will later hold. ``FeatureResult`` does not
carry ``view``/``view_version``, so the record wraps them alongside the result.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

from fintech_feature_platform.fs_core.models import EntityKey, FeatureResult
from fintech_feature_platform.fs_core.pit import select_pit


@dataclass(frozen=True)
class OfflineFeatureRecord:
    view: str
    view_version: int
    result: FeatureResult


class InMemoryOfflineStore:
    def __init__(self) -> None:
        self._history: list[OfflineFeatureRecord] = []

    def append(self, view: str, view_version: int, result: FeatureResult) -> None:
        self._history.append(OfflineFeatureRecord(view, view_version, result))

    def append_many(
        self, view: str, view_version: int, results: Iterable[FeatureResult]
    ) -> None:
        for result in results:
            self.append(view, view_version, result)

    def get(
        self,
        entity_key: EntityKey,
        feature_name: str | None = None,
        feature_version: int | None = None,
        view: str | None = None,
        view_version: int | None = None,
    ) -> list[OfflineFeatureRecord]:
        wanted_key = entity_key.encode()
        matches: list[OfflineFeatureRecord] = []
        for record in self._history:
            result = record.result
            if result.entity_key.encode() != wanted_key:
                continue
            if feature_name is not None and result.ref.name != feature_name:
                continue
            if feature_version is not None and result.ref.version != feature_version:
                continue
            if view is not None and record.view != view:
                continue
            if view_version is not None and record.view_version != view_version:
                continue
            matches.append(record)
        return matches

    def get_pit(
        self,
        entity_key: EntityKey,
        *,
        feature_name: str,
        feature_version: int,
        view: str,
        view_version: int,
        observation_ts: datetime,
        safety_gap: timedelta = timedelta(0),
    ) -> OfflineFeatureRecord | None:
        """Latest PIT-eligible record for one feature (shared eligibility rule)."""
        records = self.get(
            entity_key,
            feature_name=feature_name,
            feature_version=feature_version,
            view=view,
            view_version=view_version,
        )
        selected, _ = select_pit(records, observation_ts, safety_gap)
        return selected
