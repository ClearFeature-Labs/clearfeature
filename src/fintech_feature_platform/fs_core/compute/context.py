"""In-request caches for ComputeCore.

A ``RequestContext`` loads each source payload at most once and caches each
computed feature at most once. The cached entry is a ``FeatureComputation``
(value + per-feature D9 metadata), so a shared dependency keeps its own
``data_ts``/``value_hash`` when reused by several dependents.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

# Per-node F2 status : OK (computed, all required inputs fresh), DEGRADED
# (computed, an optional input stale/degraded), SKIPPED (a required input stale/missing,
# so the feature was not computed).
STATUS_OK = "OK"
STATUS_DEGRADED = "DEGRADED"
STATUS_SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class FeatureComputation:
    """A computed feature value plus the D9 metadata derived from its own inputs."""

    value: Any
    data_ts: datetime
    max_input_data_ts: datetime
    value_hash: str
    input_fingerprint: str
    status: str = STATUS_OK
    # max(input available_at) when EVERY input carries one; None otherwise.
    available_at: datetime | None = None
    # "source_provided" only when EVERY input availability is a trusted claim.
    availability_source: str | None = None


class RequestContext:
    def __init__(self, source_loader: Callable[[str], Any]) -> None:
        self._source_loader = source_loader
        self._sources: dict[str, Any] = {}
        self._features: dict[str, FeatureComputation] = {}
        self._statuses: dict[str, str] = {}

    def get_source(self, name: str) -> Any:
        """Return a source payload, loading it (once) via the source loader."""
        if name not in self._sources:
            self._sources[name] = self._source_loader(name)
        return self._sources[name]

    def has_feature(self, name: str) -> bool:
        return name in self._features

    def get_feature(self, name: str) -> FeatureComputation:
        return self._features[name]

    def cache_feature(self, name: str, computation: FeatureComputation) -> None:
        self._features[name] = computation
        self._statuses[name] = computation.status

    def is_skipped(self, name: str) -> bool:
        return self._statuses.get(name) == STATUS_SKIPPED

    def mark_skipped(self, name: str) -> None:
        self._statuses[name] = STATUS_SKIPPED

    def statuses(self) -> dict[str, str]:
        """Per-node status (OK/DEGRADED/SKIPPED) for every feature resolved this request."""
        return dict(self._statuses)
