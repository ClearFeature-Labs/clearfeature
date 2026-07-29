"""Local FeatureStore workflow: wire the in-memory components into one path.

This is the single orchestration entry point that proves the full local path
works end to end: registry + report resolver + RequestContext + ComputeCore +
offline/online stores. It is intentionally not a service, API, or framework.
Each ``compute`` call runs the same fixed sequence; it always appends offline
history, and writes online latest unless ``write_online=False`` (used by the
offline-only historical backfill path).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any

from fintech_feature_platform.fs_core.compute.context import (
    STATUS_OK,
    FeatureComputation,
    RequestContext,
)
from fintech_feature_platform.fs_core.compute.engine import ComputeCore
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.dedup import partition_new_results
from fintech_feature_platform.fs_core.hashing import value_hash
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureResult,
    FeatureWriteSet,
    SourceStamp,
)
from fintech_feature_platform.fs_core.observability.metrics import MetricsRecorder
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.models import FeatureViewDef, Registry
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore


def _timed_loader(loader, metrics: MetricsRecorder, execution_mode: str):
    """Time each lazy source load as the input_fetch pipeline stage (once per source —
    RequestContext caches, so exactly one observation per source per request)."""

    def timed(source_name: str):
        started = perf_counter()
        try:
            return loader(source_name)
        finally:
            metrics.observe(
                "fsp_pipeline_stage_duration_seconds",
                perf_counter() - started,
                {"execution_mode": execution_mode, "stage": "input_fetch"},
            )

    return timed


@dataclass(frozen=True)
class ComputeOutcome:
    results: dict[str, FeatureResult]
    # Per-feature D9 outcome ("written" | "written_recompute" | "skipped_stale" |
    # "noop"), or "disabled" when write_online=False. Never rely on truthiness.
    online_written: dict[str, str]
    duplicates_skipped: int = 0
    # Per-node F2 status (OK/DEGRADED/SKIPPED,); values-free observability.
    node_statuses: dict[str, str] = field(default_factory=dict)


class FeatureStore:
    def __init__(
        self,
        registry: Registry,
        udfs: UdfRegistry,
        resolver: ReportResolver,
        offline: InMemoryOfflineStore,
        online: InMemoryOnlineStore,
        *,
        metrics: MetricsRecorder | None = None,
    ) -> None:
        self._core = ComputeCore(registry, udfs, metrics=metrics)
        self._registry = registry
        self._resolver = resolver
        self._offline = offline
        self._online = online
        self._metrics = metrics
        self._expected_types = {s.name: s.report_type for s in registry.sources}

    def compute_write_set(
        self,
        *,
        view: str,
        view_version: int,
        entity_key: EntityKey,
        requested_features: Sequence[str],
        source_refs: Mapping[str, str],
        source_stamps: Mapping[str, SourceStamp],
        calc_ts: datetime,
        request_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        correlation_id: str | None = None,
        write_policy: str = "online_first",
        source_loader: Callable[[str], Any] | None = None,
        execution_mode: str = "online",
    ) -> FeatureWriteSet:
        """Compute requested features and return them without writing any store.

        This is the compute-only seam: it resolves sources, runs ``ComputeCore``, and
        wraps the results in a ``FeatureWriteSet``. It writes neither offline nor online
        and runs no dedup. The Kafka online worker calls this and then performs
        explicit Valkey-first writes; it must NOT call ``compute`` (which keeps the
        synchronous offline-first persistence below).

        ``source_stamps`` maps each source name to its own ``(report_ts, content_hash)``
        so ComputeCore can derive per-feature ``data_ts`` (D3) and the D9 metadata —
        there is deliberately no single request-level ``data_ts`` parameter anymore.

        ``source_loader`` lets a caller supply its own ``load(source_name) -> payload``
        (e.g. an online worker reading raw reports directly from event descriptors,
        bypassing the Postgres metadata lookup). When ``None`` (the default), the
        ``ReportResolver`` is used exactly as before — current callers are unchanged.
        """
        loader = (
            source_loader
            if source_loader is not None
            else self._resolver.make_source_loader(source_refs, self._expected_types)
        )
        # Stage timing : input_fetch wraps each lazy source load (they
        # happen INSIDE compute, on first touch, once per source — so the compute stage
        # INCLUDES input_fetch; compute-exclusive time = compute - input_fetch).
        if self._metrics is not None:
            loader = _timed_loader(loader, self._metrics, execution_mode)
        context = RequestContext(loader)

        compute_started = perf_counter()
        results = self._core.compute(
            view=view,
            view_version=view_version,
            entity_key=entity_key,
            requested_features=requested_features,
            context=context,
            source_stamps=source_stamps,
            calc_ts=calc_ts,
            execution_mode=execution_mode,
        )
        if self._metrics is not None:
            self._metrics.observe(
                "fsp_pipeline_stage_duration_seconds",
                perf_counter() - compute_started,
                {"execution_mode": execution_mode, "stage": "compute"},
            )

        return FeatureWriteSet(
            view=view,
            view_version=view_version,
            entity_key=entity_key,
            results=results,
            source_refs=dict(source_refs),
            request_id=request_id,
            job_id=job_id,
            run_id=run_id,
            correlation_id=correlation_id,
            write_policy=write_policy,
            node_statuses=context.statuses(),
        )

    def compute(
        self,
        *,
        view: str,
        view_version: int,
        entity_key: EntityKey,
        requested_features: Sequence[str],
        source_refs: Mapping[str, str],
        source_stamps: Mapping[str, SourceStamp],
        calc_ts: datetime,
        write_online: bool = True,
        skip_duplicate_offline: bool = False,
    ) -> ComputeOutcome:
        # Compute via the shared compute-only seam, then persist exactly as before.
        # This synchronous persist path is used by batch-inline items and backfills,
        # so its stage/feature metrics are recorded in batch mode.
        write_set = self.compute_write_set(
            view=view,
            view_version=view_version,
            entity_key=entity_key,
            requested_features=requested_features,
            source_refs=source_refs,
            source_stamps=source_stamps,
            calc_ts=calc_ts,
            execution_mode="batch",
        )
        results = write_set.results

        # Idempotent reruns (e.g. backfill): append only non-duplicate results, and write
        # online only for those. Default off, so live compute behaviour is unchanged.
        duplicates_skipped = 0
        if skip_duplicate_offline:
            to_write, duplicates_skipped = partition_new_results(
                self._offline, view, view_version, list(results.values())
            )
        else:
            to_write = list(results.values())

        offline_started = perf_counter()
        self._offline.append_many(view, view_version, to_write)
        if self._metrics is not None:
            self._metrics.observe(
                "fsp_pipeline_stage_duration_seconds",
                perf_counter() - offline_started,
                {"execution_mode": "batch", "stage": "offline_write"},
            )
        if write_online:
            online_started = perf_counter()
            online_written = self._online.write_many(view, view_version, to_write)
            if self._metrics is not None:
                self._metrics.observe(
                    "fsp_pipeline_stage_duration_seconds",
                    perf_counter() - online_started,
                    {"execution_mode": "batch", "stage": "online_write"},
                )
                for outcome in online_written.values():
                    self._metrics.incr(
                        "fsp_online_write_outcomes_total",
                        {"execution_mode": "batch", "outcome": outcome},
                    )
        else:
            # Offline-only path (e.g. historical backfill): never touch online latest.
            online_written = {result.ref.encode(): "disabled" for result in to_write}

        return ComputeOutcome(
            results=results,
            online_written=online_written,
            duplicates_skipped=duplicates_skipped,
            node_statuses=dict(write_set.node_statuses),
        )

    def _find_view_def(self, view: str, view_version: int) -> FeatureViewDef:
        return self._registry.find_view(view, view_version)  # the ONE authoritative lookup

    def compute_dependent_from_offline(
        self,
        *,
        view: str,
        view_version: int,
        entity_key: EntityKey,
        feature_name: str,
        calc_ts: datetime,
        safety_gap: timedelta = timedelta(0),
    ) -> FeatureResult | None:
        """Recompute one F2 dependent for one entity from offline dependency values.

        Used by recompute waves : dependency inputs are read from offline history
        as the latest PIT-eligible value at ``calc_ts`` (``safety_gap`` honored) and prefilled
        into a ``RequestContext`` so ``ComputeCore`` computes the dependent through the exact
        same D3/D9 + staleness/status path as online/batch — no parallel compute engine.

        Returns ``None`` (SKIPPED) when a required dependency has no eligible offline value or
        an edge is stale. Same-entity only: a dependent that also reads a raw
        source raises ``ValueError`` (out of scope for the recompute wave — the
        wave has no report to read).
        """
        view_def = self._find_view_def(view, view_version)
        features_by_name = {f.name: f for f in view_def.features}
        feature = features_by_name.get(feature_name)
        if feature is None:
            raise ValueError(f"unknown feature {feature_name!r} in view {view!r}")

        def _no_source(name: str) -> Any:
            raise ValueError(
                f"recompute wave for {feature_name!r} cannot read raw source {name!r} "
                "from offline history (only pure-dependency F2 recompute is supported)"
            )

        context = RequestContext(_no_source)
        for dep in feature.deps:
            target = features_by_name.get(dep.feature)
            if target is None:
                raise ValueError(
                    f"feature {feature_name!r} depends on unknown feature {dep.feature!r}"
                )
            version = dep.version or target.feature_version
            record = self._offline.get_pit(
                entity_key,
                feature_name=dep.feature,
                feature_version=version,
                view=view,
                view_version=view_version,
                observation_ts=calc_ts,
                safety_gap=safety_gap,
            )
            if record is None:
                return None  # missing required input -> SKIPPED (caller records it)
            r = record.result
            context.cache_feature(
                dep.feature,
                FeatureComputation(
                    value=r.value,
                    data_ts=r.data_ts,
                    max_input_data_ts=r.max_input_data_ts or r.data_ts,
                    value_hash=r.value_hash or value_hash(r.value),
                    input_fingerprint=r.input_fingerprint or value_hash(r.value),
                    status=STATUS_OK,
                    available_at=r.available_at,
                    availability_source=r.availability_source,
                ),
            )

        results = self._core.compute(
            view=view,
            view_version=view_version,
            entity_key=entity_key,
            requested_features=[feature_name],
            context=context,
            source_stamps={},
            calc_ts=calc_ts,
            # Propagation recompute waves are batch-natured (asynchronous, offline-driven).
            execution_mode="batch",
        )
        return results.get(feature_name)
