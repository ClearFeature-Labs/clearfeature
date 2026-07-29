"""Minimal ComputeCore: compute requested features from a validated registry.

This is the first in-memory vertical slice, intentionally not a full engine:

- dependencies are resolved with simple recursion (no general DAG/scheduler);
- cycles are detected via the recursion stack;
- only requested features and their transitive dependencies are computed;
- source payloads are read through ``RequestContext`` (once per request) and
  dependency computations are cached (each computed once).

Temporal semantics : each feature's ``data_ts`` is
derived from its OWN inputs — never a request-level timestamp. Per feature:

    data_ts           = min(input data_ts)      (source input -> its report_ts;
    max_input_data_ts = max(input data_ts)       dependency input -> its data_ts)
    value_hash        = canonical hash of the computed value
    input_fingerprint = canonical hash over (input_id, input_data_ts, input_value_hash)

The same ``compute`` call is what the online and batch engines use.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from time import perf_counter

from fintech_feature_platform.fs_core.compute.context import (
    STATUS_DEGRADED,
    STATUS_OK,
    FeatureComputation,
    RequestContext,
)
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.hashing import input_fingerprint, value_hash
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureResult,
    SourceStamp,
)
from fintech_feature_platform.fs_core.registry.models import (
    FeatureDef,
    FeatureViewDef,
    Registry,
)


class ComputeCore:
    def __init__(
        self,
        registry: Registry,
        udfs: UdfRegistry,
        *,
        metrics: object | None = None,
    ) -> None:
        self._registry = registry
        self._udfs = udfs
        # Per-feature timing. Identities are PREBUILT once from the
        # registry (the bundle-format ``view:vN:feature:vN``): nothing is derived or
        # registered on the hot path, and only registry features can ever appear.
        self._metrics = metrics
        self._feature_ids: dict[str, dict[str, str]] = {
            v.name: {
                f.name: f"{v.name}:v{v.view_version}:{f.name}:v{f.feature_version}"
                for f in v.features
            }
            for v in registry.feature_views
        }

    def compute(
        self,
        *,
        view: str,
        view_version: int,
        entity_key: EntityKey,
        requested_features: Sequence[str],
        context: RequestContext,
        source_stamps: Mapping[str, SourceStamp],
        calc_ts: datetime,
        execution_mode: str = "online",
    ) -> dict[str, FeatureResult]:
        view_def = self._find_view(view, view_version)
        features_by_name = {f.name: f for f in view_def.features}
        feature_ids = self._feature_ids.get(view, {})

        results: dict[str, FeatureResult] = {}
        for name in requested_features:
            computation = self._resolve(
                name, features_by_name, context, source_stamps, calc_ts, [],
                feature_ids, execution_mode,
            )
            # SKIPPED (a required input stale/missing) produces no FeatureResult; the
            # status is recorded on the context for the caller to surface.
            if computation is None:
                continue
            results[name] = FeatureResult(
                ref=features_by_name[name].ref(),
                entity_key=entity_key,
                value=computation.value,
                data_ts=computation.data_ts,
                calc_ts=calc_ts,
                max_input_data_ts=computation.max_input_data_ts,
                input_fingerprint=computation.input_fingerprint,
                value_hash=computation.value_hash,
                available_at=computation.available_at,
                availability_source=computation.availability_source,
            )
        return results

    def _find_view(self, view: str, view_version: int) -> FeatureViewDef:
        for view_def in self._registry.feature_views:
            if view_def.name == view:
                if view_def.view_version != view_version:
                    raise ValueError(
                        f"view {view!r} version mismatch: requested {view_version}, "
                        f"registry has {view_def.view_version}"
                    )
                return view_def
        raise ValueError(f"unknown view {view!r}")

    def _resolve(
        self,
        name: str,
        features_by_name: dict[str, FeatureDef],
        context: RequestContext,
        source_stamps: Mapping[str, SourceStamp],
        calc_ts: datetime,
        visiting: list[str],
        feature_ids: dict[str, str],
        execution_mode: str,
    ) -> FeatureComputation | None:
        """Resolve one node in topological order; ``None`` means SKIPPED (not computed)."""
        if context.has_feature(name):
            return context.get_feature(name)
        if context.is_skipped(name):
            return None
        if name not in features_by_name:
            raise ValueError(f"unknown feature {name!r}")
        if name in visiting:
            cycle = " -> ".join([*visiting, name])
            raise ValueError(f"dependency cycle detected: {cycle}")

        feature = features_by_name[name]
        visiting.append(name)
        try:
            sources = {src: context.get_source(src) for src in feature.inputs}
            # Resolve each F2 edge; a stale/missing REQUIRED input skips this feature, a
            # stale/degraded OPTIONAL input degrades it.
            dep_computations: dict[str, FeatureComputation] = {}
            degraded = False
            for dep in feature.deps:
                comp = self._resolve(
                    dep.feature, features_by_name, context, source_stamps,
                    calc_ts, visiting, feature_ids, execution_mode,
                )
                if comp is None:  # upstream SKIPPED
                    if dep.required:
                        context.mark_skipped(name)
                        return None
                    degraded = True  # optional missing: proceed without it
                    continue
                if _edge_is_stale(dep, comp.data_ts, calc_ts):
                    if dep.required:
                        context.mark_skipped(name)
                        return None
                    degraded = True  # optional stale: keep the (old) value, degrade
                if comp.status == STATUS_DEGRADED:
                    degraded = True  # degradation propagates upward
                dep_computations[dep.feature] = comp
        finally:
            visiting.pop()

        # (input_id, input_data_ts, input_value_hash) per D9; source identity is the
        # payload content_hash (stable across replays), dependency identity is the
        # versioned FeatureRef with the dependency's own data_ts/value_hash.
        inputs: list[tuple[str, datetime, str]] = []
        input_availabilities: list[datetime | None] = []
        input_availability_sources: list[str | None] = []
        for src in feature.inputs:
            stamp = source_stamps.get(src)
            if stamp is None:
                raise ValueError(
                    f"feature {name!r} reads source {src!r} but no source stamp "
                    f"(report_ts/content_hash) was provided for it"
                )
            inputs.append((f"source:{src}", stamp.report_ts, stamp.content_hash))
            input_availabilities.append(stamp.available_at)
            input_availability_sources.append(stamp.availability_source)
        for dep, computation in dep_computations.items():
            inputs.append(
                (
                    features_by_name[dep].ref().encode(),
                    computation.data_ts,
                    computation.value_hash,
                )
            )
            input_availabilities.append(computation.available_at)
            input_availability_sources.append(computation.availability_source)

        udf = self._udfs.get(feature.udf)
        deps = {dep: comp.value for dep, comp in dep_computations.items()}
        # Per-feature EXCLUSIVE timing : only the UDF invocation is
        # timed. Dependencies were resolved above in their OWN ``_resolve`` frames
        # (memoized — never re-timed here), and lazy source fetches are timed as the
        # input_fetch stage at the loader boundary. One observation = one entity
        # evaluation, in both online and batch modes (batch computes item-by-item).
        if self._metrics is not None:
            udf_started = perf_counter()
            value = udf(sources, deps)
            duration = perf_counter() - udf_started
            feature_id = feature_ids.get(name)
            if feature_id is not None:
                labels = {"execution_mode": execution_mode, "feature_id": feature_id}
                self._metrics.observe(
                    "fsp_feature_compute_duration_seconds", duration, labels
                )
                self._metrics.incr("fsp_feature_compute_items_total", labels)
        else:
            value = udf(sources, deps)

        input_ts = [ts for _, ts, _ in inputs]
        # A feature is available only when its LAST required input became available;
        # any input with unknown availability makes the whole feature's availability
        # unknown (None -> conservative calc_ts fallback at PIT time)..
        all_available = (
            input_availabilities and all(a is not None for a in input_availabilities)
        )
        # Trusted only when EVERY input availability is a source-provided claim —
        # that is the only case where availability is rerun-stable and may join the
        # replay identity (see models.trusted_available_at).
        all_trusted = all_available and all(
            source == "source_provided" for source in input_availability_sources
        )
        computation = FeatureComputation(
            value=value,
            data_ts=min(input_ts),
            max_input_data_ts=max(input_ts),
            value_hash=value_hash(value),
            input_fingerprint=input_fingerprint(inputs),
            status=STATUS_DEGRADED if degraded else STATUS_OK,
            available_at=max(input_availabilities) if all_available else None,
            availability_source=(
                "source_provided" if all_trusted
                else ("ingestion_time" if all_available else None)
            ),
        )
        context.cache_feature(name, computation)
        return computation


def _edge_is_stale(dep, input_data_ts: datetime, calc_ts: datetime) -> bool:
    """True if the input is older than the edge's ``max_input_age_seconds`` at eval time."""
    if dep.max_input_age_seconds is None:
        return False
    return calc_ts - input_data_ts > timedelta(seconds=dep.max_input_age_seconds)
