"""Reactive propagation worker: plan + debounce + execute recompute waves.

Two pure-ish handlers over an ``AppBackend``:

- ``handle_feature_updated`` — plan reactive dependents (reverse-DAG) for one
  ``FeatureUpdated`` and coalesce them into the ``DebounceStore``. No store/publish here, so
  the reactive consumer can commit the source offset once the update is safely captured.
- ``execute_wave`` — drain the debounce store and run one offline-only recompute **wave**: a
  child run with its own ``wave_id`` and bounded, values-free accounting. Each unit reads its
  dependency values from offline history (PIT-safe), recomputes the F2 dependent, appends the
  result to offline history, and re-emits a downstream ``FeatureUpdated`` when the recomputed
  feature itself has reactive dependents (chaining, depth-bounded by the registry).

Waves are **offline-only by default** (W3): no online write. Per-entity deterministic errors
are counted and skipped; only infra failures (offline append / downstream publish) propagate
so the runner withholds the source-offset commit and replays (offline dedup is idempotent).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.api.model_feature_batch import compute_model_feature_batch
from fintech_feature_platform.fs_core.dedup import partition_new_results
from fintech_feature_platform.fs_core.events.models import (
    FEATURE_UPDATE_SOURCE_RECOMPUTE_WAVE,
    FeatureUpdated,
)
from fintech_feature_platform.fs_core.model_runner import ModelRunner
from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.observability.metrics import recorder_of
from fintech_feature_platform.fs_core.propagation import (
    DebounceStore,
    emit_feature_updates,
    find_view,
    plan_recompute_wave,
)


@dataclass(frozen=True)
class FeatureUpdatedResult:
    # status: ok | invalid_event
    status: str
    candidates: int = 0
    error: str | None = None


def handle_feature_updated(
    backend: AppBackend,
    debounce: DebounceStore,
    event: FeatureUpdated,
    *,
    now: datetime | None = None,
) -> FeatureUpdatedResult:
    """Plan reactive dependents for one update and coalesce them into the debounce store."""
    try:
        candidates = plan_recompute_wave(backend.registry, event)
    except (KeyError, TypeError, ValueError) as exc:  # structural: bad event shape
        return FeatureUpdatedResult(status="invalid_event", error=str(exc))
    metrics = recorder_of(backend)
    metrics.incr("feature_updates_total", {"source": event.source})
    for candidate in candidates:
        newly = debounce.observe(candidate, event, now)
        if not newly:  # coalesced into an existing pending unit
            metrics.incr("propagation_debounced_total")
    metrics.gauge("propagation_pending_waves", len(debounce.pending()))
    return FeatureUpdatedResult(status="ok", candidates=len(candidates))


@dataclass(frozen=True)
class RecomputeWave:
    """Child-run accounting for one recompute wave — bounded counts + refs, never values."""

    wave_id: str
    trigger_update_ids: tuple[str, ...]
    entity_count: int
    dependent_feature_refs: tuple[str, ...]
    status: str  # completed | completed_with_errors
    created_at: datetime
    planned: int = 0
    computed: int = 0
    skipped: int = 0
    failed: int = 0
    debounced: int = 0
    downstream_emitted: int = 0
    parent_run_id: str | None = None
    parent_job_id: str | None = None
    manifest_id: str | None = None

    def counts(self) -> dict[str, int]:
        return {
            "planned": self.planned,
            "computed": self.computed,
            "skipped": self.skipped,
            "failed": self.failed,
            "debounced": self.debounced,
            "downstream_emitted": self.downstream_emitted,
        }


@dataclass
class _WaveTally:
    computed: int = 0
    skipped: int = 0
    failed: int = 0
    downstream_emitted: int = 0
    entities: set[str] = field(default_factory=set)
    dependent_refs: set[str] = field(default_factory=set)
    trigger_ids: list[str] = field(default_factory=list)


def execute_wave(
    backend: AppBackend,
    debounce: DebounceStore,
    *,
    calc_ts: datetime | None = None,
    safety_gap: timedelta = timedelta(0),
    model_runner: ModelRunner | None = None,
) -> RecomputeWave:
    """Drain the debounce store and run one offline-only recompute wave (a child run).

    Per-entity deterministic errors are counted (``failed``/``skipped``) and never halt the
    wave. Offline-append and downstream-publish failures are infra failures: they propagate
    so the caller does not commit — replay is safe (offline dedup is idempotent).

    An F3 (``kind: "model"``) dependent is recomputed offline via the batch model path when a
    ``model_runner`` is provided; without one it is counted ``skipped`` (a clear defer — F3 is
    never computed online or without its pinned model).
    """
    calc_ts = calc_ts or datetime.now(tz=UTC)
    wave_id = f"wave_{uuid4().hex}"
    entries = debounce.drain()
    tally = _WaveTally()
    for entry in entries:
        tally.trigger_ids.extend(entry.trigger_update_ids)
        tally.dependent_refs.add(
            f"{entry.dependent_feature}:v{entry.dependent_version}"
        )
        tally.entities.add(f"{entry.entity.entity_type}:{entry.entity.encoded()}")
        entity_key = EntityKey(
            tuple((name, value) for name, value in entry.entity.entity_key.items())
        )
        view_def = find_view(backend.registry, entry.view, entry.view_version)
        feature = None
        if view_def is not None:
            feature = next(
                (f for f in view_def.features if f.name == entry.dependent_feature), None
            )

        if feature is not None and feature.kind == "model":
            _recompute_model_dependent(
                backend, model_runner, entry, entity_key, calc_ts, safety_gap, wave_id, tally
            )
            continue

        try:
            result = backend.store.compute_dependent_from_offline(
                view=entry.view,
                view_version=entry.view_version,
                entity_key=entity_key,
                feature_name=entry.dependent_feature,
                calc_ts=calc_ts,
                safety_gap=safety_gap,
            )
        except (KeyError, TypeError, ValueError):  # per-entity deterministic error
            tally.failed += 1
            continue
        if result is None:  # required input missing/stale -> SKIPPED
            tally.skipped += 1
            continue

        # Durable offline append (idempotent) BEFORE any downstream emission. An infra
        # failure here propagates -> no commit -> replay.
        new_results, _ = partition_new_results(
            backend.offline, entry.view, entry.view_version, [result]
        )
        backend.offline.append_many(entry.view, entry.view_version, new_results)
        tally.computed += 1

        # Chain: re-emit only if the recomputed feature has further reactive dependents.
        if view_def is not None:
            tally.downstream_emitted += emit_feature_updates(
                publisher=backend.events,
                view_def=view_def,
                results=[result],
                entity_type=entry.entity.entity_type,
                source=FEATURE_UPDATE_SOURCE_RECOMPUTE_WAVE,
            )

    metrics = recorder_of(backend)
    metrics.incr("propagation_waves_total")
    metrics.incr("propagation_wave_items_total", {"outcome": "computed"}, value=tally.computed)
    metrics.incr("propagation_wave_items_total", {"outcome": "skipped"}, value=tally.skipped)
    metrics.incr("propagation_wave_items_total", {"outcome": "failed"}, value=tally.failed)
    for entry in entries:
        # Propagation lag: how stale the triggering input was at recompute time (values-free).
        metrics.observe(
            "propagation_lag_seconds",
            max(0.0, (calc_ts - entry.latest_data_ts).total_seconds()),
        )

    status = "completed" if tally.failed == 0 else "completed_with_errors"
    return RecomputeWave(
        wave_id=wave_id,
        trigger_update_ids=tuple(tally.trigger_ids),
        entity_count=len(tally.entities),
        dependent_feature_refs=tuple(sorted(tally.dependent_refs)),
        status=status,
        created_at=calc_ts,
        planned=len(entries),
        computed=tally.computed,
        skipped=tally.skipped,
        failed=tally.failed,
        downstream_emitted=tally.downstream_emitted,
    )


def _recompute_model_dependent(
    backend: AppBackend,
    model_runner: ModelRunner | None,
    entry,
    entity_key: EntityKey,
    calc_ts: datetime,
    safety_gap: timedelta,
    wave_id: str,
    tally: _WaveTally,
) -> None:
    """Recompute one F3 dependent offline via the batch model path (single-entity frame).

    Without a ``model_runner`` the unit is a clear defer (counted ``skipped``). Deterministic
    model/config errors are counted ``failed``; infra failures propagate (no commit -> replay).
    """
    if model_runner is None:
        tally.skipped += 1  # F3 needs its pinned model; defer clearly rather than guess
        return
    try:
        result = compute_model_feature_batch(
            backend,
            model_runner,
            view=entry.view,
            view_version=entry.view_version,
            feature_name=entry.dependent_feature,
            entity_keys=[entity_key],
            observation_ts=calc_ts,
            safety_gap=safety_gap,
            wave_id=wave_id,
            emit_source=FEATURE_UPDATE_SOURCE_RECOMPUTE_WAVE,
        )
    except (KeyError, TypeError, ValueError):  # per-entity deterministic model/config error
        tally.failed += 1
        return
    tally.computed += result.computed
    tally.skipped += result.skipped
    tally.downstream_emitted += result.downstream_emitted
