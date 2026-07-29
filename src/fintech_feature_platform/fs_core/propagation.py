"""Feature propagation: values-free changelog planning + debounce.

Propagation is *not* "recompute everything immediately". It is a controlled changelog
pipeline :

- a durable offline write/import emits a values-free ``FeatureUpdated`` event, but only for
  a feature that has at least one **reactive** dependent (``build_feature_updated_events``
  + the shared ``emit_feature_updates`` helper — the single seam every emitter reuses);
- ``plan_recompute_wave`` is the pure reverse-DAG planner: given a ``FeatureUpdated`` it
  returns the reactive dependents that must be recomputed (only ``reactive`` edges produce
  candidates; ``lazy``/``scheduled``/``none`` never do);
- ``DebounceStore`` coalesces many updates for the same ``entity + dependent`` into one
  recompute unit of work, carrying the latest input watermark.

This module is pure (no Kafka, no store I/O). Execution of a recompute wave lives in
``api/propagation_worker.py``; the reactive consumer runner lives in
``api/propagation_worker_runner.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

from fintech_feature_platform.fs_core.events.models import EntityRef, FeatureUpdated
from fintech_feature_platform.fs_core.events.publisher import EventPublisher
from fintech_feature_platform.fs_core.events.topics import FEATURE_UPDATES
from fintech_feature_platform.fs_core.models import FeatureResult
from fintech_feature_platform.fs_core.registry.models import (
    PROPAGATION_REACTIVE,
    FeatureDependency,
    FeatureViewDef,
    Registry,
)


def _now() -> datetime:
    return datetime.now(tz=UTC)


def find_view(
    registry: Registry, view: str, view_version: int
) -> FeatureViewDef | None:
    for candidate in registry.feature_views:
        if candidate.name == view and candidate.view_version == view_version:
            return candidate
    return None


def reactive_dependents(
    view_def: FeatureViewDef,
) -> dict[str, list[tuple[str, FeatureDependency]]]:
    """Reverse-DAG index: input feature name -> [(dependent feature name, reactive edge)].

    Only ``reactive`` edges are indexed; ``lazy``/``scheduled``/``none`` are omitted so a
    changed feature only ever produces reactive recompute candidates.
    """
    index: dict[str, list[tuple[str, FeatureDependency]]] = {}
    for feature in view_def.features:
        for dep in feature.deps:
            if dep.propagation == PROPAGATION_REACTIVE:
                index.setdefault(dep.feature, []).append((feature.name, dep))
    return index


def features_with_reactive_dependents(view_def: FeatureViewDef) -> frozenset[str]:
    """Names of features that at least one reactive edge depends on (emit gate)."""
    return frozenset(reactive_dependents(view_def).keys())


def build_feature_updated_events(
    *,
    view_def: FeatureViewDef,
    results: list[FeatureResult],
    entity_type: str,
    source: str,
    run_id: str | None = None,
    job_id: str | None = None,
    manifest_id: str | None = None,
    occurred_at: datetime | None = None,
) -> list[FeatureUpdated]:
    """Build values-free ``FeatureUpdated`` events for written features with reactive deps.

    A feature with no reactive dependent produces no event (nobody would consume it), which
    keeps the topic bounded. The event carries refs/timestamps/hash ids only — never the
    feature value or any payload.
    """
    triggers = features_with_reactive_dependents(view_def)
    occurred_at = occurred_at or _now()
    events: list[FeatureUpdated] = []
    for result in results:
        if result.ref.name not in triggers:
            continue
        entity = EntityRef(
            entity_type=entity_type, entity_key=dict(result.entity_key.parts)
        )
        events.append(
            FeatureUpdated(
                update_id=uuid4().hex,
                entity=entity,
                view=view_def.name,
                view_version=view_def.view_version,
                feature_name=result.ref.name,
                feature_version=result.ref.version,
                data_ts=result.data_ts,
                calc_ts=result.calc_ts,
                source=source,
                occurred_at=occurred_at,
                max_input_data_ts=result.max_input_data_ts,
                input_fingerprint=result.input_fingerprint,
                value_hash=result.value_hash,
                run_id=run_id,
                job_id=job_id,
                manifest_id=manifest_id,
            )
        )
    return events


def publish_feature_updates(
    publisher: EventPublisher, events: list[FeatureUpdated]
) -> None:
    """Publish each event to ``fp.feature-updates`` keyed by entity (per-entity ordering).

    Publish failures propagate so the caller can withhold its source-offset commit (Kafka
    consumers) or record a visible ``propagation_enqueue_status=failed`` (sync imports).
    """
    for event in events:
        publisher.publish(FEATURE_UPDATES, event.entity.encoded(), event)


def emit_feature_updates(
    *,
    publisher: EventPublisher,
    view_def: FeatureViewDef,
    results: list[FeatureResult],
    entity_type: str,
    source: str,
    run_id: str | None = None,
    job_id: str | None = None,
    manifest_id: str | None = None,
) -> int:
    """The single shared emitter seam: build + publish FeatureUpdated, return the count.

    Every durable-offline-write emitter (Offline Writer, Batch worker, table/DWH imports)
    calls exactly this, so propagation logic is never duplicated per emitter.
    """
    events = build_feature_updated_events(
        view_def=view_def,
        results=results,
        entity_type=entity_type,
        source=source,
        run_id=run_id,
        job_id=job_id,
        manifest_id=manifest_id,
    )
    publish_feature_updates(publisher, events)
    return len(events)


# --- reverse-DAG recompute planner -------------------------------------------


@dataclass(frozen=True)
class RecomputeCandidate:
    """One reactive dependent to recompute because an input feature was updated."""

    entity: EntityRef
    view: str
    view_version: int
    dependent_feature: str
    dependent_version: int
    changed_input_feature: str
    changed_input_version: int
    policy: str
    reason: str


def plan_recompute_wave(
    registry: Registry, event: FeatureUpdated
) -> list[RecomputeCandidate]:
    """Reverse-DAG planner: reactive dependents of the updated feature (deterministic).

    Only ``reactive`` edges produce candidates; the version pin must match the updated
    feature version. Cycles/depth are already rejected at registry build; waves
    chain one level at a time via downstream re-emission, so this returns direct dependents
    only.
    """
    view_def = find_view(registry, event.view, event.view_version)
    if view_def is None:
        return []
    index = reactive_dependents(view_def)
    features_by_name = {f.name: f for f in view_def.features}
    candidates: list[RecomputeCandidate] = []
    for dependent_name, dep in index.get(event.feature_name, []):
        dependent = features_by_name[dependent_name]
        resolved_version = dep.version or event.feature_version
        if resolved_version != event.feature_version:
            continue
        candidates.append(
            RecomputeCandidate(
                entity=event.entity,
                view=event.view,
                view_version=event.view_version,
                dependent_feature=dependent_name,
                dependent_version=dependent.feature_version,
                changed_input_feature=event.feature_name,
                changed_input_version=event.feature_version,
                policy=PROPAGATION_REACTIVE,
                reason="reactive_edge",
            )
        )
    candidates.sort(key=lambda c: (c.dependent_feature, c.dependent_version))
    return candidates


# --- debounce ----------------------------------------------------------------


@dataclass
class DebounceEntry:
    """One coalesced recompute unit keyed by entity + dependent feature.

    Carries the latest input watermark (``latest_data_ts``/``latest_calc_ts``) and the set
    of trigger update ids that coalesced into it — counts/ids only, never values.
    """

    entity: EntityRef
    view: str
    view_version: int
    dependent_feature: str
    dependent_version: int
    changed_input_feature: str
    changed_input_version: int
    latest_update_id: str
    latest_data_ts: datetime
    latest_calc_ts: datetime
    first_seen_at: datetime
    updated_at: datetime
    count: int = 1
    trigger_update_ids: list[str] = field(default_factory=list)


class DebounceStore:
    """In-memory debounce: many updates for one ``entity+dependent`` -> one recompute.

    Durable/timed debounce is a documented later-hardening deferral; this in-process store
    is enough here (it proves the consumer never enqueues duplicate work within one
    process). ``observe`` coalesces by key and keeps the latest input watermark; ``drain``
    returns the pending units and clears the store.
    """

    def __init__(self) -> None:
        self._entries: dict[tuple, DebounceEntry] = {}

    @staticmethod
    def _key(candidate: RecomputeCandidate) -> tuple:
        return (
            candidate.entity.entity_type,
            candidate.entity.encoded(),
            candidate.view,
            candidate.view_version,
            candidate.dependent_feature,
            candidate.dependent_version,
        )

    def observe(
        self,
        candidate: RecomputeCandidate,
        event: FeatureUpdated,
        now: datetime | None = None,
    ) -> bool:
        """Record an update for a candidate. Returns True if this created a new unit.

        A repeat within the window for the same key coalesces (no new unit) and advances
        the watermark to the freshest update seen.
        """
        now = now or _now()
        key = self._key(candidate)
        entry = self._entries.get(key)
        if entry is None:
            self._entries[key] = DebounceEntry(
                entity=candidate.entity,
                view=candidate.view,
                view_version=candidate.view_version,
                dependent_feature=candidate.dependent_feature,
                dependent_version=candidate.dependent_version,
                changed_input_feature=candidate.changed_input_feature,
                changed_input_version=candidate.changed_input_version,
                latest_update_id=event.update_id,
                latest_data_ts=event.data_ts,
                latest_calc_ts=event.calc_ts,
                first_seen_at=now,
                updated_at=now,
                count=1,
                trigger_update_ids=[event.update_id],
            )
            return True
        entry.count += 1
        entry.updated_at = now
        entry.trigger_update_ids.append(event.update_id)
        if event.data_ts >= entry.latest_data_ts:
            entry.latest_update_id = event.update_id
            entry.latest_data_ts = event.data_ts
            entry.latest_calc_ts = event.calc_ts
        return False

    def pending(self) -> list[DebounceEntry]:
        return list(self._entries.values())

    def drain(self) -> list[DebounceEntry]:
        entries = list(self._entries.values())
        self._entries.clear()
        return entries
