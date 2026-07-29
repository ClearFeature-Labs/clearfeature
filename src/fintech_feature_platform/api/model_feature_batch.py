"""Batch-only F3 model-as-feature compute.

An F3 model feature (registry ``kind: "model"``) is computed **only** here — never by
ComputeCore, never online. This path:

1. reads each entity's declared input feature values from offline history, PIT-safe
   (``data_ts <= observation_ts`` and ``calc_ts <= observation_ts`` via the earlier selector,
   ``safety_gap`` honored) — a required input with no eligible value skips the entity and
   the model is **not** called for it;
2. builds a vector frame (one row per eligible entity) and calls the ``ModelRunner`` **once**
   for the whole batch (vector-first contract);
3. writes the model outputs as offline ``FeatureResult`` rows with D3/D9 metadata
   (``data_ts=min(inputs)``, ``max_input_data_ts=max(inputs)``, ``input_fingerprint`` over
   the input refs **plus the pinned model identity**, ``value_hash`` of the output) and
   model lineage (``model_uri``/``model_digest``/``model_output_name``);
4. emits values-free ``FeatureUpdated`` after the durable append when the F3 output has a
   reactive dependent (shared recompute seam).

Returns bounded, values-free accounting only — no feature values, payloads, model artifact
bytes, ``object_key``/``storage_uri``, SQL, or DWH rows.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from time import perf_counter

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.fs_core.dedup import partition_new_results
from fintech_feature_platform.fs_core.events.models import (
    FEATURE_UPDATE_SOURCE_BATCH_WORKER,
)
from fintech_feature_platform.fs_core.hashing import input_fingerprint, value_hash
from fintech_feature_platform.fs_core.model_runner import ModelRef, ModelRunner
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.observability.metrics import recorder_of
from fintech_feature_platform.fs_core.propagation import emit_feature_updates, find_view
from fintech_feature_platform.fs_core.registry.models import FeatureDef, FeatureViewDef

# Beta supports MLflow-pinned models only; other schemes fail loudly (no live server needed).
SUPPORTED_MODEL_URI_PREFIX = "mlflow://"


@dataclass(frozen=True)
class ModelFeatureBatchResult:
    """Bounded, values-free accounting for one F3 batch compute (lineage refs + counts)."""

    status: str  # ok
    feature: str
    feature_version: int
    model_uri: str
    model_digest: str
    model_output_name: str
    input_feature_refs: tuple[str, ...]
    planned: int = 0
    computed: int = 0
    skipped: int = 0
    duplicates_skipped: int = 0
    downstream_emitted: int = 0
    run_id: str | None = None
    job_id: str | None = None
    wave_id: str | None = None

    def counts(self) -> dict[str, int]:
        return {
            "planned": self.planned,
            "computed": self.computed,
            "skipped": self.skipped,
            "duplicates_skipped": self.duplicates_skipped,
            "downstream_emitted": self.downstream_emitted,
        }


@dataclass
class _Row:
    entity_key: EntityKey
    frame: dict[str, object]
    inputs: list[tuple[str, datetime, str]] = field(default_factory=list)


def _resolve_model_feature(
    view_def: FeatureViewDef, feature_name: str
) -> FeatureDef:
    feature = next((f for f in view_def.features if f.name == feature_name), None)
    if feature is None or feature.kind != "model":
        raise ValueError(
            f"feature {feature_name!r} is not a model (F3) feature in view "
            f"{view_def.name!r}"
        )
    if not feature.model.uri.startswith(SUPPORTED_MODEL_URI_PREFIX):
        raise ValueError(
            f"unsupported model uri {feature.model.uri!r}; only "
            f"{SUPPORTED_MODEL_URI_PREFIX}* is supported in beta"
        )
    return feature


def compute_model_feature_batch(
    backend: AppBackend,
    runner: ModelRunner,
    *,
    view: str,
    view_version: int,
    feature_name: str,
    entity_keys: Sequence[EntityKey],
    observation_ts: datetime,
    safety_gap: timedelta = timedelta(0),
    run_id: str | None = None,
    job_id: str | None = None,
    wave_id: str | None = None,
    emit_source: str = FEATURE_UPDATE_SOURCE_BATCH_WORKER,
) -> ModelFeatureBatchResult:
    """Compute one F3 model feature for a batch of entities (offline-only)."""
    view_def = find_view(backend.registry, view, view_version)
    if view_def is None:
        raise ValueError(f"unknown view {view!r} version {view_version}")
    feature = _resolve_model_feature(view_def, feature_name)
    model = feature.model
    features_by_name = {f.name: f for f in view_def.features}
    input_feature_refs = tuple(
        FeatureRef(
            dep.feature, dep.version or features_by_name[dep.feature].feature_version
        ).encode()
        for dep in feature.deps
    )
    model_ref = ModelRef(uri=model.uri, digest=model.digest, output_name=model.output_name)

    # 1. Read PIT-safe inputs per entity; a missing required input skips the entity.
    rows: list[_Row] = []
    skipped = 0
    for entity_key in entity_keys:
        row = _build_row(
            backend, view, view_version, feature, features_by_name, entity_key,
            observation_ts, safety_gap,
        )
        if row is None:
            skipped += 1
            continue
        rows.append(row)

    # 2. Vector-first: call the model ONCE for the whole batch. Per-feature timing
    # : ONE duration observation per model invocation with the item
    # count incremented by the real number of rows — never one synthetic observation
    # per row (this is the documented vectorized-batch semantics for F3).
    if rows:
        predict_started = perf_counter()
        predictions = runner.predict(model_ref, [row.frame for row in rows])
        feature_id = (
            f"{view_def.name}:v{view_def.view_version}:"
            f"{feature.name}:v{feature.feature_version}"
        )
        metrics = recorder_of(backend)
        try:
            labels = {"execution_mode": "batch", "feature_id": feature_id}
            metrics.observe(
                "fsp_feature_compute_duration_seconds",
                perf_counter() - predict_started, labels,
            )
            metrics.incr("fsp_feature_compute_items_total", labels, value=len(rows))
        except Exception:  # noqa: BLE001, S110 - metrics must never break F3 compute
            pass
    else:
        predictions = []
    if len(predictions) != len(rows):
        raise ValueError(
            f"model runner returned {len(predictions)} predictions for {len(rows)} rows"
        )

    # 3. Build offline rows with D3/D9 metadata + model lineage.
    results: list[FeatureResult] = []
    for row, prediction in zip(rows, predictions, strict=True):
        input_ts = [ts for _, ts, _ in row.inputs]
        fingerprint_inputs = [
            *row.inputs,
            (f"model:{model.uri}#{model.output_name}", min(input_ts), model.digest),
        ]
        results.append(
            FeatureResult(
                ref=FeatureRef(feature_name, feature.feature_version),
                entity_key=row.entity_key,
                value=prediction,
                data_ts=min(input_ts),
                calc_ts=observation_ts,
                max_input_data_ts=max(input_ts),
                input_fingerprint=input_fingerprint(fingerprint_inputs),
                value_hash=value_hash(prediction),
                model_uri=model.uri,
                model_digest=model.digest,
                model_output_name=model.output_name,
            )
        )

    # 4. Durable offline append (bulk), then values-free downstream emission.
    new_results, duplicates = partition_new_results(
        backend.offline, view, view_version, results
    )
    backend.offline.append_many(view, view_version, new_results)
    downstream = emit_feature_updates(
        publisher=backend.events,
        view_def=view_def,
        results=results,
        entity_type=view_def.entity,
        source=emit_source,
        run_id=run_id or wave_id,
        job_id=job_id,
    )

    return ModelFeatureBatchResult(
        status="ok",
        feature=feature_name,
        feature_version=feature.feature_version,
        model_uri=model.uri,
        model_digest=model.digest,
        model_output_name=model.output_name,
        input_feature_refs=input_feature_refs,
        planned=len(entity_keys),
        computed=len(results),
        skipped=skipped,
        duplicates_skipped=duplicates,
        downstream_emitted=downstream,
        run_id=run_id,
        job_id=job_id,
        wave_id=wave_id,
    )


def _build_row(
    backend: AppBackend,
    view: str,
    view_version: int,
    feature: FeatureDef,
    features_by_name: dict[str, FeatureDef],
    entity_key: EntityKey,
    observation_ts: datetime,
    safety_gap: timedelta,
) -> _Row | None:
    """Read one entity's PIT-safe input feature values; ``None`` if a required one is absent."""
    row = _Row(entity_key=entity_key, frame={})
    for dep in feature.deps:
        version = dep.version or features_by_name[dep.feature].feature_version
        record = backend.offline.get_pit(
            entity_key,
            feature_name=dep.feature,
            feature_version=version,
            view=view,
            view_version=view_version,
            observation_ts=observation_ts,
            safety_gap=safety_gap,
        )
        if record is None:
            if dep.required:
                return None  # required input missing -> skip entity, do not call model
            continue  # optional input omitted from the frame
        r = record.result
        row.frame[dep.feature] = r.value
        row.inputs.append(
            (
                FeatureRef(dep.feature, version).encode(),
                r.data_ts,
                r.value_hash or value_hash(r.value),
            )
        )
    if not row.inputs:  # nothing eligible to score
        return None
    return row
