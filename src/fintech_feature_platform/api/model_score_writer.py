"""Pure model score writer handler (Kafka-first writeback).

``handle_model_score_write`` consumes one ``ModelScoreWriteRequested``, resolves each
score feature's version from the registry (rejecting non-``model_score`` features),
builds a ``FeatureWriteSet`` (``run_id = score_write_id``), writes the online store
**first** (existing CAS/freshness) when ``write_online``, and republishes a
``FeatureOfflineWriteRequested`` so the existing Offline Writer persists history when
``write_offline``.

The platform never executes the model — it materializes the supplied outputs. This
module never calls ``ComputeCore`` / ``FeatureStore.compute`` / ``compute_write_set``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.fs_core.events.models import (
    FeatureOfflineWriteRequested,
    ModelScoreWriteRequested,
)
from fintech_feature_platform.fs_core.events.topics import FEATURE_WRITE_OFFLINE
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureRef,
    FeatureResult,
    FeatureWriteSet,
)
from fintech_feature_platform.fs_core.registry.models import FeatureViewDef, Registry


@dataclass(frozen=True)
class WriterResult:
    # status: ok | invalid_event | online_write_failed | publish_failed
    status: str
    score_write_id: str
    # Per-feature D9 outcome strings (scores degenerate to CAS-on-data_ts).
    online_written: dict[str, str] = field(default_factory=dict)
    downstream_published: bool = False
    error: str | None = None


def _find_view(registry: Registry, name: str, version: int) -> FeatureViewDef:
    return registry.find_view(name, version)  # ONE authoritative lookup


def handle_model_score_write(
    backend: AppBackend, event: ModelScoreWriteRequested
) -> WriterResult:
    """Materialize externally-supplied model scores (online-first, then offline event)."""
    # --- validate + build the write set (deterministic; failures are poison) ------
    try:
        if not event.write_online and not event.write_offline:
            raise ValueError("write_online and write_offline are both false")
        view_def = _find_view(backend.registry, event.view, event.view_version)
        features_by_name = {f.name: f for f in view_def.features}
        entity_key = EntityKey.from_mapping(
            {field_name: event.entity.entity_key[field_name] for field_name in view_def.key_fields},
            key_order=view_def.key_fields,
        )
        results: dict[str, FeatureResult] = {}
        for score in event.scores:
            feature_def = features_by_name.get(score.feature)
            if feature_def is None:
                raise ValueError(f"unknown feature {score.feature!r} in view {event.view!r}")
            if feature_def.kind != "model_score":
                raise ValueError(
                    f"feature {score.feature!r} is not a model_score feature"
                )
            results[score.feature] = FeatureResult(
                ref=FeatureRef(score.feature, feature_def.feature_version),
                entity_key=entity_key,
                value=score.value,
                data_ts=score.data_ts,
                calc_ts=score.calc_ts,
            )
        write_set = FeatureWriteSet(
            view=event.view,
            view_version=event.view_version,
            entity_key=entity_key,
            results=results,
            source_refs={},
            request_id=event.score_write_id,
            job_id=event.score_write_id,
            run_id=event.score_write_id,
            correlation_id=event.correlation_id,
        )
    except (KeyError, TypeError, ValueError) as exc:
        return WriterResult(
            status="invalid_event", score_write_id=event.score_write_id, error=str(exc)
        )

    # --- online first (existing CAS/freshness) -----------------------------------
    online_written: dict[str, str] = {}
    if event.write_online:
        try:
            online_written = backend.online.write_many(
                event.view, event.view_version, list(write_set.results.values())
            )
        except Exception as exc:  # noqa: BLE001 - retryable: do not commit, replay
            return WriterResult(
                status="online_write_failed",
                score_write_id=event.score_write_id,
                error=str(exc),
            )

    # --- durable offline-write event (reuse the Offline Writer) ------------------
    if event.write_offline:
        offline_event = FeatureOfflineWriteRequested(
            request_id=event.score_write_id,
            job_id=event.score_write_id,
            correlation_id=event.correlation_id,
            occurred_at=datetime.now(tz=UTC),
            write_set=write_set,
        )
        try:
            backend.events.publish(
                FEATURE_WRITE_OFFLINE, entity_key.encode(), offline_event
            )
        except Exception as exc:  # noqa: BLE001 - online done; do not commit, replay-safe
            return WriterResult(
                status="publish_failed",
                score_write_id=event.score_write_id,
                online_written=online_written,
                error=str(exc),
            )

    return WriterResult(
        status="ok",
        score_write_id=event.score_write_id,
        online_written=online_written,
        downstream_published=event.write_offline,
    )
