"""Pure Offline Writer handler.

Consumes a ``FeatureOfflineWriteRequested`` (the durable, values-bearing event the
online worker publishes after the online write) and appends the computed values to
offline feature history **idempotently**, reusing the same dedup as the sync path
(``partition_new_results`` + ``offline.append_many``).

It writes only offline history — no online writes, no status events, no DLQ/retry. The
runner commits the offline-write message only after the append succeeds, so a replay
re-appends nothing (exact-duplicate dedup).
"""

from __future__ import annotations

from dataclasses import dataclass

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.fs_core.dedup import partition_new_results
from fintech_feature_platform.fs_core.events.models import (
    FEATURE_UPDATE_SOURCE_OFFLINE_WRITER,
    FeatureOfflineWriteRequested,
)
from fintech_feature_platform.fs_core.observability.metrics import recorder_of
from fintech_feature_platform.fs_core.propagation import emit_feature_updates, find_view


@dataclass(frozen=True)
class OfflineWriteResult:
    # status: ok | invalid_event | append_failed | propagation_publish_failed
    #       | unexpected_error
    status: str
    view: str
    view_version: int
    entity_key: str
    received_count: int = 0
    new_count: int = 0
    duplicates_skipped: int = 0
    propagation_updates_emitted: int = 0
    error: str | None = None


def handle_feature_offline_write(
    backend: AppBackend, event: FeatureOfflineWriteRequested
) -> OfflineWriteResult:
    """Append the event's computed values to offline history, idempotently."""
    try:
        write_set = event.write_set
        view = write_set.view
        view_version = write_set.view_version
        entity_key = write_set.entity_key.encode()
        results = list(write_set.results.values())
    except Exception as exc:  # noqa: BLE001 - malformed event payload
        return OfflineWriteResult(
            status="invalid_event", view="", view_version=0, entity_key="", error=str(exc)
        )

    try:
        new_results, duplicates_skipped = partition_new_results(
            backend.offline, view, view_version, results
        )
        backend.offline.append_many(view, view_version, new_results)
    except Exception as exc:  # noqa: BLE001 - transient/store failure: do not commit
        recorder_of(backend).incr("offline_append_errors_total")
        return OfflineWriteResult(
            status="append_failed",
            view=view,
            view_version=view_version,
            entity_key=entity_key,
            received_count=len(results),
            error=str(exc),
        )
    recorder_of(backend).incr("offline_append_rows_total", value=len(new_results))

    # Emit values-free FeatureUpdated AFTER the durable offline append and BEFORE the source
    # offset commits. Emit for all results (not just new_results) so a replay after a publish
    # failure still announces the durably-written features. A publish failure is transient:
    # the runner must NOT commit, and replay is safe (offline dedup is idempotent).
    view_def = find_view(backend.registry, view, view_version)
    emitted = 0
    if view_def is not None:
        try:
            emitted = emit_feature_updates(
                publisher=backend.events,
                view_def=view_def,
                results=results,
                entity_type=view_def.entity,
                source=FEATURE_UPDATE_SOURCE_OFFLINE_WRITER,
                run_id=write_set.run_id,
                job_id=write_set.job_id,
            )
        except Exception as exc:  # noqa: BLE001 - publish failure: do not commit, replay
            return OfflineWriteResult(
                status="propagation_publish_failed",
                view=view,
                view_version=view_version,
                entity_key=entity_key,
                received_count=len(results),
                new_count=len(new_results),
                duplicates_skipped=duplicates_skipped,
                error=str(exc),
            )

    return OfflineWriteResult(
        status="ok",
        view=view,
        view_version=view_version,
        entity_key=entity_key,
        received_count=len(results),
        new_count=len(new_results),
        duplicates_skipped=duplicates_skipped,
        propagation_updates_emitted=emitted,
    )
