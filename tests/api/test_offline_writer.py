"""Tests for the Offline Writer handler (idempotent offline append)."""

import dataclasses
from datetime import UTC, datetime

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.offline_writer import handle_feature_offline_write
from fintech_feature_platform.api.online_worker import handle_feature_compute_requested
from fintech_feature_platform.fs_core.events.models import (
    EntityRef,
    FeatureComputeRequested,
    FeatureOfflineWriteRequested,
    ReportDescriptor,
)
from fintech_feature_platform.fs_core.models import EntityKey

_TS = datetime(2026, 6, 27, 10, tzinfo=UTC)
_ENTITY = {"user_id": "1", "application_id": "A1"}
_OBJECT_KEY = "mem://rep_credit_ow"


def _entity_key() -> EntityKey:
    return EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])


def _compute_event() -> FeatureComputeRequested:
    descriptor = ReportDescriptor(
        report_ref="rep_credit_ow",
        source_name="credit_report",
        report_type="credit_report",
        schema_version="v1",
        report_ts=_TS,
        object_key=_OBJECT_KEY,
        content_hash="sha256:x",
        size_bytes=10,
        compression="none",
        format="json",
    )
    return FeatureComputeRequested(
        request_id="freq_ow",
        job_id="job_ow",
        priority="online",
        deadline_ms=1000,
        entity=EntityRef("application", dict(_ENTITY)),
        view="user_credit_risk",
        view_version=1,
        reports=[descriptor],
        write_policy="online_first",
        idempotency_key="idem_ow",
        correlation_id="corr_ow",
        occurred_at=_TS,
        requested_features=["declared_income"],
    )


def _offline_event(backend) -> FeatureOfflineWriteRequested:
    """Produce a real offline-write event via the online worker, round-tripped JSON."""
    backend.payloads.put(
        _OBJECT_KEY, {"declared_income": 4_200_000, "monthly_obligations": 800_000}
    )
    handle_feature_compute_requested(backend, _compute_event())
    published = backend.events.published[0].event  # FeatureOfflineWriteRequested
    # simulate Kafka wire transfer
    return FeatureOfflineWriteRequested.from_json(published.to_json())


def test_offline_writer_appends_history():
    backend = build_memory_backend()
    event = _offline_event(backend)
    # the online worker did NOT append offline
    assert backend.offline.get(_entity_key(), feature_name="declared_income") == []

    result = handle_feature_offline_write(backend, event)

    assert result.status == "ok"
    assert result.new_count == 1
    rows = backend.offline.get(_entity_key(), feature_name="declared_income")
    assert len(rows) == 1
    assert rows[0].result.value == 4_200_000


def test_offline_writer_is_idempotent_on_rerun():
    backend = build_memory_backend()
    event = _offline_event(backend)

    first = handle_feature_offline_write(backend, event)
    second = handle_feature_offline_write(backend, event)

    assert first.new_count == 1
    assert second.new_count == 0
    assert second.duplicates_skipped == 1
    # offline history did not grow on the replay
    assert len(backend.offline.get(_entity_key(), feature_name="declared_income")) == 1


def test_offline_writer_returns_append_failed_on_store_error():
    class _FailingOffline:
        def get(self, *args, **kwargs):
            return []

        def append_many(self, view, view_version, results):
            raise RuntimeError("postgres down")

    backend = build_memory_backend()
    event = _offline_event(backend)
    backend = dataclasses.replace(backend, offline=_FailingOffline())

    result = handle_feature_offline_write(backend, event)
    assert result.status == "append_failed"
