"""Runtime isolation: batch pause / rate limit + guarded Mode-2 online refresh."""

import json

from fastapi.testclient import TestClient

from fintech_feature_platform.api import batch_worker_runner
from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.batch_controls import (
    ConfiguredBatchRuntimeControls,
    DisabledRateLimiter,
    NoopBatchRuntimeControls,
    UnlimitedRateLimiter,
)
from fintech_feature_platform.api.batch_worker import handle_batch_chunk
from fintech_feature_platform.api.jsonl_ingestion import run_jsonl_ingestion
from fintech_feature_platform.fs_core.events.consumer import (
    InMemoryEventConsumer,
    InMemoryMessage,
)
from fintech_feature_platform.fs_core.events.models import BatchChunkRequested
from fintech_feature_platform.fs_core.events.topics import (
    BATCH_JOB_EVENTS,
    FEATURE_COMPUTE_BATCH,
)
from fintech_feature_platform.fs_core.models import EntityKey

_VIEW = "user_credit_risk"
_KEY_ORDER = ["user_id", "application_id"]


def _row(user_id="1", *, income=100_000):
    return json.dumps(
        {
            "entity_key": {"user_id": user_id, "application_id": "A1"},
            "event_ts": "2026-07-01T00:00:00Z",
            "payload": {"declared_income": income, "monthly_obligations": 700_000},
        }
    )


def _ingest(backend, rows):
    return run_jsonl_ingestion(
        backend=backend, lines=rows, entity_type="application",
        source_name="credit_report", report_type="credit_report",
    )


def _batch_body(manifest_id, *, features=None, write_online=False):
    return {
        "view": _VIEW, "view_version": 1,
        "requested_features": features or ["declared_income"],
        "scope": {"type": "source_dataset_manifest", "manifest_id": manifest_id},
        "idempotency_key": "batch-iso-1", "write_online": write_online,
    }


def _key(user_id="1"):
    return EntityKey.from_mapping(
        {"user_id": user_id, "application_id": "A1"}, key_order=_KEY_ORDER
    )


def _setup(backend, rows=None, *, features=None, write_online=False):
    manifest = _ingest(backend, rows if rows is not None else [_row("1")])
    TestClient(create_app(backend)).post(
        "/v1/batch/jobs",
        json=_batch_body(manifest.manifest_id, features=features, write_online=write_online),
    )
    return [
        r.event for r in backend.events.published
        if r.topic == FEATURE_COMPUTE_BATCH and isinstance(r.event, BatchChunkRequested)
    ][0]


def _controls(**kw):
    base = dict(
        rate_limiter=UnlimitedRateLimiter(),
        online_refresh_limiter=UnlimitedRateLimiter(),
    )
    base.update(kw)
    return ConfiguredBatchRuntimeControls(**base)


# --- pause / rate limit (runner) ---------------------------------------------

def test_runner_paused_does_not_compute_or_commit():
    backend = build_memory_backend()
    event = _setup(backend)
    controls = _controls(pause_enabled=True, max_consumer_lag=10, lag_fn=lambda: 999)
    consumer = InMemoryEventConsumer([InMemoryMessage(event.to_json())])
    published_before = len(backend.events.published)

    result = batch_worker_runner.process_next(consumer, backend, controls=controls)

    assert result.status == "paused"
    assert result.committed is False
    assert result.reason and "lag" in result.reason
    assert consumer.committed == []
    # No compute, no offline write, no BatchChunkProcessed.
    assert backend.offline.get(_key("1"), feature_name="declared_income") == []
    processed = [
        r for r in backend.events.published[published_before:]
        if r.topic == BATCH_JOB_EVENTS
    ]
    assert processed == []


def test_runner_rate_limited_does_not_commit():
    backend = build_memory_backend()
    event = _setup(backend)
    controls = _controls(rate_limiter=DisabledRateLimiter())  # no chunk tokens
    consumer = InMemoryEventConsumer([InMemoryMessage(event.to_json())])

    result = batch_worker_runner.process_next(consumer, backend, controls=controls)

    assert result.status == "rate_limited"
    assert result.committed is False
    assert consumer.committed == []
    assert backend.offline.get(_key("1"), feature_name="declared_income") == []


def test_runner_not_paused_processes_and_commits():
    backend = build_memory_backend()
    event = _setup(backend)
    consumer = InMemoryEventConsumer([InMemoryMessage(event.to_json())])
    result = batch_worker_runner.process_next(
        consumer, backend, controls=NoopBatchRuntimeControls()
    )
    assert result.status == "ok"
    assert result.committed is True
    assert backend.offline.get(_key("1"), feature_name="declared_income")


# --- run-loop backoff on non-commit control statuses -------------------------

def test_run_backs_off_when_paused():
    backend = build_memory_backend()
    event = _setup(backend)
    controls = _controls(pause_enabled=True, max_consumer_lag=0, lag_fn=lambda: 5)
    consumer = InMemoryEventConsumer([InMemoryMessage(event.to_json())])
    slept: list[float] = []

    results = batch_worker_runner.run(
        consumer, backend, controls=controls, max_messages=1,
        pause_backoff_s=0.5, sleep_fn=slept.append,
    )

    assert results[0].status == "paused"
    assert results[0].committed is False
    assert consumer.committed == []
    assert slept == [0.5]  # backed off once, exactly the configured duration


def test_run_backs_off_when_rate_limited():
    backend = build_memory_backend()
    event = _setup(backend)
    controls = _controls(rate_limiter=DisabledRateLimiter())
    consumer = InMemoryEventConsumer([InMemoryMessage(event.to_json())])
    slept: list[float] = []

    results = batch_worker_runner.run(
        consumer, backend, controls=controls, max_messages=1,
        pause_backoff_s=0.25, sleep_fn=slept.append,
    )

    assert results[0].status == "rate_limited"
    assert results[0].committed is False
    assert slept == [0.25]


def test_run_does_not_back_off_on_ok():
    backend = build_memory_backend()
    event = _setup(backend)
    consumer = InMemoryEventConsumer([InMemoryMessage(event.to_json())])
    slept: list[float] = []

    results = batch_worker_runner.run(
        consumer, backend, controls=NoopBatchRuntimeControls(), max_messages=1,
        pause_backoff_s=0.5, sleep_fn=slept.append,
    )

    assert results[0].status == "ok"
    assert results[0].committed is True
    assert slept == []  # no control backoff on a committed chunk


# --- guarded Mode-2 online refresh (worker) ----------------------------------

def test_guarded_refresh_writes_offline_before_online():
    backend = build_memory_backend()
    event = _setup(backend, write_online=True)
    order: list[str] = []
    orig_append = backend.offline.append_many
    orig_write = backend.online.write

    def _append(*a, **k):
        order.append("offline")
        return orig_append(*a, **k)

    def _write(*a, **k):
        order.append("online")
        return orig_write(*a, **k)

    backend.offline.append_many = _append
    backend.online.write = _write

    handle_batch_chunk(backend, event, NoopBatchRuntimeControls())
    # Offline is primary: append happens before any online refresh write.
    assert order[0] == "offline"
    assert "online" in order


def test_guarded_refresh_counts_d9_outcomes():
    backend = build_memory_backend()
    event = _setup(backend, write_online=True)
    result = handle_batch_chunk(backend, event, NoopBatchRuntimeControls())
    counts = result.online_refresh_counts
    assert counts is not None
    assert counts["online_refresh_attempted"] == 1
    assert counts["online_refresh_written"] == 1
    # Value is actually online now.
    assert backend.online.get(_VIEW, 1, _key("1"), "declared_income", 1) is not None


def test_guarded_refresh_skipped_stale_counted():
    backend = build_memory_backend()
    # Seed a FRESHER online value so the batch (older data_ts) is D9 skipped_stale.
    from datetime import UTC, datetime

    from fintech_feature_platform.fs_core.models import FeatureRef, FeatureResult
    backend.online.write(_VIEW, 1, FeatureResult(
        ref=FeatureRef("declared_income", 1), entity_key=_key("1"), value=999,
        data_ts=datetime(2030, 1, 1, tzinfo=UTC), calc_ts=datetime(2030, 1, 1, tzinfo=UTC),
    ))
    event = _setup(backend, write_online=True)
    result = handle_batch_chunk(backend, event, NoopBatchRuntimeControls())
    assert result.online_refresh_counts["online_refresh_skipped_stale"] == 1
    # The fresher online value is untouched.
    assert backend.online.get(_VIEW, 1, _key("1"), "declared_income", 1).value == 999


def test_guarded_refresh_rate_limited_by_max_features():
    backend = build_memory_backend()
    # Two features per item -> cap at 1 -> 1 attempted, 1 rate_limited; chunk still ok.
    event = _setup(
        backend, features=["declared_income", "monthly_obligations"], write_online=True
    )
    controls = _controls(online_refresh_max_features=1)
    result = handle_batch_chunk(backend, event, controls)
    counts = result.online_refresh_counts
    assert counts["online_refresh_attempted"] == 1
    assert counts["online_refresh_rate_limited"] == 1
    assert result.status == "ok"  # offline chunk completed regardless
    assert len(backend.offline.get(_key("1"))) == 2  # both features written offline


def test_guarded_refresh_store_failure_counted_not_fatal():
    backend = build_memory_backend()
    event = _setup(backend, write_online=True)

    def _boom(*a, **k):
        raise RuntimeError("online store down")

    backend.online.write = _boom
    result = handle_batch_chunk(backend, event, NoopBatchRuntimeControls())
    # Online refresh failure is secondary: counted, chunk still ok, offline intact.
    assert result.status == "ok"
    assert result.online_refresh_counts["online_refresh_failed"] == 1
    assert backend.offline.get(_key("1"), feature_name="declared_income")


def test_refresh_disabled_writes_nothing_online():
    backend = build_memory_backend()
    event = _setup(backend, write_online=True)
    controls = _controls(online_refresh_limiter=DisabledRateLimiter())
    result = handle_batch_chunk(backend, event, controls)
    assert result.online_refresh_counts["online_refresh_rate_limited"] == 1
    assert result.online_refresh_counts["online_refresh_attempted"] == 0
    assert backend.online.get(_VIEW, 1, _key("1"), "declared_income", 1) is None


def test_processed_event_has_counts_only_no_values():
    backend = build_memory_backend()
    # Alphabetic sentinel value can't collide with the manifest_id UUID hex in the event.
    event = _setup(backend, [_row("1", income="SENTINEL_VAL")], write_online=True)
    handle_batch_chunk(backend, event, NoopBatchRuntimeControls())
    processed = [
        r.event for r in backend.events.published if r.topic == BATCH_JOB_EVENTS
    ][-1]
    raw = processed.to_json().decode()
    assert "online_refresh_counts" in raw
    assert "SENTINEL_VAL" not in raw  # no feature value
    assert "payload" not in raw
    assert "object_key" not in raw
    assert "storage_uri" not in raw


def test_offline_only_manifest_job_has_no_refresh_counts():
    backend = build_memory_backend()
    event = _setup(backend, write_online=False)
    result = handle_batch_chunk(backend, event, NoopBatchRuntimeControls())
    assert result.online_refresh_counts is None
    assert backend.online.get(_VIEW, 1, _key("1"), "declared_income", 1) is None
