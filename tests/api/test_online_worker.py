"""Tests for the pure online worker handler (Valkey-first, no Kafka)."""

import dataclasses
from datetime import UTC, datetime, timedelta

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.online_worker import (
    handle_feature_compute_requested,
)
from fintech_feature_platform.fs_core.events.models import (
    EntityRef,
    FeatureComputeRequested,
    ReportDescriptor,
)
from fintech_feature_platform.fs_core.events.topics import (
    FEATURE_COMPUTE_COMPLETED,
    FEATURE_WRITE_OFFLINE,
)
from fintech_feature_platform.fs_core.models import EntityKey

_TS = datetime(2026, 6, 27, 10, tzinfo=UTC)
_ENTITY = {"user_id": "1", "application_id": "A1"}
_OBJECT_KEY = "mem://rep_credit_worker"


def _entity_key() -> EntityKey:
    return EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])


def _backend_with_payload():
    backend = build_memory_backend()
    backend.payloads.put(
        _OBJECT_KEY,
        {"declared_income": 4_200_000, "monthly_obligations": 800_000},
    )
    return backend


def _event() -> FeatureComputeRequested:
    descriptor = ReportDescriptor(
        report_ref="rep_credit_worker",
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
        request_id="freq_w",
        job_id="job_w",
        priority="online",
        deadline_ms=1000,
        entity=EntityRef("application", dict(_ENTITY)),
        view="user_credit_risk",
        view_version=1,
        reports=[descriptor],
        write_policy="online_first",
        idempotency_key="idem_w",
        correlation_id="corr_w",
        occurred_at=_TS,
        requested_features=["declared_income"],
    )


def test_handler_expands_group_and_writes_outputs():
    # affordability_input_v1 = [declared_income, monthly_obligations] (credit_report only)
    backend = _backend_with_payload()
    event = dataclasses.replace(
        _event(), requested_features=[], requested_feature_groups=["affordability_input_v1"]
    )
    result = handle_feature_compute_requested(backend, event)
    assert result.status == "ok"
    ek = _entity_key()
    assert backend.online.get("user_credit_risk", 1, ek, "declared_income", 1) is not None
    assert (
        backend.online.get("user_credit_risk", 1, ek, "monthly_obligations", 1)
        is not None
    )


def test_handler_does_not_materialize_dependencies():
    # Requesting only debt_to_income_ratio computes its deps but must not write them.
    backend = build_memory_backend()
    backend.payloads.put(
        "mem://rep_credit_worker",
        {"declared_income": 4_200_000, "monthly_obligations": 800_000},
    )
    backend.payloads.put("mem://rep_tax_worker", {"income": 3_000_000})
    credit = ReportDescriptor(
        report_ref="rep_credit_worker", source_name="credit_report",
        report_type="credit_report", schema_version="v1", report_ts=_TS,
        object_key="mem://rep_credit_worker", content_hash="sha256:x", size_bytes=10,
        compression="none", format="json",
    )
    tax = ReportDescriptor(
        report_ref="rep_tax_worker", source_name="tax_report",
        report_type="tax_report", schema_version="v1", report_ts=_TS,
        object_key="mem://rep_tax_worker", content_hash="sha256:y", size_bytes=10,
        compression="none", format="json",
    )
    event = dataclasses.replace(
        _event(), reports=[credit, tax], requested_features=["debt_to_income_ratio"],
        requested_feature_groups=[],
    )
    result = handle_feature_compute_requested(backend, event)
    assert result.status == "ok"
    ek = _entity_key()
    assert (
        backend.online.get("user_credit_risk", 1, ek, "debt_to_income_ratio", 1)
        is not None
    )
    # dependencies are computed but NOT materialized in V1 (ephemeral)
    assert backend.online.get("user_credit_risk", 1, ek, "monthly_obligations", 1) is None
    assert backend.online.get("user_credit_risk", 1, ek, "income_from_tax", 1) is None


def test_handler_does_not_append_offline():
    backend = _backend_with_payload()
    result = handle_feature_compute_requested(backend, _event())
    assert result.status == "ok"
    # The worker writes Valkey-first only; offline history is NOT written here.
    assert backend.offline.get(_entity_key()) == []


def test_handler_writes_online_after_compute():
    backend = _backend_with_payload()
    handle_feature_compute_requested(backend, _event())
    got = backend.online.get(
        "user_credit_risk", 1, _entity_key(), "declared_income", 1
    )
    assert got is not None and got.value == 4_200_000


def test_handler_publishes_completion_after_online_write():
    backend = _backend_with_payload()
    result = handle_feature_compute_requested(backend, _event())
    assert result.downstream_published is True
    topics = [r.topic for r in backend.events.published]
    # offline-write event is published BEFORE the completion status event
    assert topics == [FEATURE_WRITE_OFFLINE, FEATURE_COMPUTE_COMPLETED]
    completion = backend.events.published[-1]
    assert completion.event.event_type == "feature_compute.completed"
    assert "declared_income" in completion.event.written_features


def test_handler_publishes_values_bearing_offline_write_event():
    backend = _backend_with_payload()
    handle_feature_compute_requested(backend, _event())

    offline = backend.events.published[0]
    assert offline.topic == FEATURE_WRITE_OFFLINE
    assert offline.event.event_type == "feature_write.offline.requested"
    # the event carries the computed value (durable, replayable into offline history)
    result = offline.event.write_set.results["declared_income"]
    assert result.value == 4_200_000


def test_handler_offline_write_publish_failure_blocks_completion():
    class _OfflineWriteFailsPublisher:
        def __init__(self):
            self.published = []

        def publish(self, topic, key, event):
            if topic == FEATURE_WRITE_OFFLINE:
                raise RuntimeError("broker down")
            self.published.append((topic, key, event))

    backend = dataclasses.replace(
        _backend_with_payload(), events=_OfflineWriteFailsPublisher()
    )
    result = handle_feature_compute_requested(backend, _event())

    assert result.status == "publish_failed"
    assert result.downstream_published is False
    # completion was never published because the offline-write event failed first
    assert backend.events.published == []
    # online was still written (Valkey-first); replay is CAS-safe
    got = backend.online.get(
        "user_credit_risk", 1, _entity_key(), "declared_income", 1
    )
    assert got is not None and got.value == 4_200_000


def test_handler_does_not_publish_when_online_write_fails():
    class _FailingOnline:
        def write_many(self, view, view_version, results):
            raise RuntimeError("valkey down")

    backend = dataclasses.replace(_backend_with_payload(), online=_FailingOnline())
    result = handle_feature_compute_requested(backend, _event())
    assert result.status == "online_write_failed"
    assert result.downstream_published is False
    # no completion event published when the online write failed
    assert backend.events.published == []


def test_handler_loads_payload_from_descriptor_without_meta_lookup():
    class _RaisingMeta:
        def get_meta(self, report_ref):
            raise AssertionError("worker must not query Postgres metadata")

        def add(self, meta):  # pragma: no cover - not used here
            raise AssertionError("worker must not write metadata")

    backend = dataclasses.replace(_backend_with_payload(), metas=_RaisingMeta())
    result = handle_feature_compute_requested(backend, _event())
    assert result.status == "ok"
    got = backend.online.get(
        "user_credit_risk", 1, _entity_key(), "declared_income", 1
    )
    assert got is not None and got.value == 4_200_000


def test_handler_never_calls_feature_store_compute(monkeypatch):
    backend = _backend_with_payload()

    def _boom(*args, **kwargs):
        raise AssertionError("worker must call compute_write_set, not compute")

    monkeypatch.setattr(backend.store, "compute", _boom)
    result = handle_feature_compute_requested(backend, _event())
    assert result.status == "ok"


def test_handler_compute_failure_on_unknown_view():
    backend = _backend_with_payload()
    event = dataclasses.replace(_event(), view="does_not_exist")
    result = handle_feature_compute_requested(backend, event)
    assert result.status == "compute_failed"
    assert result.downstream_published is False
    assert backend.events.published == []


# --- per-feature data_ts (D3/D9,) -----------------------------------

_TAX_OBJECT_KEY = "mem://rep_tax_worker"
_TAX_TS = datetime(2026, 6, 27, 14, tzinfo=UTC)


def _two_source_event() -> FeatureComputeRequested:
    tax_descriptor = ReportDescriptor(
        report_ref="rep_tax_worker",
        source_name="tax_report",
        report_type="tax_report",
        schema_version="v1",
        report_ts=_TAX_TS,
        object_key=_TAX_OBJECT_KEY,
        content_hash="sha256:tax",
        size_bytes=10,
        compression="none",
        format="json",
    )
    base = _event()
    return dataclasses.replace(
        base,
        reports=[*base.reports, tax_descriptor],
        requested_features=["declared_income", "income_from_tax", "debt_to_income_ratio"],
    )


def test_multi_source_request_gives_each_feature_its_own_data_ts():
    backend = _backend_with_payload()
    backend.payloads.put(_TAX_OBJECT_KEY, {"income": 3_000_000})

    result = handle_feature_compute_requested(backend, _two_source_event())

    assert result.status == "ok"
    results = result.write_set.results
    # F1 features carry their OWN source report_ts — not the request max (_TAX_TS).
    assert results["declared_income"].data_ts == _TS
    assert results["declared_income"].max_input_data_ts == _TS
    assert results["income_from_tax"].data_ts == _TAX_TS
    # Derived: D3 min over inputs, D9 max over inputs.
    ratio = results["debt_to_income_ratio"]
    assert ratio.data_ts == _TS
    assert ratio.max_input_data_ts == _TAX_TS
    assert ratio.input_fingerprint is not None
    assert ratio.value_hash is not None


def test_online_worker_surfaces_f2_node_statuses():
    #: the online worker's result carries the bounded F2 status summary
    # (values-free) via the compute write set; all fresh legacy deps -> OK.
    backend = _backend_with_payload()
    backend.payloads.put(_TAX_OBJECT_KEY, {"income": 3_000_000})

    result = handle_feature_compute_requested(backend, _two_source_event())

    assert result.status == "ok"
    assert result.write_set.node_statuses["debt_to_income_ratio"] == "OK"
    assert result.write_set.node_statuses["income_from_tax"] == "OK"


# --- absolute deadlines  -------------------------------------------

def _published(backend, topic):
    return [r for r in backend.events.published if r.topic == topic]


def test_expired_event_skips_valkey_and_offline_publishes_deadline_expired():
    backend = _backend_with_payload()
    # expires_at in the past -> event has already timed out when the worker sees it.
    now = datetime.now(tz=UTC)
    event = dataclasses.replace(
        _event(),
        event_ts=now - timedelta(seconds=10),
        expires_at=now - timedelta(seconds=5),
    )

    result = handle_feature_compute_requested(backend, event)

    assert result.status == "deadline_expired"
    # No Valkey write.
    assert backend.online.get(
        "user_credit_risk", 1, _entity_key(), "declared_income", 1
    ) is None
    # No values-bearing offline-write event.
    assert _published(backend, FEATURE_WRITE_OFFLINE) == []
    # A completion event with the deadline outcome and no written features / values.
    completions = _published(backend, FEATURE_COMPUTE_COMPLETED)
    assert len(completions) == 1
    completed = completions[0].event
    assert completed.online_write_status == "deadline_expired"
    assert completed.written_features == []
    assert result.write_set is None
    assert result.online_written == {}


def test_expired_event_completion_publish_failure_is_replayable():
    class _FailingPublisher:
        def publish(self, topic, key, event, *, headers=None):
            raise RuntimeError("kafka down")

    backend = dataclasses.replace(_backend_with_payload(), events=_FailingPublisher())
    now = datetime.now(tz=UTC)
    event = dataclasses.replace(
        _event(),
        event_ts=now - timedelta(seconds=10),
        expires_at=now - timedelta(seconds=5),
    )

    result = handle_feature_compute_requested(backend, event)

    # Publish failed -> not a terminal deadline outcome; runner must not commit (replay).
    assert result.status == "publish_failed"


def test_non_expired_event_writes_online_and_reports_status():
    backend = _backend_with_payload()
    now = datetime.now(tz=UTC)
    event = dataclasses.replace(
        _event(),
        event_ts=now,
        expires_at=now + timedelta(seconds=30),
    )

    result = handle_feature_compute_requested(backend, event)

    assert result.status == "ok"
    assert backend.online.get(
        "user_credit_risk", 1, _entity_key(), "declared_income", 1
    ) is not None
    completed = _published(backend, FEATURE_COMPUTE_COMPLETED)[0].event
    assert completed.online_write_status == "written"


def test_event_without_expires_at_never_expires():
    # Backward compatibility: a legacy event (no expires_at) is never deadline-expired.
    backend = _backend_with_payload()
    result = handle_feature_compute_requested(backend, _event())  # expires_at=None
    assert result.status == "ok"
