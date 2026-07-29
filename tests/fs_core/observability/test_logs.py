"""Structured operational logging.

Proves: valid one-JSON-object-per-line service output with the stable event contract
(timestamp/level/event/service + bounded fields), level/format env control, the
print_round migration (structured rounds + per-item events; bounded-run CLI prints
kept), technical correlation IDs where they genuinely exist (DLQ/retry/batch chunk),
no secrets in security/readiness events, and no per-feature log flood (per-feature
detail stays in Prometheus).
"""

import io
import json
import logging
from types import SimpleNamespace

from fintech_feature_platform.fs_core.observability.logs import (
    ROOT_LOGGER_NAME,
    configure_logging,
    get_logger,
    log_event,
)

SENTINEL = "sentinel-Bearer-P4ssw0rd-hunter2-DO-NOT-LEAK"


def _capture(service: str = "test-service") -> io.StringIO:
    buffer = io.StringIO()
    configure_logging(service, stream=buffer)
    return buffer


def _teardown() -> None:
    logging.getLogger(ROOT_LOGGER_NAME).handlers.clear()


def _lines(buffer: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in buffer.getvalue().splitlines()]


# --- contract ----------------------------------------------------------------

def test_json_lines_with_stable_event_contract():
    buffer = _capture("online-worker")
    try:
        log_event(
            get_logger("worker"), logging.INFO, "worker_item_completed",
            worker_role="online-worker", operation="process_item",
            result="success", duration_ms=1.5,
        )
        (record,) = _lines(buffer)
        assert record["event"] == "worker_item_completed"  # stable machine identity
        assert record["level"] == "info"
        assert record["service"] == "online-worker"
        assert record["worker_role"] == "online-worker"
        assert record["operation"] == "process_item"
        assert record["result"] == "success"
        assert record["duration_ms"] == 1.5
        assert "timestamp" in record
    finally:
        _teardown()


def test_none_fields_are_dropped_and_reconfigure_is_idempotent():
    buffer = _capture()
    try:
        configure_logging("test-service", stream=buffer)  # second call: one handler
        assert len(logging.getLogger(ROOT_LOGGER_NAME).handlers) == 1
        log_event(get_logger("x"), logging.INFO, "e", present=1, absent=None)
        (record,) = _lines(buffer)
        assert "absent" not in record
    finally:
        _teardown()


def test_log_level_env_controls_output(monkeypatch):
    monkeypatch.setenv("FSP_LOG_LEVEL", "ERROR")
    buffer = _capture()
    try:
        log_event(get_logger("x"), logging.INFO, "suppressed")
        log_event(get_logger("x"), logging.ERROR, "kept")
        assert [r["event"] for r in _lines(buffer)] == ["kept"]
    finally:
        _teardown()


def test_text_format_for_local_development(monkeypatch):
    monkeypatch.setenv("FSP_LOG_FORMAT", "text")
    buffer = _capture()
    try:
        log_event(get_logger("x"), logging.WARNING, "some_event", result="failure")
        line = buffer.getvalue().strip()
        assert line == "WARNING some_event result=failure"
    finally:
        _teardown()


# --- worker item + round events (print_round migration) ----------------------

def test_worker_item_events_by_result(monkeypatch):
    from fintech_feature_platform.api.runner_daemon import record_worker_item

    monkeypatch.setenv("FSP_LOG_LEVEL", "DEBUG")  # per-item success is DEBUG (review C)
    buffer = _capture("offline-writer")
    backend = SimpleNamespace()  # no recorder -> metrics no-op; logging still works
    try:
        record_worker_item(backend, "offline-writer", "written", 0.01)
        record_worker_item(backend, "offline-writer", "noop", 0.01)
        record_worker_item(backend, "offline-writer", "retry_republished", 0.01)
        record_worker_item(backend, "offline-writer", "dead_lettered", 0.01)
        record_worker_item(backend, "offline-writer", "idle", 0.01)  # not an item
        records = _lines(buffer)
        assert [(r["event"], r["level"], r["result"]) for r in records] == [
            ("worker_item_completed", "debug", "success"),
            ("worker_item_completed", "debug", "noop"),
            ("worker_item_failed", "warning", "retry"),
            ("worker_item_failed", "error", "dead_lettered"),
        ]
        assert all(r["worker_role"] == "offline-writer" for r in records)
        assert all("duration_ms" in r for r in records)
    finally:
        _teardown()


def test_routine_success_is_silent_at_default_info_but_failures_are_visible():
    """Final review C volume policy: at the default INFO level, routine per-item
    success emits NOTHING (Prometheus owns aggregates; worker_round_completed is the
    INFO summary), while retries/failures remain visible."""
    from fintech_feature_platform.api.runner_daemon import record_worker_item

    buffer = _capture("online-worker")  # default level INFO
    backend = SimpleNamespace()
    try:
        for _ in range(100):
            record_worker_item(backend, "online-worker", "ok", 0.001)
        assert buffer.getvalue() == ""  # 100 successes -> zero INFO lines
        record_worker_item(backend, "online-worker", "retry_republished", 0.001)
        record_worker_item(backend, "online-worker", "dead_lettered", 0.001)
        assert [(r["event"], r["level"]) for r in _lines(buffer)] == [
            ("worker_item_failed", "warning"),
            ("worker_item_failed", "error"),
        ]
    finally:
        _teardown()


def test_api_request_log_is_debug_on_success_error_on_5xx(monkeypatch):
    monkeypatch.setenv("FSP_LOG_LEVEL", "DEBUG")
    from fastapi.testclient import TestClient

    from fintech_feature_platform.api.app import create_app

    app = create_app()  # create_app reconfigures the fsp logger to stdout...
    buffer = io.StringIO()
    configure_logging("api", stream=buffer)  # ...rebind to the capture buffer
    try:
        TestClient(app).get("/health")
        records = [r for r in _lines(buffer) if r["event"] == "api_request_completed"]
        assert records and records[-1]["level"] == "debug"
        assert records[-1]["operation"] == "/health"
    finally:
        _teardown()


def test_uvicorn_output_adopted_into_json_envelope():
    """Final review B: uvicorn lifecycle/error records render as runtime_log JSON;
    the plaintext access logger is silenced (requests are covered by
    api_request_completed + Prometheus); errors keep their tracebacks."""
    from fintech_feature_platform.fs_core.observability.logs import (
        adopt_uvicorn_logging,
    )

    buffer = io.StringIO()
    adopt_uvicorn_logging("api", stream=buffer)
    try:
        logging.getLogger("uvicorn.error").info("Application startup complete.")
        logging.getLogger("uvicorn.access").info('127.0.0.1 - "GET /x" 200')
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            logging.getLogger("uvicorn.error").error("worker crashed", exc_info=True)
        records = _lines(buffer)
        assert [r["event"] for r in records] == ["runtime_log", "runtime_log"]
        assert records[0]["detail"] == "Application startup complete."
        assert records[0]["logger"] == "uvicorn.error"
        assert "RuntimeError: boom" in records[1]["error"]  # errors never hidden
        assert "GET /x" not in buffer.getvalue()  # access line dropped, not JSON-wrapped
    finally:
        for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
            logging.getLogger(name).handlers.clear()
            logging.getLogger(name).propagate = True


def test_uvicorn_adoption_noop_in_text_dev_mode(monkeypatch):
    monkeypatch.setenv("FSP_LOG_FORMAT", "text")
    from fintech_feature_platform.fs_core.observability.logs import (
        adopt_uvicorn_logging,
    )

    access = logging.getLogger("uvicorn.access")
    handlers_before = list(access.handlers)
    adopt_uvicorn_logging("api")
    assert list(access.handlers) == handlers_before  # dev output untouched


def test_round_log_quiet_when_idle_and_structured_when_busy(capsys):
    from fintech_feature_platform.api.runner_daemon import log_round

    buffer = _capture("online-worker")
    idle = SimpleNamespace(status="idle", committed=False)
    done = SimpleNamespace(status="ok", committed=True)
    try:
        log_round([idle, idle], "online-worker")  # all-idle -> silent
        assert buffer.getvalue() == ""
        log_round([idle, done, done], "online-worker")
        (record,) = _lines(buffer)
        assert record["event"] == "worker_round_completed"
        assert record["processed"] == 2
        assert record["committed"] == 2
        assert capsys.readouterr().out == ""  # migrated off print entirely
    finally:
        _teardown()


def test_forever_paths_migrated_but_cli_output_kept():
    """Migration map enforced at the source level: daemon rounds -> structured logs,
    bounded-run (--once/--max-messages) operator output stays a CLI print."""
    import inspect

    import fintech_feature_platform.api.batch_worker_runner as b
    import fintech_feature_platform.api.metadata_writer_runner as md
    import fintech_feature_platform.api.model_score_writer_runner as ms
    import fintech_feature_platform.api.offline_writer_runner as off
    import fintech_feature_platform.api.online_worker_runner as on
    import fintech_feature_platform.api.propagation_worker_runner as p

    for module in (on, off, md, ms, b):
        source = inspect.getsource(module)
        assert "print_round" not in source, module.__name__
        assert "log_round(results" in source, module.__name__
        assert 'print(f"processed=' in source, module.__name__  # CLI output kept
    p_source = inspect.getsource(p)
    assert "propagation_wave_completed" in p_source
    assert 'print(\n        f"observed=' in p_source or "observed=" in p_source


# --- correlation identifiers --------------------------------------------------

def test_dlq_log_carries_technical_correlation_ids():
    from fintech_feature_platform.api.dlq import route_to_dlq
    from fintech_feature_platform.fs_core.events.consumer import InMemoryMessage
    from fintech_feature_platform.fs_core.events.publisher import InMemoryEventPublisher

    buffer = _capture("online-worker")
    payload = json.dumps(
        {"request_id": "freq_abc", "job_id": "job_xyz", "correlation_id": "corr_1"}
    ).encode("utf-8")
    try:
        ok = route_to_dlq(
            publisher=InMemoryEventPublisher(),
            source_topic="fp.feature-compute.online",
            failure_stage="online_worker",
            failure_status="deserialization_failed",
            error="boom",
            message=InMemoryMessage(payload),
            attempt_count=5,
        )
        assert ok is True
        (record,) = _lines(buffer)
        assert record["event"] == "message_dead_lettered"
        assert record["level"] == "error"
        assert record["request_id"] == "freq_abc"
        assert record["job_id"] == "job_xyz"
        assert record["correlation_id"] == "corr_1"
        assert record["attempt_count"] == 5
        assert "payload" not in json.dumps(record).lower().replace("source_payload", "")
    finally:
        _teardown()


def test_retry_log_carries_request_and_correlation_ids():
    from fintech_feature_platform.api.dlq import republish_with_attempt
    from fintech_feature_platform.fs_core.events.publisher import InMemoryEventPublisher

    buffer = _capture("online-worker")
    event = SimpleNamespace(
        request_id="freq_retry", correlation_id="corr_2",
        to_json=lambda: b"{}", event_type="t", idempotency_key="k",
    )
    try:
        ok = republish_with_attempt(
            publisher=InMemoryEventPublisher(), topic="fp.feature-compute.online",
            key="k", event=event, headers={}, attempt=2,
        )
        assert ok is True
        (record,) = _lines(buffer)
        assert record["event"] == "message_retry_republished"
        assert record["level"] == "warning"
        assert record["request_id"] == "freq_retry"
        assert record["correlation_id"] == "corr_2"
        assert record["attempt"] == 2
    finally:
        _teardown()


# --- security ----------------------------------------------------------------

def test_security_disabled_warning_is_structured_and_secret_free(monkeypatch):
    from fintech_feature_platform.api.security import SecurityConfig, warn_if_disabled

    monkeypatch.setenv("FSP_SECURITY_MODE", "disabled")
    monkeypatch.setenv("FSP_ENVIRONMENT", "development")
    monkeypatch.setenv(
        "FSP_API_KEYS",
        json.dumps([{"key_id": "ops", "role": "operator", "secret": SENTINEL}]),
    )
    buffer = _capture("feature-api")
    try:
        warn_if_disabled(SecurityConfig.from_env(), "feature-api")
        (record,) = _lines(buffer)
        assert record["event"] == "security_disabled_mode"
        assert record["level"] == "warning"
        assert SENTINEL not in buffer.getvalue()
    finally:
        _teardown()


# --- volume ------------------------------------------------------------------

def test_no_per_feature_or_per_store_call_logging():
    """Per-feature/per-store detail is Prometheus's job : the compute and
    timing seams must contain NO logging calls, and a real multi-feature compute must
    emit zero log lines."""
    import inspect

    from fintech_feature_platform.fs_core.compute import engine
    from fintech_feature_platform.fs_core.observability import timing

    for module in (engine, timing):
        source = inspect.getsource(module)
        assert "log_event" not in source, module.__name__
        assert "logging" not in source, module.__name__

    # Functional: computing many features produces no log output at all.
    from datetime import UTC, datetime

    from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
    from fintech_feature_platform.fs_core.feature_store import FeatureStore
    from fintech_feature_platform.fs_core.models import EntityKey, SourceStamp
    from fintech_feature_platform.fs_core.raw.meta_repository import (
        InMemoryMetaRepository,
    )
    from fintech_feature_platform.fs_core.raw.payload_store import InMemoryPayloadStore
    from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
    from fintech_feature_platform.fs_core.registry.loader import build_registry
    from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
    from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore

    n = 50
    registry = build_registry({
        "registry_version": "logs-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {"src": {"type": "raw_report", "report_type": "r",
                            "ts_field": "report_ts"}},
        "feature_views": {"v": {"entity": "e", "key_fields": ["id"], "view_version": 1,
                                "owner": "o", "status": "active", "features": {
            f"f{i}": {"kind": "udf", "feature_version": 1, "udf": "udf.c",
                      "dtype": "int", "status": "live", "inputs": ["src"]}
            for i in range(n)
        }}},
    })
    store = FeatureStore(
        registry, UdfRegistry({"udf.c": lambda s, d: s["src"]["v"]}),
        ReportResolver(InMemoryPayloadStore(), InMemoryMetaRepository()),
        InMemoryOfflineStore(), InMemoryOnlineStore(),
    )
    ts = datetime(2026, 1, 10, tzinfo=UTC)
    buffer = _capture("online-worker")
    try:
        store.compute_write_set(
            view="v", view_version=1,
            entity_key=EntityKey.from_mapping({"id": "1"}, key_order=["id"]),
            requested_features=[f"f{i}" for i in range(n)],
            source_refs={"src": "r"},
            source_stamps={"src": SourceStamp(report_ts=ts, content_hash="sha256:s")},
            calc_ts=ts, source_loader=lambda name: {"v": 1},
        )
        assert buffer.getvalue() == ""  # zero log lines for 50 feature evaluations
    finally:
        _teardown()
