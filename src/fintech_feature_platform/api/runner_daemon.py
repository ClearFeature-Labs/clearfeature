"""Shared bits for the runners' ``--forever`` daemon mode.

The worker runners are bounded helpers by design (drain-until-idle); under Docker Compose
that would mean exit-on-idle -> container restart -> Kafka consumer-group rebalance on
every idle cycle. ``--forever`` instead loops the existing ``run()`` in bounded rounds on
the SAME consumer: no rebalance churn, flat memory (per-round results are discarded), and
the unchanged commit/DLQ discipline. Stop via SIGTERM (``docker compose stop``).
"""

from __future__ import annotations

import logging
import signal
import threading

from fintech_feature_platform.fs_core.observability.logs import get_logger, log_event

# Iterations per round. Only affects memory of the per-round results list and log
# cadence — correctness is per-message. At the default 1s poll timeout an idle round
# lasts ~FOREVER_ROUND seconds.
FOREVER_ROUND = 1000

# The propagation runner flushes its debounced recompute wave at the END of each round,
# so its round length IS the worst-case debounce window (~30s at the 1s poll timeout).
PROPAGATION_FOREVER_ROUND = 30

_logger = get_logger("worker")

# Graceful shutdown : ONE shared flag + signal seam for all six
# --forever runners. SIGTERM/SIGINT only REQUEST a stop; the run() loops check the
# flag at iteration boundaries, so the current in-flight item always finishes with
# its normal commit/DLQ semantics (commit-after-effect is never weakened), then the
# main closes the consumer exactly once. With auto-commit DISABLED,
# confluent Consumer.close() commits nothing and sends an immediate LeaveGroup, so
# partitions reassign without waiting out the session timeout. SIGKILL still cannot
# run any of this — safe replay covers that path (proven live).
_SHUTDOWN = threading.Event()


def install_shutdown_signals(worker_role: str) -> None:
    """Install SIGTERM/SIGINT -> graceful-stop handlers (daemon --forever mode only;
    bounded CLI runs keep normal Ctrl-C behavior)."""

    def _handle(signum, frame) -> None:  # noqa: ARG001 - signal handler signature
        log_event(
            _logger, logging.INFO, "worker_shutdown_requested",
            worker_role=worker_role, signal=signal.Signals(signum).name,
        )
        _SHUTDOWN.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


def shutdown_requested() -> bool:
    return _SHUTDOWN.is_set()


def reset_shutdown_for_tests() -> None:
    """Clear the process-wide flag (test isolation only)."""
    _SHUTDOWN.clear()


def close_consumer(consumer, worker_role: str) -> None:
    """Close the Kafka consumer exactly once (idempotent) and log the completion.

    A close failure is logged AND re-raised — shutdown errors are never hidden; the
    process is exiting either way, but the operator sees the truth.
    """
    if getattr(consumer, "_fsp_closed", False):
        return
    try:
        close = getattr(consumer, "close", None)
        if close is not None:
            close()
    except Exception as exc:
        log_event(
            _logger, logging.ERROR, "worker_shutdown_error",
            worker_role=worker_role, error_type=type(exc).__name__,
        )
        raise
    consumer._fsp_closed = True  # noqa: SLF001 - our own idempotence marker
    log_event(
        _logger, logging.INFO, "worker_shutdown_complete", worker_role=worker_role
    )


def log_round(results, worker_role: str) -> None:
    """Log one daemon round (structured); quiet when all-idle (keeps logs bounded).

    Replaces the ``print_round``.
    """
    done = [r for r in results if r.status != "idle"]
    if not done:
        return
    committed = sum(1 for r in done if r.committed)
    log_event(
        _logger, logging.INFO, "worker_round_completed",
        worker_role=worker_role, processed=len(done), committed=committed,
    )


# ProcessResult.status -> the CLOSED worker result domain (catalog WORKER_RESULTS).
# idle/paused/rate_limited are NOT items: nothing was consumed-and-resolved in that poll
# (paused/rate-limited offsets stay uncommitted; the item is counted when processed).
_NOT_AN_ITEM = frozenset({"idle", "paused", "rate_limited"})
_SUCCESS_STATUSES = frozenset({"ok", "observed", "projected", "written"})
_NOOP_STATUSES = frozenset({"noop", "skipped", "deadline_expired"})


def classify_result(status: str) -> str | None:
    """Map a runner ProcessResult status onto the closed worker result domain.

    Returns ``None`` when the poll produced no work item. Unknown/new statuses map to
    ``failure`` (conservative, still inside the closed domain). Semantics: ``success``
    and ``noop``/``dead_lettered``/``retry`` count each logical item exactly once at its
    terminal-for-this-attempt outcome; an infra ``failure`` leaves the offset uncommitted,
    so the SAME item is counted again on each real retry attempt (documented).
    """
    if status in _NOT_AN_ITEM:
        return None
    if status in _SUCCESS_STATUSES:
        return "success"
    if status in _NOOP_STATUSES:
        return "noop"
    if status == "dead_lettered":
        return "dead_lettered"
    if status == "retry_republished":
        return "retry"
    return "failure"


def record_worker_item(backend, worker_role: str, status: str, duration_s: float) -> None:
    """The ONE shared per-item instrumentation seam for all six workers.

    Metrics  + one structured log event per processed item :
    ``worker_item_completed`` at DEBUG for success/noop (final review C: routine
    per-item success volume — counts/latency already live in Prometheus; the INFO-level
    aggregate is ``worker_round_completed``), ``worker_item_failed`` at WARNING for
    retry and ERROR for dead_lettered/failure (failures stay visible at default INFO).
    Per-feature and per-store detail stays in Prometheus — never logged here.
    """
    result = classify_result(status)
    if result is None:
        return
    from fintech_feature_platform.fs_core.observability.metrics import recorder_of

    metrics = recorder_of(backend)
    metrics.incr("fsp_worker_items_total", {"worker_role": worker_role, "result": result})
    metrics.observe(
        "fsp_worker_processing_duration_seconds", duration_s, {"worker_role": worker_role}
    )
    if result == "success":
        import time as _time

        metrics.gauge(
            "fsp_worker_last_success_unixtime_seconds", _time.time(),
            {"worker_role": worker_role},
        )
    if result in ("success", "noop"):
        event, level = "worker_item_completed", logging.DEBUG
    elif result == "retry":
        event, level = "worker_item_failed", logging.WARNING
    else:  # dead_lettered / failure
        event, level = "worker_item_failed", logging.ERROR
    log_event(
        _logger, level, event,
        worker_role=worker_role, operation="process_item", status=status,
        result=result, duration_ms=round(duration_s * 1000, 3),
    )


def maybe_start_metrics_server(worker_role: str, backend, settings):
    """Start this process's observability server when the observability port > 0.

    The ONE shared lifecycle seam for API + all workers: sets the bounded
    ``fsp_process_info{worker_role=...}`` identity and serves the process-local registry
    on ``/metrics`` plus role-aware readiness on ``/ready`` — one deliberate port for
    both capabilities ( closure: ``FSP_OBSERVABILITY_PORT``, with
    ``FSP_METRICS_PORT`` as the retained compatibility fallback; port 0 = the process
    exposes neither endpoint; documented+tested). Returns the server (caller may keep
    it for explicit shutdown; the serving thread is a daemon, so it never blocks
    process exit) or ``None`` when disabled or when the backend's recorder has no
    Prometheus registry (e.g. lightweight test doubles).
    """
    port = getattr(settings, "observability_port", getattr(settings, "metrics_port", 0))
    if port is None or port <= 0:
        return None
    recorder = getattr(backend, "metrics", None)
    registry = getattr(recorder, "registry", None)
    if registry is None:
        return None
    # Catalog-enforced bounded identity (raises fast on a non-canonical role).
    recorder.gauge("fsp_process_info", 1, {"worker_role": worker_role})
    from fintech_feature_platform.api.readiness import build_readiness_checker
    from fintech_feature_platform.fs_core.observability.exposition import MetricsServer

    checker = build_readiness_checker(worker_role, backend)
    return MetricsServer(registry, port, ready_check=checker.check).start()
