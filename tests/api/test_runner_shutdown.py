"""Graceful Kafka consumer shutdown.

SIGTERM/SIGINT request a graceful stop through ONE shared seam (`runner_daemon`):
run() loops stop at an iteration boundary (the in-flight item always finishes with its
normal commit/DLQ semantics), the main closes the consumer exactly once, and the
process exits cleanly. Auto-commit stays disabled, so close() commits nothing —
uncommitted work remains replayable (SIGKILL path proven live in the smoke).
"""

import inspect
import json
import signal
import subprocess
import sys
import textwrap
from types import SimpleNamespace

import pytest

from fintech_feature_platform.api import online_worker_runner
from fintech_feature_platform.api.runner_daemon import (
    close_consumer,
    install_shutdown_signals,
    reset_shutdown_for_tests,
    shutdown_requested,
)


@pytest.fixture(autouse=True)
def _clean_shutdown_state():
    reset_shutdown_for_tests()
    old_term = signal.getsignal(signal.SIGTERM)
    old_int = signal.getsignal(signal.SIGINT)
    yield
    reset_shutdown_for_tests()
    signal.signal(signal.SIGTERM, old_term)
    signal.signal(signal.SIGINT, old_int)


class _CountingConsumer:
    def __init__(self) -> None:
        self.polls = 0
        self.closes = 0

    def poll(self, timeout_s):
        self.polls += 1
        return None

    def commit(self, message):  # pragma: no cover - not reached in these tests
        raise AssertionError("no commits expected")

    def close(self):
        self.closes += 1


def test_sigterm_sets_the_shared_shutdown_flag():
    install_shutdown_signals("online-worker")
    assert shutdown_requested() is False
    signal.raise_signal(signal.SIGTERM)
    assert shutdown_requested() is True


def test_sigint_also_requests_graceful_stop():
    install_shutdown_signals("offline-writer")
    signal.raise_signal(signal.SIGINT)
    assert shutdown_requested() is True


def test_run_loop_does_not_poll_after_shutdown_requested():
    install_shutdown_signals("online-worker")
    signal.raise_signal(signal.SIGTERM)
    consumer = _CountingConsumer()
    results = online_worker_runner.run(consumer, SimpleNamespace(), max_messages=100)
    assert results == []  # loop exited at the boundary check
    assert consumer.polls == 0  # no polling after shutdown begins


def test_in_flight_item_completes_before_stop(monkeypatch):
    """The item being processed when the signal lands finishes with its normal
    semantics (commit-after-effect untouched); only the NEXT poll is skipped."""
    calls = {"n": 0}

    def fake_process_next(consumer, backend, *, poll_timeout_s, max_attempts):
        calls["n"] += 1
        if calls["n"] == 3:
            signal.raise_signal(signal.SIGTERM)  # arrives DURING the 3rd item
        return online_worker_runner.ProcessResult(status="ok", committed=True)

    monkeypatch.setattr(online_worker_runner, "process_next", fake_process_next)
    install_shutdown_signals("online-worker")
    results = online_worker_runner.run(
        _CountingConsumer(), SimpleNamespace(), max_messages=100
    )
    assert len(results) == 3  # the in-flight 3rd item completed and was recorded
    assert all(r.committed for r in results)


def test_close_consumer_is_idempotent_and_logged():
    consumer = _CountingConsumer()
    close_consumer(consumer, "online-worker")
    close_consumer(consumer, "online-worker")  # second call is a no-op
    assert consumer.closes == 1


def test_close_consumer_errors_are_not_hidden():
    class Exploding:
        def close(self):
            raise RuntimeError("close failed")

    with pytest.raises(RuntimeError, match="close failed"):
        close_consumer(Exploding(), "online-worker")


def test_bounded_cli_mode_does_not_install_handlers():
    """Only --forever installs handlers: bounded runs keep normal Ctrl-C behavior."""
    source = inspect.getsource(online_worker_runner._main)
    body = source.split("if args.forever:")[1].split("max_messages = 1")[0]
    assert "install_shutdown_signals" in body  # inside the forever branch...
    before = source.split("if args.forever:")[0]
    assert "install_shutdown_signals" not in before  # ...and nowhere earlier


def test_all_six_runners_use_the_shared_lifecycle():
    """No copied shutdown logic: every runner wires the ONE shared seam."""
    import fintech_feature_platform.api.batch_worker_runner as b
    import fintech_feature_platform.api.metadata_writer_runner as md
    import fintech_feature_platform.api.model_score_writer_runner as ms
    import fintech_feature_platform.api.offline_writer_runner as off
    import fintech_feature_platform.api.online_worker_runner as on
    import fintech_feature_platform.api.propagation_worker_runner as p

    for module in (on, off, md, ms, b, p):
        source = inspect.getsource(module)
        assert "install_shutdown_signals(" in source, module.__name__
        assert "while not shutdown_requested():" in source, module.__name__
        assert "close_consumer(consumer" in source, module.__name__
        assert "signal.signal" not in source, module.__name__  # no copied handlers


def test_subprocess_sigterm_exits_cleanly_and_closes_once():
    """Real signal semantics: a daemon-style loop receives SIGTERM, stops polling,
    closes its consumer exactly once, and exits 0 within the grace period."""
    script = textwrap.dedent("""
        import json, sys, time
        from fintech_feature_platform.fs_core.observability.logs import configure_logging
        from fintech_feature_platform.api.runner_daemon import (
            close_consumer, install_shutdown_signals, shutdown_requested,
        )

        configure_logging("online-worker")

        class Consumer:
            closes = 0
            def close(self):
                Consumer.closes += 1

        consumer = Consumer()
        install_shutdown_signals("online-worker")
        print("READY", flush=True)
        while not shutdown_requested():
            time.sleep(0.05)  # stands in for the poll loop
        close_consumer(consumer, "online-worker")
        close_consumer(consumer, "online-worker")
        print(json.dumps({"closes": Consumer.closes}), flush=True)
    """)
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    assert proc.stdout.readline().strip() == "READY"
    proc.send_signal(signal.SIGTERM)
    out, err = proc.communicate(timeout=15)  # shutdown must not hang
    assert proc.returncode == 0, err
    lines = [line for line in out.splitlines() if line.strip()]
    assert json.loads(lines[-1]) == {"closes": 1}
    events = [json.loads(line)["event"] for line in lines[:-1] if line.startswith("{")]
    assert "worker_shutdown_requested" in events
    assert "worker_shutdown_complete" in events
