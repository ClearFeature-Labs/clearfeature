"""Readiness semantics.

Readiness = "can this process perform its role?" — a dependency question. These tests
prove: role-aware probe selection, dependency-failure -> NOT READY without a crash,
bounded probe execution, no work-progress coupling (idle/empty-queue/never-processed is
READY), no secret leakage, the bounded response schema, and the shared server
lifecycle (/ready riding the observability port).
"""

import dataclasses
import io
import json
import time
import urllib.error
import urllib.request

import pytest

from fintech_feature_platform.api.readiness import (
    DEP_KAFKA,
    DEP_MINIO,
    DEP_POSTGRES,
    DEP_VALKEY,
    ROLE_DEPENDENCIES,
    DependencyProbe,
    ReadinessChecker,
    build_readiness_checker,
)

# Synthetic secret sentinel: must never appear in any readiness output.
SENTINEL = "sentinel-Bearer-P4ssw0rd-hunter2-DO-NOT-LEAK"


def _ok() -> None:
    return None


def _fail() -> None:
    raise RuntimeError(f"connection refused: password={SENTINEL} dsn=postgresql://u:{SENTINEL}@h/db")


def _probes(**status):
    return tuple(
        DependencyProbe(name, _ok if healthy else _fail)
        for name, healthy in status.items()
    )


# --- checker semantics -------------------------------------------------------

def test_role_dependency_matrix_is_the_documented_one():
    assert ROLE_DEPENDENCIES == {
        "api": frozenset({DEP_KAFKA, DEP_POSTGRES, DEP_MINIO, DEP_VALKEY}),
        "online-worker": frozenset({DEP_KAFKA, DEP_MINIO, DEP_VALKEY}),
        "offline-writer": frozenset({DEP_KAFKA, DEP_POSTGRES}),
        "metadata-writer": frozenset({DEP_KAFKA, DEP_POSTGRES}),
        "model-score-writer": frozenset({DEP_KAFKA, DEP_VALKEY}),
        "batch-worker": frozenset({DEP_KAFKA, DEP_POSTGRES, DEP_MINIO, DEP_VALKEY}),
        "propagation-worker": frozenset({DEP_KAFKA, DEP_POSTGRES}),
    }


def test_healthy_worker_is_ready():
    checker = ReadinessChecker(
        "online-worker", _probes(kafka=True, minio=True, valkey=True)
    )
    report = checker.check()
    assert report.ready is True
    assert report.to_dict() == {
        "status": "ready", "role": "online-worker",
        "checks": {"kafka": "ok", "minio": "ok", "valkey": "ok"},
    }


def test_missing_required_dependency_is_not_ready_without_crash():
    checker = ReadinessChecker("offline-writer", _probes(kafka=True, postgres=False))
    report = checker.check()  # must not raise
    assert report.ready is False
    assert report.checks == {"kafka": "ok", "postgres": "failed"}


def test_role_ignores_irrelevant_dependencies():
    # A broken MinIO must NOT make the offline-writer unready (not in its matrix).
    checker = ReadinessChecker(
        "offline-writer", _probes(kafka=True, postgres=True, minio=False)
    )
    report = checker.check()
    assert report.ready is True
    assert "minio" not in report.checks


def test_readiness_never_depends_on_work_progress():
    # An idle process that has processed NOTHING (no items, no last-success, empty
    # queue) is ready as long as its dependencies are: probes are the ONLY input.
    checker = ReadinessChecker("online-worker", _probes(kafka=True, minio=True, valkey=True))
    assert checker.check().ready is True
    # And structurally: the check CODE never consults metrics/progress state (the
    # module docstring mentions those concepts to forbid them, so inspect code only).
    import inspect

    from fintech_feature_platform.api.readiness import _ProbeRunner

    code = inspect.getsource(ReadinessChecker.check) + inspect.getsource(_ProbeRunner.run)
    for forbidden in ("last_success", "snapshot", "items_total", "queue", "lag"):
        assert forbidden not in code, forbidden


def test_probe_timeout_is_bounded():
    def _hang() -> None:
        time.sleep(5)

    checker = ReadinessChecker(
        "offline-writer",
        (DependencyProbe("kafka", _hang), DependencyProbe("postgres", _ok)),
        timeout_s=0.2,
    )
    started = time.monotonic()
    report = checker.check()
    assert time.monotonic() - started < 2.0  # one hanging call cannot block readiness
    assert report.ready is False
    assert report.checks["kafka"] == "timeout"


def test_repeated_hanging_probes_hold_bounded_threads_and_single_flight():
    """join(timeout) never kills the thread, so the runner is
    single-flight — a permanently hanging dependency holds exactly ONE live probe
    thread and ONE outstanding call no matter how often /ready is polled."""
    import threading

    gate = threading.Event()  # released in the finally block
    calls = {"count": 0}

    def hanging() -> None:
        calls["count"] += 1
        gate.wait()

    checker = ReadinessChecker(
        "online-worker",
        (DependencyProbe("kafka", hanging), DependencyProbe("valkey", _ok)),
        timeout_s=0.05,
    )
    before = threading.active_count()
    try:
        durations = []
        for _ in range(50):
            started = time.perf_counter()
            report = checker.check()
            durations.append(time.perf_counter() - started)
            assert report.ready is False
            assert report.checks["kafka"] == "timeout"
            assert threading.active_count() <= before + 1  # the ONE in-flight thread
        assert calls["count"] == 1  # single-flight: one call into the dependency
        # After the first (paid) timeout, in-flight checks answer instantly.
        assert min(durations[1:]) < 0.02
    finally:
        gate.set()


_TIMEOUT_AUDIT_SCRIPT = """
import inspect

# --- READINESS DEADLINES: probe-only clients, a few seconds, no retries -------
from fintech_feature_platform.fs_core.stores.valkey_online import (
    connect_valkey,
    connect_valkey_probe,
)

probe_valkey = connect_valkey_probe("localhost", 6399, db=0)  # config only
kwargs = probe_valkey.connection_pool.connection_kwargs
assert 0 < kwargs["socket_timeout"] <= 5          # short op bound
assert 0 < kwargs["socket_connect_timeout"] <= 5  # short connect bound
assert probe_valkey.get_retry().get_retries() == 0  # NO readiness retries

from fintech_feature_platform.fs_core.raw.minio_payload_store import (
    connect_minio,
    connect_minio_probe,
)

probe_minio = connect_minio_probe("localhost:9999", "k", "s")  # config only
probe_kw = probe_minio._http.connection_pool_kw
assert 0 < probe_kw["timeout"].connect_timeout <= 5  # NOT the 300s data-plane bound
assert 0 < probe_kw["timeout"].read_timeout <= 5
assert probe_kw["retries"].total in (False, 0)  # NO readiness retries

from fintech_feature_platform.api.local_backend import (
    _build_connection_provider,
    _build_readiness_pg_pool,
)
from fintech_feature_platform.api.settings import Settings

settings = Settings(postgres_dsn="postgresql://u:x@127.0.0.1:9/nodb", db_pool_size=1)
ready_pool = _build_readiness_pg_pool(settings)
try:
    assert ready_pool.timeout <= 5  # short checkout bound (PoolTimeout)
    assert ready_pool.max_size == 1
    ready_kwargs = ready_pool.kwargs
    assert ready_kwargs["connect_timeout"] <= 5
    assert "statement_timeout" in ready_kwargs["options"]
    assert ready_kwargs["keepalives"] == 1  # short keepalives bound silent stalls
    assert ready_kwargs["keepalives_interval"] <= 5
finally:
    ready_pool.close()
ready_pool.close()  # closure audit: closing an already-closed readiness pool is safe

from fintech_feature_platform.fs_core.events.publisher import KafkaEventPublisher

# Kafka: the probe passes an explicit short timeout enforced by librdkafka.
assert inspect.signature(KafkaEventPublisher.ping).parameters["timeout_s"].default == 2.0
assert "timeout=timeout_s" in inspect.getsource(KafkaEventPublisher.ping)

# --- DATA-PLANE SEPARATION: business clients keep their own intended policy ---
business_valkey = connect_valkey("localhost", 6399, db=0)
bkw = business_valkey.connection_pool.connection_kwargs
assert bkw["socket_timeout"] not in (None, 0)  # finite (redis-py 8 defaults)...
assert bkw["socket_keepalive"]
assert business_valkey.get_retry().get_retries() == 10  # ...with its own retry policy

business_minio = connect_minio("localhost:9999", "k", "s")
bpk = business_minio._http.connection_pool_kw
assert bpk["timeout"].connect_timeout == 300  # data-plane budget untouched
assert bpk["retries"].total == 5

business_pool = _build_connection_provider(settings)
try:
    assert business_pool.timeout == 30.0  # business checkout bound untouched
finally:
    business_pool.close()
# No client construction inside the readiness module: probes close over clients
# built once at backend construction (never per /ready request).
import fintech_feature_platform.api.readiness as readiness_module

readiness_source = inspect.getsource(readiness_module)
for constructor in ("connect_valkey", "connect_minio", "connect_postgres",
                    "ConnectionPool(", "Minio(", "Redis("):
    assert constructor not in readiness_source, constructor
print("TIMEOUT-AUDIT-OK")
"""

_SILENT_STALL_SCRIPT = """
import socket, time

# An accepted-but-never-answering listener: connects succeed (SYN queue), responses
# never come — the ACKed-silent stall that only client-side deadlines can bound.
listener = socket.socket()
listener.bind(("127.0.0.1", 0))
listener.listen(5)
port = listener.getsockname()[1]

from fintech_feature_platform.fs_core.stores.valkey_online import connect_valkey_probe

t0 = time.perf_counter()
try:
    connect_valkey_probe("127.0.0.1", port).ping()
    raise SystemExit("valkey probe unexpectedly succeeded")
except Exception:
    elapsed = time.perf_counter() - t0
assert elapsed < 8, f"valkey probe took {elapsed:.1f}s (deadline breached)"
print(f"valkey_silent_stall_s={elapsed:.2f}")

from fintech_feature_platform.fs_core.raw.minio_payload_store import connect_minio_probe

t0 = time.perf_counter()
try:
    connect_minio_probe(f"127.0.0.1:{port}", "k", "s").bucket_exists("b")
    raise SystemExit("minio probe unexpectedly succeeded")
except Exception:
    elapsed = time.perf_counter() - t0
assert elapsed < 8, f"minio probe took {elapsed:.1f}s (NOT the 300s data-plane bound)"
print(f"minio_silent_stall_s={elapsed:.2f}")

from fintech_feature_platform.api.local_backend import _build_readiness_pg_pool
from fintech_feature_platform.api.settings import Settings

pool = _build_readiness_pg_pool(
    Settings(postgres_dsn=f"postgresql://u:x@127.0.0.1:{port}/db")
)
t0 = time.perf_counter()
try:
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    raise SystemExit("postgres probe unexpectedly succeeded")
except Exception:
    elapsed = time.perf_counter() - t0
finally:
    pool.close()
assert elapsed < 8, f"postgres probe took {elapsed:.1f}s (deadline breached)"
print(f"postgres_silent_stall_s={elapsed:.2f}")
listener.close()
print("SILENT-STALL-OK")
"""


def _run_isolated_script(script: str, *required: str) -> str:
    import importlib.util
    import subprocess
    import sys

    for package in required:
        if importlib.util.find_spec(package) is None:  # locates only; imports nothing
            pytest.skip(f"optional package {package} not installed")
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_probe_clients_fail_fast_against_silent_dependency():
    """Recovery-bound proof at the client layer with REAL network I/O: an
    accepted-but-never-answering socket must fail within the few-second readiness
    deadline for every probe client — never the 300s/OS-scale data-plane budgets.
    With single-flight, this client deadline IS the readiness recovery bound."""
    stdout = _run_isolated_script(
        _SILENT_STALL_SCRIPT, "redis", "minio", "psycopg_pool"
    )
    assert "SILENT-STALL-OK" in stdout


def test_real_dependency_clients_have_finite_io_timeouts():
    """Timeout-sanity invariant : every REAL probe operation is bounded
    by the CLIENT itself — _ProbeRunner's 2s join is latency/concurrency protection,
    not the termination guarantee. Pins the effective constructed config so a client
    downgrade (e.g. redis-py <8 defaulted socket timeouts to None) fails loudly
    instead of silently reintroducing an infinite probe. Runs in an ISOLATED
    interpreter so the optional SDK imports never leak into this process (the default
    suite's lazy-import guard tests assert redis/psycopg/minio stay unimported).
    """
    stdout = _run_isolated_script(_TIMEOUT_AUDIT_SCRIPT, "redis", "minio", "psycopg_pool")
    assert "TIMEOUT-AUDIT-OK" in stdout


def test_recovery_after_hung_probe_finally_returns():
    """When the hung call eventually completes, the next check probes fresh -> READY."""
    import threading

    gate = threading.Event()

    def slow(gate=gate) -> None:
        gate.wait()

    checker = ReadinessChecker(
        "offline-writer", (DependencyProbe("kafka", slow),), timeout_s=0.05
    )
    assert checker.check().checks["kafka"] == "timeout"
    assert checker.check().checks["kafka"] == "timeout"  # still ONE hung call
    gate.set()
    time.sleep(0.1)  # let the in-flight thread finish
    report = checker.check()
    assert report.ready is True
    assert report.checks["kafka"] == "ok"


def test_report_schema_is_bounded_and_leaks_no_secret():
    checker = ReadinessChecker("model-score-writer", _probes(kafka=False, valkey=True))
    payload = checker.check().to_dict()
    assert set(payload) == {"status", "role", "checks"}
    assert set(payload["checks"].values()) <= {"ok", "failed", "timeout"}
    assert SENTINEL not in json.dumps(payload)


def test_unknown_role_is_rejected():
    with pytest.raises(ValueError, match="unknown readiness role"):
        ReadinessChecker("mystery-role", ())


def test_backend_without_probes_is_always_ready():
    class Bare:  # lightweight test double: no readiness_probes attribute
        pass

    assert build_readiness_checker("api", Bare()).check().ready is True


def test_readiness_transitions_are_logged_only_on_change():
    import logging

    from fintech_feature_platform.fs_core.observability.logs import configure_logging

    buffer = io.StringIO()
    configure_logging("test-readiness", stream=buffer)
    healthy = {"value": False}

    def flappy() -> None:
        if not healthy["value"]:
            raise RuntimeError("down")

    checker = ReadinessChecker("offline-writer", (DependencyProbe("kafka", flappy),))
    checker.check()  # -> not ready (transition: logged)
    checker.check()  # steady not-ready (no log)
    healthy["value"] = True
    checker.check()  # -> ready (recovery: logged)
    checker.check()  # steady ready (no log)

    events = [json.loads(line)["event"] for line in buffer.getvalue().splitlines()]
    assert events == ["readiness_failed", "readiness_recovered"]
    logging.getLogger("fsp").handlers.clear()


# --- API /ready --------------------------------------------------------------

def test_api_ready_healthy_returns_200():
    from fastapi.testclient import TestClient

    from fintech_feature_platform.api.app import create_app

    client = TestClient(create_app())  # memory backend: no external dependencies
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready", "role": "api", "checks": {}}


def test_api_ready_failed_dependency_returns_503_without_secrets():
    from fastapi.testclient import TestClient

    from fintech_feature_platform.api.app import create_app
    from fintech_feature_platform.api.backend import build_memory_backend

    backend = dataclasses.replace(
        build_memory_backend(), readiness_probes=_probes(valkey=False, kafka=True)
    )
    client = TestClient(create_app(backend=backend))
    response = client.get("/ready")
    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "not_ready"
    assert payload["checks"] == {"kafka": "ok", "valkey": "failed"}
    assert SENTINEL not in response.text


def test_api_health_liveness_contract_unchanged():
    from fastapi.testclient import TestClient

    from fintech_feature_platform.api.app import create_app

    assert TestClient(create_app()).get("/health").json() == {"status": "ok"}


# --- worker observability server ---------------------------------------------

def test_worker_ready_endpoint_lifecycle_and_recovery():
    from prometheus_client import CollectorRegistry

    from fintech_feature_platform.fs_core.observability.exposition import MetricsServer

    healthy = {"value": True}

    def probe() -> None:
        if not healthy["value"]:
            raise RuntimeError(f"broker down {SENTINEL}")

    checker = ReadinessChecker("offline-writer", (DependencyProbe("kafka", probe),))
    server = MetricsServer(CollectorRegistry(), 0, ready_check=checker.check).start()
    url = f"http://127.0.0.1:{server.port}/ready"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:  # healthy + idle -> READY
            assert resp.status == 200
            assert json.loads(resp.read())["status"] == "ready"

        healthy["value"] = False  # break the dependency -> NOT READY, no crash
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            urllib.request.urlopen(url, timeout=5)
        assert excinfo.value.code == 503
        body = excinfo.value.read().decode("utf-8")
        assert json.loads(body)["checks"] == {"kafka": "failed"}
        assert SENTINEL not in body

        healthy["value"] = True  # restore -> readiness recovers
        with urllib.request.urlopen(url, timeout=5) as resp:
            assert resp.status == 200
    finally:
        server.close()
    with pytest.raises(urllib.error.URLError):
        urllib.request.urlopen(url, timeout=1)  # socket released deterministically


def test_metrics_only_server_returns_404_for_ready():
    from prometheus_client import CollectorRegistry

    from fintech_feature_platform.fs_core.observability.exposition import MetricsServer

    server = MetricsServer(CollectorRegistry(), 0).start()
    try:
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            urllib.request.urlopen(f"http://127.0.0.1:{server.port}/ready", timeout=5)
        assert excinfo.value.code == 404
    finally:
        server.close()
    server.close()  # closure audit: double-close of the observability server is safe


def test_disabled_observability_port_disables_worker_readiness_endpoint():
    # The documented closure contract: observability port 0 (FSP_OBSERVABILITY_PORT,
    # legacy fallback FSP_METRICS_PORT) -> no process observability server at all,
    # so workers expose neither /metrics nor /ready.
    from fintech_feature_platform.api.backend import build_memory_backend
    from fintech_feature_platform.api.runner_daemon import maybe_start_metrics_server
    from fintech_feature_platform.api.settings import Settings

    assert (
        maybe_start_metrics_server("online-worker", build_memory_backend(), Settings())
        is None
    )


def test_worker_shared_port_serves_metrics_and_readiness():
    # The ONE shared lifecycle helper serves BOTH endpoints on the one port.
    import socket

    from fintech_feature_platform.api.backend import build_memory_backend
    from fintech_feature_platform.api.runner_daemon import maybe_start_metrics_server
    from fintech_feature_platform.api.settings import Settings

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = maybe_start_metrics_server(
        "online-worker", build_memory_backend(), Settings(observability_port=port)
    )
    assert server is not None
    try:
        base = f"http://127.0.0.1:{server.port}"
        with urllib.request.urlopen(f"{base}/ready", timeout=5) as resp:
            ready = json.loads(resp.read())
        assert ready == {"status": "ready", "role": "online-worker", "checks": {}}
        with urllib.request.urlopen(f"{base}/metrics", timeout=5) as resp:
            body = resp.read().decode("utf-8")
        assert 'fsp_process_info{worker_role="online-worker"} 1.0' in body
    finally:
        server.close()
