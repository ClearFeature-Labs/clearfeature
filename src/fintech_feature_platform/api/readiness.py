"""Process readiness.

Readiness answers exactly one question:

    "Can this process currently perform its assigned role?"

It is a DEPENDENCY question, never a work-progress question: readiness NEVER consults
queue depth, last-success timestamps, throughput, or whether anything was processed.
An idle worker with an empty queue is READY. Liveness ("is the process alive?") stays
separate: the API keeps ``/health``; workers rely on process supervision.

Design:

- ``DependencyProbe``: one named, side-effect-free check over a client that was already
  built at backend-construction time (Postgres pool checkout, Valkey ping, MinIO
  bucket_exists, Kafka producer metadata). Probes are built ONCE per process in
  ``local_backend.py`` — never a fresh backend per readiness request. The memory
  backend has no external dependencies, so it carries zero probes and is always ready.
- ``ROLE_DEPENDENCIES``: the closed worker-role -> required-dependency matrix, derived
  from actual handler store usage. Best-effort stores (e.g. the writers' Valkey
  request-status updates, which may fail without failing the item) are deliberately
  NOT readiness dependencies.
- ``ReadinessChecker``: runs the role's probes with a bounded per-probe timeout (a
  daemon thread joined for ``PROBE_TIMEOUT_S`` — one hanging network call can never
  block readiness) and per-dependency SINGLE-FLIGHT: while a probe call is still
  in flight from an earlier check, later checks report ``timeout`` for it WITHOUT
  spawning another thread or another call into the dependency. Live probe threads are
  therefore bounded by the number of dependencies (<= 4), no matter how often
  orchestration polls a permanently hanging dependency; when the hung call finally
  returns, the next check probes fresh (recovery preserved). A probe failure ->
  NOT READY, never a crash. Ready/not-ready TRANSITIONS are logged
  (``readiness_failed`` / ``readiness_recovered``); steady states are not, so
  aggressive polling cannot flood logs.

Security: the machine-readable report contains only the stable dependency category and
``ok|failed|timeout`` — never exception messages, connection strings, or credentials.
Failure detail is logged as the exception CLASS name only.

READINESS DEADLINE vs DATA-PLANE TIMEOUT. Two distinct concepts that must never be conflated:

- READINESS DEADLINE: a SMALL, explicit operational bound on the capability check —
  a probe must terminate within a few seconds, because with single-flight an
  in-flight probe blocks the next one, so the probe's own deadline IS the recovery
  bound after a dependency comes back. "Finite eventually" (300 s SDK budgets,
  OS-scale TCP behavior) is not a readiness deadline.
- DATA-PLANE TIMEOUT: the timeout/retry policy of real production work (payload
  transfers, queries). It is deliberately DIFFERENT and is never shortened to
  satisfy readiness.

Enforced deadlines (probe-only clients built ONCE at backend construction — see
``local_backend.py``; evidence: the release qualification records):
kafka ``list_topics(timeout=2)`` — 2 s by librdkafka on the existing producer;
valkey — dedicated probe client, connect/op 2 s each, NO retries (<=4 s); minio —
dedicated probe client, urllib3 Timeout(connect=2, read=2), retries disabled
(<=4 s; the data-plane 300 s x5 policy is untouched); postgres — dedicated
``max_size=1``/``min_size=0`` readiness pool: checkout 2 s (PoolTimeout),
connect_timeout 2 s, statement_timeout 2 s, short TCP keepalives (~3 s) +
tcp_user_timeout 2.5 s for network stalls (<=~5 s). ``_ProbeRunner`` (single-flight
+ the 2 s join) is concurrency and HTTP-latency protection; the probe client's own
deadline provides actual termination and therefore the recovery bound.
Config-pinning tests guard both the probe deadlines and the unchanged data-plane
policies.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass

from fintech_feature_platform.fs_core.observability.catalog import WORKER_ROLES
from fintech_feature_platform.fs_core.observability.logs import get_logger, log_event

# Bounded per-probe execution time. Probes use existing clients whose own timeouts may
# be long (pool checkout, socket connect) — the join bound wins regardless.
PROBE_TIMEOUT_S = 2.0

# The closed dependency vocabulary (stable categories, safe to expose).
DEP_KAFKA = "kafka"
DEP_POSTGRES = "postgres"
DEP_MINIO = "minio"
DEP_VALKEY = "valkey"

# worker_role -> required dependencies, derived from handler/runner store usage:
#   payloads -> minio; metas/offline/metadata/batch_meta/source_datasets -> postgres;
#   online/status/results/batch_status -> valkey; events -> kafka.
# Best-effort request-status updates (offline/metadata writers) are excluded on purpose.
# The role KEY domain is the ONE canonical catalog.WORKER_ROLES set — enforced at
# import time below so the two contracts can never silently drift.
ROLE_DEPENDENCIES: dict[str, frozenset[str]] = {
    "api": frozenset({DEP_KAFKA, DEP_POSTGRES, DEP_MINIO, DEP_VALKEY}),
    "online-worker": frozenset({DEP_KAFKA, DEP_MINIO, DEP_VALKEY}),
    "offline-writer": frozenset({DEP_KAFKA, DEP_POSTGRES}),
    "metadata-writer": frozenset({DEP_KAFKA, DEP_POSTGRES}),
    "model-score-writer": frozenset({DEP_KAFKA, DEP_VALKEY}),
    "batch-worker": frozenset({DEP_KAFKA, DEP_POSTGRES, DEP_MINIO, DEP_VALKEY}),
    "propagation-worker": frozenset({DEP_KAFKA, DEP_POSTGRES}),
}

if set(ROLE_DEPENDENCIES) != set(WORKER_ROLES):  # pragma: no cover - drift guard
    raise RuntimeError(
        "readiness ROLE_DEPENDENCIES must cover exactly the canonical "
        f"catalog.WORKER_ROLES; missing={set(WORKER_ROLES) - set(ROLE_DEPENDENCIES)} "
        f"extra={set(ROLE_DEPENDENCIES) - set(WORKER_ROLES)}"
    )

_logger = get_logger("readiness")


@dataclass(frozen=True)
class DependencyProbe:
    """One named readiness check; ``check`` raises on failure, returns None on success.

    CONTRACT: ``check`` must only perform I/O that the CLIENT itself bounds in finite
    time (see the module TIMEOUT INVARIANT). A never-returning callable violates this
    contract — the single-flight runner tolerates it without thread growth, but that
    probe would stay ``timeout`` forever.
    """

    name: str  # stable category from the DEP_* vocabulary
    check: Callable[[], None]


@dataclass(frozen=True)
class ReadinessReport:
    ready: bool
    role: str
    checks: dict[str, str]  # dependency category -> ok | failed | timeout

    def to_dict(self) -> dict:
        return {
            "status": "ready" if self.ready else "not_ready",
            "role": self.role,
            "checks": dict(self.checks),
        }


class _ProbeRunner:
    """One probe with a hard time bound AND single-flight concurrency (never raises).

    ``join(timeout)`` cannot terminate a hanging thread, so the runner keeps the
    in-flight thread and refuses to start another until it finishes: repeated checks
    against a permanently hanging dependency report ``timeout`` instantly, hold exactly
    ONE live thread and exactly ONE outstanding dependency call. No retries, no fresh
    clients — the probe closes over the client built at backend construction.
    """

    def __init__(self, probe: DependencyProbe, timeout_s: float) -> None:
        self._probe = probe
        self._timeout_s = timeout_s
        self._thread: threading.Thread | None = None
        self._outcome: dict[str, str] = {}
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return self._probe.name

    def run(self) -> str:
        """Returns ok|failed|timeout. ``timeout`` also covers "previous call still
        hanging" — the dependency is equally not-answering in both cases."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return "timeout"  # single-flight: no new thread, no second call
            outcome: dict[str, str] = {}
            check = self._probe.check

            def _target() -> None:
                try:
                    check()
                    outcome["status"] = "ok"
                except Exception as exc:  # noqa: BLE001 - must not crash the process
                    outcome["status"] = "failed"
                    outcome["error_type"] = type(exc).__name__
                # Never the exception message: errors can embed endpoints/DSNs.

            thread = threading.Thread(
                target=_target, name=f"fsp-ready-{self._probe.name}", daemon=True
            )
            self._thread = thread
            self._outcome = outcome
        thread.start()
        thread.join(self._timeout_s)
        if thread.is_alive():
            return "timeout"
        if outcome.get("status") == "failed":
            log_event(
                _logger, logging.DEBUG, "readiness_probe_failed",
                dependency=self._probe.name, error_type=outcome.get("error_type"),
            )
        return outcome.get("status", "failed")


class ReadinessChecker:
    """Role-aware readiness over the process's prebuilt dependency probes.

    Selects the probes required by ``role`` (per ``ROLE_DEPENDENCIES``); probes the
    backend built but the role does not need are ignored. Logs only transitions.
    """

    def __init__(
        self,
        role: str,
        probes: tuple[DependencyProbe, ...],
        *,
        timeout_s: float = PROBE_TIMEOUT_S,
    ) -> None:
        if role not in ROLE_DEPENDENCIES:
            raise ValueError(f"unknown readiness role {role!r}")
        required = ROLE_DEPENDENCIES[role]
        self.role = role
        # One stateful runner per dependency (single-flight lives here, for the
        # lifetime of the process — probes and runners are built once).
        self._runners = tuple(
            _ProbeRunner(p, timeout_s) for p in probes if p.name in required
        )
        self._last_ready: bool | None = None
        self._lock = threading.Lock()

    def check(self) -> ReadinessReport:
        checks = {runner.name: runner.run() for runner in self._runners}
        ready = all(status == "ok" for status in checks.values())
        report = ReadinessReport(ready=ready, role=self.role, checks=checks)
        self._log_transition(report)
        return report

    def _log_transition(self, report: ReadinessReport) -> None:
        with self._lock:
            previous, self._last_ready = self._last_ready, report.ready
        if report.ready and previous is False:
            log_event(
                _logger, logging.INFO, "readiness_recovered",
                worker_role=report.role,
            )
        elif not report.ready and previous is not False:
            failed = sorted(
                name for name, status in report.checks.items() if status != "ok"
            )
            log_event(
                _logger, logging.WARNING, "readiness_failed",
                worker_role=report.role, failed_dependencies=failed,
            )


def build_readiness_checker(role: str, backend) -> ReadinessChecker:
    """The one construction seam: role matrix x the backend's prebuilt probes.

    A backend without ``readiness_probes`` (lightweight test doubles) or with an empty
    tuple (memory backend: no external dependencies) yields an always-ready checker.
    """
    probes = getattr(backend, "readiness_probes", ()) or ()
    return ReadinessChecker(role, tuple(probes))
