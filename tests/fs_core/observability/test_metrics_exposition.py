"""Per-process /metrics exposition server + lifecycle.

One tiny internal HTTP server per process, serving the process-local registry in
Prometheus text format; disabled mode starts nothing; shutdown is clean; the shared
lifecycle helper exposes fsp_process_info for every canonical role; exposition never
contains business/customer identifiers (sentinel regression).
"""

import urllib.request
from dataclasses import replace

import pytest

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.runner_daemon import maybe_start_metrics_server
from fintech_feature_platform.api.settings import Settings
from fintech_feature_platform.fs_core.observability.catalog import WORKER_ROLES
from fintech_feature_platform.fs_core.observability.exposition import MetricsServer
from fintech_feature_platform.fs_core.observability.prometheus_recorder import (
    PrometheusMetricsRecorder,
)


def _scrape(port: int) -> tuple[str, str]:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
        return resp.read().decode("utf-8"), resp.headers.get("Content-Type", "")


def test_metrics_server_serves_prometheus_text():
    recorder = PrometheusMetricsRecorder()
    recorder.incr("online_requests_total", {"outcome": "ok"})
    server = MetricsServer(recorder.registry, port=0)  # ephemeral port (tests)
    server.start()
    try:
        body, content_type = _scrape(server.port)
        assert 'online_requests_total{outcome="ok"} 1.0' in body
        assert content_type.startswith("text/plain")
    finally:
        server.close()


def test_shutdown_releases_the_server():
    recorder = PrometheusMetricsRecorder()
    server = MetricsServer(recorder.registry, port=0)
    server.start()
    port = server.port
    server.close()
    with pytest.raises(OSError):
        _scrape(port)  # connection refused after close


@pytest.mark.parametrize("role", WORKER_ROLES)
def test_shared_lifecycle_exposes_process_identity_per_role(role):
    backend = build_memory_backend()
    settings = replace(Settings(), observability_port=0)  # helper needs a real port:
    server = maybe_start_metrics_server(role, backend, replace(settings, observability_port=-1))
    assert server is None  # sanity: invalid/disabled -> no server
    # now with an ephemeral test port
    probe = MetricsServer(backend.metrics.registry, port=0)  # find a free port cleanly
    probe.start()
    free_port = probe.port
    probe.close()
    server = maybe_start_metrics_server(
        role, backend, replace(settings, observability_port=free_port)
    )
    try:
        assert server is not None
        body, _ = _scrape(server.port)
        assert f'fsp_process_info{{worker_role="{role}"}} 1.0' in body
    finally:
        server.close()


def test_disabled_mode_starts_no_server():
    backend = build_memory_backend()
    settings = Settings()  # observability_port default 0 = disabled
    assert settings.observability_port == 0
    assert maybe_start_metrics_server("api", backend, settings) is None


def test_backend_without_prometheus_recorder_is_safe():
    class Bare:
        metrics = None

    assert maybe_start_metrics_server(
        "api", Bare(), replace(Settings(), observability_port=1)
    ) is None


# --- security sentinel regression (does not replace cardinality checks) -----

def test_exposition_contains_no_business_identifiers():
    sentinels = (
        "debt_to_income_ratio",   # feature name
        "customer_id",            # entity key field
        "user_id",                # user identifier
        "sha256:",                # bundle/artifact digest
        "Bearer ",                # API key material
        "fsp_dev_password",       # credential
    )
    recorder = PrometheusMetricsRecorder()
    recorder.incr("online_requests_total", {"outcome": "ok"})
    recorder.gauge("fsp_process_info", 1, {"worker_role": "api"})
    server = MetricsServer(recorder.registry, port=0)
    server.start()
    try:
        body, _ = _scrape(server.port)
        for sentinel in sentinels:
            assert sentinel not in body, f"exposition leaked {sentinel!r}"
    finally:
        server.close()
