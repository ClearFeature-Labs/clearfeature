"""Metrics wiring: API process + all six worker runners + legacy endpoint.

Proves the API exposes the process-identity metric through the same shared lifecycle as
workers, every runner main is wired to the shared helper (no copy-pasted server logic),
and the legacy /v1/observability/metrics endpoint keeps its schema.
"""

import inspect
import urllib.request

from fintech_feature_platform.api.settings import Settings, load_settings


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_api_process_exposes_process_identity(monkeypatch):
    port = _free_port()
    monkeypatch.setenv("FSP_METRICS_PORT", str(port))
    monkeypatch.setenv("FSP_SECURITY_MODE", "disabled")
    monkeypatch.setenv("FSP_ENVIRONMENT", "development")
    # Prevent the module-level autostart app from racing this test for the port.
    monkeypatch.setenv("FSP_API_APP_AUTOSTART", "0")
    from fintech_feature_platform.api.app import create_app

    app = create_app()
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
            body = resp.read().decode("utf-8")
        assert 'fsp_process_info{worker_role="api"} 1.0' in body
    finally:
        server = getattr(app.state, "metrics_server", None)
        if server is not None:
            server.close()


def test_settings_parse_observability_port(monkeypatch):
    """ closure contract: FSP_OBSERVABILITY_PORT is primary,
    FSP_METRICS_PORT stays a backward-compatible fallback, 0 = disabled default."""
    assert Settings().observability_port == 0  # 0 = disabled (documented default)
    monkeypatch.setenv("FSP_METRICS_PORT", "9464")  # legacy var keeps working
    assert load_settings().observability_port == 9464
    monkeypatch.setenv("FSP_OBSERVABILITY_PORT", "9500")  # primary wins over legacy
    assert load_settings().observability_port == 9500
    monkeypatch.delenv("FSP_METRICS_PORT")
    assert load_settings().observability_port == 9500  # primary alone works too


def test_every_worker_runner_wired_to_shared_lifecycle():
    """Each runner main calls the ONE shared helper (no copy-pasted server code).

    The mains cannot be executed in tests (they connect to Kafka), so wiring is asserted
    at the source level; the helper's behavior itself is covered functionally per-role in
    test_metrics_exposition.py.
    """
    import fintech_feature_platform.api.batch_worker_runner as b
    import fintech_feature_platform.api.metadata_writer_runner as md
    import fintech_feature_platform.api.model_score_writer_runner as ms
    import fintech_feature_platform.api.offline_writer_runner as off
    import fintech_feature_platform.api.online_worker_runner as on
    import fintech_feature_platform.api.propagation_worker_runner as p

    expected_role = {
        on: "online-worker", b: "batch-worker", off: "offline-writer",
        md: "metadata-writer", ms: "model-score-writer", p: "propagation-worker",
    }
    for module, role in expected_role.items():
        source = inspect.getsource(module)
        assert "maybe_start_metrics_server" in source, module.__name__
        assert f'"{role}"' in source, f"{module.__name__} must use canonical role {role!r}"
        # No copy-pasted server internals in runners:
        assert "MetricsServer(" not in source, module.__name__


def test_legacy_observability_endpoint_schema_unchanged():
    from fastapi.testclient import TestClient

    from fintech_feature_platform.api.app import create_app

    client = TestClient(create_app())
    # conftest provides the dev security bypass; operator role not needed in disabled mode
    response = client.get("/v1/observability/metrics")
    assert response.status_code == 200
    data = response.json()
    assert set(data) == {"metrics", "generated_at"}
    assert set(data["metrics"]) == {"counters", "gauges", "histograms"}


def test_route_class_is_framework_template_not_raw_path():
    """many concrete IDs -> ONE route series.

    The middleware label comes from the matched Starlette route TEMPLATE
    (request.scope['route'].path, brace-normalized) — never the raw URL — so
    per-ID paths cannot create new Prometheus series; unmatched paths map to
    the bounded 'unmatched' fallback.
    """
    from fastapi.testclient import TestClient

    from fintech_feature_platform.api.app import create_app

    client = TestClient(create_app())
    for i in range(20):
        client.get(f"/v1/feature-requests/freq_{i}")  # 20 distinct concrete IDs
    client.get("/no/such/route")  # unmatched -> bounded fallback

    snap = client.get("/v1/observability/metrics").json()["metrics"]["counters"]
    route_keys = [k for k in snap if k.startswith("fsp_api_requests_total")]
    assert [k for k in route_keys if "freq_" in k] == [], "raw IDs leaked into series"
    assert any(":request_id" in k for k in route_keys)  # ONE template series for all 20
    assert any("unmatched" in k for k in route_keys)
