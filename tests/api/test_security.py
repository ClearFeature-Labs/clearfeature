"""API-key security : roles, modes, startup validation, leak safety.

All security tests pass explicit ``SecurityConfig`` objects (or env mappings), so the
suite-wide development bypass in ``tests/conftest.py`` does not touch them.
"""

import json

import pytest
from examples.credit_decision_demo import model_service
from fastapi import FastAPI
from fastapi.testclient import TestClient

from fintech_feature_platform.api import security as security_module
from fintech_feature_platform.api.app import ENDPOINT_POLICY, create_app
from fintech_feature_platform.api.security import (
    ApiKey,
    SecurityConfig,
    assert_policy_complete,
)

# Real-looking hex secrets (generated shape: openssl rand -hex 32).
SERVICE_SECRET = "5f2b" * 16
SERVICE_SECRET_2 = "9c1d" * 16
OPERATOR_SECRET = "a7e3" * 16
OTHER_REGISTRY_SECRET = "d40f" * 16


def config(*keys: ApiKey, mode: str = "api_key",
           environment: str = "pilot") -> SecurityConfig:
    return SecurityConfig(mode=mode, environment=environment, keys=tuple(keys))


def service_key(secret: str = SERVICE_SECRET, key_id: str = "svc-1") -> ApiKey:
    return ApiKey(key_id=key_id, role="service", secret=secret)


def operator_key() -> ApiKey:
    return ApiKey(key_id="ops-1", role="operator", secret=OPERATOR_SECRET)


def secure_client(*keys: ApiKey) -> TestClient:
    return TestClient(create_app(security=config(*keys)))


def bearer(secret: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {secret}"}


# --- public + 401/403 matrix ----------------------------------------------------------


def test_health_is_public_in_secure_mode():
    client = secure_client(service_key())
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_missing_key_is_401_with_www_authenticate():
    client = secure_client(service_key())
    response = client.get("/v1/feature-requests/nope")
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"


def test_malformed_header_is_401():
    client = secure_client(service_key())
    for header in ("Basic abc", "Bearer", "bearer ", SERVICE_SECRET):
        response = client.get("/v1/feature-requests/nope",
                              headers={"Authorization": header})
        assert response.status_code == 401, header


def test_invalid_key_is_401():
    client = secure_client(service_key())
    response = client.get("/v1/feature-requests/nope", headers=bearer("0" * 64))
    assert response.status_code == 401


def test_service_key_on_service_endpoint_passes_auth():
    client = secure_client(service_key())
    # 404 (unknown request id) proves authentication+authorization passed.
    response = client.get("/v1/feature-requests/nope", headers=bearer(SERVICE_SECRET))
    assert response.status_code == 404


def test_service_key_on_operator_endpoint_is_403():
    client = secure_client(service_key(), operator_key())
    response = client.get("/v1/observability/metrics", headers=bearer(SERVICE_SECRET))
    assert response.status_code == 403
    detail = response.json()["detail"]
    assert detail["status"] == "forbidden"
    assert detail["key_id"] == "svc-1"
    assert SERVICE_SECRET not in response.text


def test_operator_key_is_a_superset_of_service():
    client = secure_client(service_key(), operator_key())
    assert client.get("/v1/observability/metrics",
                      headers=bearer(OPERATOR_SECRET)).status_code == 200
    assert client.get("/v1/feature-requests/nope",
                      headers=bearer(OPERATOR_SECRET)).status_code == 404
    assert client.get("/v1/batch/jobs/nope",
                      headers=bearer(OPERATOR_SECRET)).status_code == 404


def test_batch_and_ingestion_are_operator_only():
    client = secure_client(service_key(), operator_key())
    response = client.get("/v1/batch/jobs/nope", headers=bearer(SERVICE_SECRET))
    assert response.status_code == 403
    response = client.get("/v1/source-datasets/nope", headers=bearer(SERVICE_SECRET))
    assert response.status_code == 403


def test_rotation_two_active_keys_both_work():
    client = secure_client(service_key(), service_key(SERVICE_SECRET_2, "svc-2"))
    for secret in (SERVICE_SECRET, SERVICE_SECRET_2):
        assert client.get("/v1/feature-requests/nope",
                          headers=bearer(secret)).status_code == 404


# --- startup validation ---------------------------------------------------------------


@pytest.mark.parametrize("bad_config, match", [
    (config(), "at least one key"),
    (config(service_key(), service_key(SERVICE_SECRET_2, "svc-1")), "duplicate key_id"),
    (config(service_key(), service_key(SERVICE_SECRET, "svc-2")), "duplicate secret"),
    (config(ApiKey("svc-1", "service", "abc123")), "shorter than"),
    (config(ApiKey("svc-1", "service", "REPLACE_WITH_" + "0" * 32)), "placeholder"),
    (config(ApiKey("svc-1", "admin", SERVICE_SECRET)), "role must be one of"),
    (config(service_key(), mode="oauth"), "FSP_SECURITY_MODE"),
    (config(mode="disabled", environment="pilot"), "refusing to start"),
])
def test_invalid_configuration_fails_startup(bad_config, match):
    with pytest.raises(RuntimeError, match=match):
        create_app(security=bad_config)


def test_from_env_rejects_malformed_json():
    with pytest.raises(RuntimeError, match="not valid JSON"):
        SecurityConfig.from_env({"FSP_API_KEYS": "not-json"})
    with pytest.raises(RuntimeError, match="exactly key_id/role/secret"):
        SecurityConfig.from_env({"FSP_API_KEYS": '[{"key_id": "a"}]'})


def test_from_env_defaults_are_fail_closed():
    parsed = SecurityConfig.from_env({})
    assert parsed.mode == "api_key"
    with pytest.raises(RuntimeError, match="at least one key"):
        parsed.validate()


# --- constant-time lookup -------------------------------------------------------------


def test_resolve_compares_every_key_with_compare_digest(monkeypatch):
    calls = []
    real = security_module.secrets.compare_digest

    def counting(a, b):
        calls.append(1)
        return real(a, b)

    monkeypatch.setattr(security_module.secrets, "compare_digest", counting)
    cfg = config(service_key(), operator_key())
    assert cfg.resolve(SERVICE_SECRET).key_id == "svc-1"
    assert len(calls) == 2  # no early exit: every key compared
    calls.clear()
    assert cfg.resolve("0" * 64) is None
    assert len(calls) == 2


# --- development bypass ---------------------------------------------------------------


def test_explicit_development_bypass(capsys):
    client = TestClient(create_app(
        security=config(mode="disabled", environment="development")))
    assert "security_disabled_mode" in capsys.readouterr().out  # structured event
    assert client.get("/v1/observability/metrics").status_code == 200


# --- docs exposure --------------------------------------------------------------------


def test_docs_disabled_in_api_key_mode():
    client = secure_client(service_key())
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(path).status_code == 404, path


def test_docs_available_with_development_bypass():
    client = TestClient(create_app(
        security=config(mode="disabled", environment="development")))
    assert client.get("/openapi.json").status_code == 200


# --- endpoint-policy completeness -----------------------------------------------------


def test_every_feature_api_route_is_classified():
    app = create_app(security=config(mode="disabled", environment="development"))
    exempt = {"/docs", "/redoc", "/openapi.json", "/docs/oauth2-redirect"}
    mounted = {route.path for route in app.routes
               if getattr(route, "methods", None) and route.path not in exempt}
    assert mounted == set(ENDPOINT_POLICY)


def test_unclassified_route_fails_startup_guard():
    app = FastAPI()

    @app.get("/surprise")
    def surprise() -> dict:
        return {}

    with pytest.raises(RuntimeError, match="without a security classification"):
        assert_policy_complete(app, {}, "test-app")


def test_model_service_routes_are_classified():
    app = model_service.create_app(
        fetch_latest=lambda payload: {},
        security=config(mode="disabled", environment="development"))
    exempt = {"/docs", "/redoc", "/openapi.json", "/docs/oauth2-redirect"}
    mounted = {route.path for route in app.routes
               if getattr(route, "methods", None) and route.path not in exempt}
    assert mounted == set(model_service.ENDPOINT_POLICY)


# --- model service: own registry + outbound credential --------------------------------


def _model_client(*keys: ApiKey, **kwargs) -> TestClient:
    return TestClient(model_service.create_app(
        fetch_latest=kwargs.pop("fetch_latest", lambda payload: {}),
        security=config(*keys), **kwargs))


def test_model_service_requires_its_own_key():
    client = _model_client(service_key(OTHER_REGISTRY_SECRET, "svc-model-client"))
    body = {"user_id": "u", "application_id": "a"}
    assert client.post("/v1/credit/decision", json=body).status_code == 401
    # A key from the FEATURE API registry means nothing here (separate registries).
    assert client.post("/v1/credit/decision", json=body,
                       headers=bearer(SERVICE_SECRET)).status_code == 401
    # Its own key authenticates (502: the stub fetch returns no features -> upstream
    # shape error is fine; what matters is it got PAST auth).
    response = client.post("/v1/credit/decision", json=body,
                           headers=bearer(OTHER_REGISTRY_SECRET))
    assert response.status_code not in (401, 403)


def test_feature_api_key_never_grants_model_service_access_and_vice_versa():
    feature_client = secure_client(service_key())
    assert feature_client.get(
        "/v1/feature-requests/nope",
        headers=bearer(OTHER_REGISTRY_SECRET)).status_code == 401


def test_model_service_secure_mode_requires_outbound_key():
    with pytest.raises(RuntimeError, match="FSP_FEATURE_API_KEY"):
        model_service.create_app(security=config(service_key()))
    # With an injected fetch (tests) no outbound key is needed.
    model_service.create_app(fetch_latest=lambda payload: {},
                             security=config(service_key()))


def test_outbound_fetch_sends_bearer_header(monkeypatch):
    captured = {}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b"{}"

    def fake_urlopen(request, timeout):
        captured["auth"] = request.get_header("Authorization")
        return _Response()

    monkeypatch.setattr(model_service.urllib.request, "urlopen", fake_urlopen)
    fetch = model_service._http_fetch_latest("http://api:8000", SERVICE_SECRET)
    fetch({"view": "credit_decision"})
    assert captured["auth"] == f"Bearer {SERVICE_SECRET}"


# --- leak safety ----------------------------------------------------------------------


def test_auth_errors_do_not_echo_credentials():
    client = secure_client(service_key(), operator_key())
    wrong = client.get("/v1/feature-requests/nope", headers=bearer("0" * 64))
    forbidden = client.get("/v1/observability/metrics",
                           headers=bearer(SERVICE_SECRET))
    for response in (wrong, forbidden):
        text = response.text
        assert SERVICE_SECRET not in text
        assert OPERATOR_SECRET not in text
        assert "0" * 64 not in text


def test_forbidden_detail_contains_only_key_id_and_role():
    client = secure_client(service_key())
    detail = client.get("/v1/observability/metrics",
                        headers=bearer(SERVICE_SECRET)).json()["detail"]
    assert set(detail) == {"status", "key_id", "role", "required"}
    assert json.dumps(detail).count(SERVICE_SECRET) == 0
