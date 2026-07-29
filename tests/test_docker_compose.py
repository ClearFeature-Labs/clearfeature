"""Docker Compose baseline contract  (Docker-free: pure file checks)."""

import re
from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[1]
_COMPOSE = _REPO / "docker-compose.yml"

_APP_SERVICES = (
    "api", "online-worker", "offline-writer", "metadata-writer",
    "model-score-writer", "batch-worker", "propagation-worker",
)
_INFRA_SERVICES = ("postgres", "valkey", "minio", "redpanda")


def _compose() -> dict:
    return yaml.safe_load(_COMPOSE.read_text(encoding="utf-8"))


def test_compose_parses_and_declares_all_services():
    data = _compose()
    for name in (*_INFRA_SERVICES, *_APP_SERVICES):
        assert name in data["services"], f"missing service {name}"


def test_host_ports_bind_to_localhost_only():
    for service, spec in _compose()["services"].items():
        for port in spec.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), (
                f"{service} publishes {port!r}; host bindings must be 127.0.0.1-only"
            )


def test_redpanda_has_internal_and_external_advertised_listeners():
    command = " ".join(_compose()["services"]["redpanda"]["command"])
    assert "internal://redpanda:19092" in command
    assert re.search(r"external://localhost:\$\{REDPANDA_KAFKA_PORT:-19092\}", command)
    # The internal listener must never advertise localhost to containers.
    assert "internal://localhost" not in command


def test_app_env_uses_docker_service_names_not_localhost():
    data = _compose()
    for service in _APP_SERVICES:
        env = data["services"][service]["environment"]
        assert env["FSP_KAFKA_BOOTSTRAP_SERVERS"] == "redpanda:19092"
        assert env["FSP_MINIO_ENDPOINT"] == "minio:9000"
        assert env["FSP_VALKEY_HOST"] == "valkey"
        assert "@postgres:5432/" in env["FSP_POSTGRES_DSN"]
        assert "localhost" not in str(env), f"{service} env references localhost"


def test_workers_run_real_runner_modules_in_forever_mode():
    data = _compose()
    for service in _APP_SERVICES:
        if service == "api":
            continue
        command = data["services"][service]["command"]
        module = command[2]
        assert module.startswith("fintech_feature_platform.api.")
        assert command[-1] == "--forever"
        # The module must actually exist in the source tree.
        rel = Path("src") / Path(*module.split(".")).with_suffix(".py")
        assert (_REPO / rel).exists(), f"{service} points at missing module {module}"


def test_common_hardening_applied_everywhere():
    for service, spec in _compose()["services"].items():
        # One-shot init services must exit and stay exited.
        expected_restart = "no" if service == "topic-init" else "unless-stopped"
        assert spec.get("restart") == expected_restart, service
        assert spec.get("logging", {}).get("options", {}).get("max-size"), service
        assert spec.get("deploy", {}).get("resources", {}).get("limits"), service


def test_stateful_services_have_volumes_and_real_healthchecks():
    data = _compose()
    assert "pgdata:/var/lib/postgresql/data" in data["services"]["postgres"]["volumes"]
    valkey_cmd = " ".join(data["services"]["valkey"]["command"])
    assert "--appendfsync everysec" in valkey_cmd and "noeviction" in valkey_cmd
    for service in _INFRA_SERVICES:
        test = data["services"][service]["healthcheck"]["test"]
        assert "true" not in [str(t).strip() for t in test], f"{service} fakes health"


def test_env_example_has_required_variables():
    text = (_REPO / ".env.example").read_text(encoding="utf-8")
    for var in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "VALKEY_MAXMEMORY",
                "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "REDPANDA_KAFKA_PORT",
                "API_PORT", "FSP_KAFKA_BOOTSTRAP_SERVERS", "FSP_POSTGRES_DSN"):
        assert var in text, f".env.example missing {var}"


def test_dockerfile_exists_and_runs_non_root():
    text = (_REPO / "Dockerfile").read_text(encoding="utf-8")
    assert "USER fsp" in text
    assert "uv sync --locked" in text
    assert (_REPO / ".dockerignore").exists()


def test_deployment_doc_covers_migration_and_connection_rules():
    text = (_REPO / "docs" / "deployment" / "docker_compose.md").read_text(encoding="utf-8")
    for expected in ("Kubernetes", "on-prem", "redpanda:19092", "127.0.0.1",
                     "not HA production", "PersistentVolumeClaims"):
        assert expected in text, f"deployment doc missing {expected!r}"


def test_runners_expose_forever_flag():
    # The compose worker commands depend on the daemon flag existing in every runner.
    runners = (_REPO / "src" / "fintech_feature_platform" / "api").glob("*_runner.py")
    for runner in runners:
        if runner.name == "kafka_init_runner.py":
            continue  # deliberate one-shot, not a daemon
        assert '"--forever"' in runner.read_text(encoding="utf-8"), runner.name


def test_app_services_set_db_pool_budgets():
    """ : per-runtime pool budgets live in deployment config."""
    data = _compose()
    sizes = {}
    for service in _APP_SERVICES:
        env = data["services"][service]["environment"]
        assert "FSP_DB_POOL_SIZE" in env, f"{service} missing FSP_DB_POOL_SIZE"
        sizes[service] = int(env["FSP_DB_POOL_SIZE"])
        assert sizes[service] > 0, f"{service} must not default to the legacy path"
    assert sizes["api"] == 10  # FastAPI threadpool concurrency budget
    # All budgets together must fit comfortably under max_connections=100.
    assert sum(sizes.values()) <= 50


def test_topic_init_is_a_one_shot_gating_service():
    """ step 2: topics are provisioned before any worker consumes.

    The service is named ``topic-init`` (renamed from ``kafka-init`` in so
    it does not read like a second broker next to ``redpanda``); the runner module
    keeps its ``kafka_init_runner`` name (it uses the Kafka admin protocol).
    """
    data = _compose()
    assert "kafka-init" not in data["services"], "deprecated service name reintroduced"
    init = data["services"]["topic-init"]
    assert init["restart"] == "no", "one-shot service must not restart forever"
    assert init["command"][-1] == "fintech_feature_platform.api.kafka_init_runner"
    assert "ports" not in init
    # Inside the Docker network the provisioner must use the internal listener.
    assert init["environment"]["FSP_KAFKA_BOOTSTRAP_SERVERS"] == "redpanda:19092"
    assert "localhost" not in str(init["environment"])
    assert init["depends_on"] == {"redpanda": {"condition": "service_healthy"}}
    # Every app service starts only after provisioning completed successfully.
    for service in _APP_SERVICES:
        deps = data["services"][service]["depends_on"]
        assert deps["topic-init"]["condition"] == "service_completed_successfully", service
