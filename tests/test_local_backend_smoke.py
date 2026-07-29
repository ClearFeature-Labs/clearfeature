"""Real local backend smoke: script/docs contract  (Docker-free file checks)."""

import os
import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SMOKE = _REPO / "scripts" / "run_local_backend_smoke.sh"
_MIGRATE = _REPO / "scripts" / "apply_postgres_migrations.sh"
_DOC = _REPO / "docs" / "deployment" / "local_backend_smoke.md"


def _smoke_text() -> str:
    return _SMOKE.read_text(encoding="utf-8")


def test_smoke_script_exists_and_is_executable():
    assert _SMOKE.exists()
    assert os.access(_SMOKE, os.X_OK), "smoke script must be executable"


def test_smoke_script_covers_the_required_chain():
    text = _smoke_text()
    for required in (
        "docker compose config",           # 1. config validation
        "--wait",                          # 2. healthcheck-gated startup
        "/health",                         # 3. API health
        "/v1/observability/metrics",       # 4. metrics
        "/v1/feature-requests/compute",    # 5. real Kafka-first request
        "/v1/features/latest",             # 6. online read-back
        "features_offline",                # 7. Postgres offline verification
        "/v1/lineage/feature-value",       # 8. lineage
        "RestartCount",                    # 9. worker stability
    ):
        assert required in text, f"smoke script missing {required!r}"


def test_smoke_uses_deterministic_smoke_ids_and_polling():
    text = _smoke_text()
    assert "smoke_0072" in text
    assert "wait_for" in text  # polling with timeout, not sleep-only timing


def test_smoke_never_destroys_volumes_by_default():
    text = _smoke_text()
    assert "CLEAN_VOLUMES=0" in text, "clean-volumes must default off"
    # The actual `docker compose down -v` command may run exactly once, and only inside
    # the --clean-volumes gated branch (mentions in messages/comments don't count).
    invocations = [line for line in text.splitlines()
                   if line.strip().startswith("docker compose down -v")]
    assert len(invocations) == 1, f"unexpected `down -v` invocations: {invocations}"
    gated_block = text.split("if (( CLEAN_VOLUMES ))")[1].split("fi")[0]
    assert "docker compose down -v" in gated_block


def test_smoke_checks_all_required_tables():
    text = _smoke_text()
    for table in ("raw_reports_meta", "features_offline", "feature_requests",
                  "request_events", "batch_jobs", "batch_chunks",
                  "source_dataset_manifests", "source_dataset_items"):
        assert table in text, f"schema check missing table {table}"


def test_smoke_has_values_free_safety_checks():
    text = _smoke_text()
    assert re.search(r"payload_json\|object_key\|storage_uri", text), (
        "metrics/lineage responses must be scanned for forbidden fields"
    )


def test_migrations_helper_exists_and_targets_infra_sql():
    assert _MIGRATE.exists()
    assert os.access(_MIGRATE, os.X_OK)
    text = _MIGRATE.read_text(encoding="utf-8")
    assert "infra/postgres/*.sql" in text
    assert "ON_ERROR_STOP" in text


def test_smoke_doc_explains_migrations_honestly():
    text = _DOC.read_text(encoding="utf-8")
    for expected in ("first init", "empty volume", "existing volume",
                     "apply_postgres_migrations.sh", "--clean-volumes"):
        assert expected.lower() in text.lower(), f"smoke doc missing {expected!r}"


def test_smoke_doc_covers_connection_rules_and_troubleshooting():
    text = _DOC.read_text(encoding="utf-8")
    for expected in ("redpanda:19092", "127.0.0.1", "Troubleshooting",
                     "deadline_expired", "restart loop", "stale volume"):
        assert expected.lower() in text.lower(), f"smoke doc missing {expected!r}"


def test_smoke_doc_positions_itself_vs_tests_and_acceptance():
    text = _DOC.read_text(encoding="utf-8").lower()
    assert "beta acceptance" in text
    assert "unit test" in text
    assert "ha" in text


def test_compose_doc_links_the_smoke():
    text = (_REPO / "docs" / "deployment" / "docker_compose.md").read_text(encoding="utf-8")
    assert "local_backend_smoke" in text
