"""Optional live integration smoke for the local backend.

Skipped by default. Requires the optional extras and running MinIO + Postgres +
Valkey (docker compose). Enable with:

    uv sync --extra dev --extra storage --extra postgres --extra online
    docker compose up -d postgres minio valkey
    FSP_BACKEND=local FSP_LOCAL_BACKEND_INTEGRATION=1 \
        uv run pytest tests/api/test_local_backend_wiring.py
    docker compose down
"""

import os
import uuid
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("FSP_LOCAL_BACKEND_INTEGRATION") != "1",
    reason="set FSP_LOCAL_BACKEND_INTEGRATION=1 (and run local backends) to enable",
)

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS raw_reports_meta ("
    "report_ref TEXT PRIMARY KEY, report_type TEXT NOT NULL, entity_type TEXT NOT NULL, "
    "entity_key JSONB NOT NULL, report_ts TIMESTAMPTZ NOT NULL, "
    "payload_size_bytes BIGINT NOT NULL, content_hash TEXT NOT NULL, "
    "storage_uri TEXT NOT NULL, format TEXT NOT NULL, compression TEXT NOT NULL, "
    "created_at TIMESTAMPTZ NOT NULL);"
    "CREATE TABLE IF NOT EXISTS features_offline ("
    "row_id BIGSERIAL PRIMARY KEY, entity_key JSONB NOT NULL, "
    "entity_key_encoded TEXT NOT NULL, view TEXT NOT NULL, view_version INTEGER NOT NULL, "
    "feature_name TEXT NOT NULL, feature_version INTEGER NOT NULL, value_json JSONB NOT NULL, "
    "data_ts TIMESTAMPTZ NOT NULL, calc_ts TIMESTAMPTZ NOT NULL);"
)


def _apply_schema() -> None:
    from fintech_feature_platform.fs_core.raw.postgres_meta_repository import (
        connect_postgres,
    )

    dsn = os.getenv(
        "FSP_POSTGRES_DSN", "postgresql://fsp:fsp_dev_password@localhost:5432/fsp"
    )
    conn = connect_postgres(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(_SCHEMA)
        conn.commit()
    finally:
        conn.close()


def test_local_backend_read_round_trip(monkeypatch):
    # Seeds the real local online (Valkey) + offline (Postgres) stores directly, then
    # exercises the read APIs over them. The compute-direct route was removed in Task
    # The Kafka-first ingest path (MinIO payload + event) is covered elsewhere.
    pytest.importorskip("minio")
    pytest.importorskip("psycopg")
    pytest.importorskip("redis")

    from datetime import UTC, datetime

    from fastapi.testclient import TestClient

    from fintech_feature_platform.api.app import create_app
    from fintech_feature_platform.api.local_backend import build_local_backend
    from fintech_feature_platform.api.settings import load_settings
    from fintech_feature_platform.fs_core.models import (
        EntityKey,
        FeatureRef,
        FeatureResult,
    )

    _apply_schema()
    monkeypatch.setenv("FSP_BACKEND", "local")
    backend = build_local_backend(load_settings())
    client = TestClient(create_app(backend))

    user_id = uuid.uuid4().hex
    entity = {"user_id": user_id, "application_id": "A1"}
    entity_key = EntityKey.from_mapping(entity, key_order=["user_id", "application_id"])
    ts = datetime(2026, 6, 22, 10, tzinfo=UTC)
    result = FeatureResult(
        ref=FeatureRef("declared_income", 1),
        entity_key=entity_key,
        value=5_100_000,
        data_ts=ts,
        calc_ts=ts,
    )
    backend.online.write("user_credit_risk", 1, result)
    backend.offline.append("user_credit_risk", 1, result)

    latest = client.post(
        "/v1/features/latest",
        json={"view": "user_credit_risk", "view_version": 1, "entity": entity,
              "requested_features": ["declared_income"]},
    )
    assert latest.json()["features"]["declared_income"]["value"] == 5_100_000

    history = client.post(
        "/v1/features/history",
        json={"view": "user_credit_risk", "view_version": 1, "entity": entity,
              "feature_name": "declared_income"},
    )
    assert len(history.json()["records"]) == 1

    consistency = client.post(
        "/v1/features/consistency-check",
        json={"view": "user_credit_risk", "view_version": 1, "entity": entity,
              "features": ["declared_income"]},
    )
    assert consistency.status_code == 200
    assert consistency.json()["status"] == "ok"
    assert consistency.json()["checks"]["declared_income"]["status"] == "ok"


def test_local_backend_training_dataset_build(monkeypatch):
    pytest.importorskip("minio")
    pytest.importorskip("psycopg")
    pytest.importorskip("redis")

    from fastapi.testclient import TestClient

    from fintech_feature_platform.api.app import create_app

    _apply_schema()
    monkeypatch.setenv("FSP_BACKEND", "local")
    client = TestClient(create_app())

    user_id = uuid.uuid4().hex
    entity = {"user_id": user_id, "application_id": "A1"}

    direct = client.post(
        "/v1/features/compute-direct",
        json={
            "view": "user_credit_risk",
            "view_version": 1,
            "entity": entity,
            "requested_features": ["declared_income"],
            "inline_sources": {
                "credit_report": {
                    "report_type": "credit_report",
                    "report_ts": "2026-06-22T10:00:00Z",
                    "payload": {"declared_income": 6_300_000, "monthly_obligations": 900_000},
                }
            },
        },
    )
    assert direct.status_code == 200

    build = client.post(
        "/v1/training-datasets/build",
        json={
            "view": "user_credit_risk",
            "view_version": 1,
            "features": ["declared_income"],
            "observations": [
                {"entity": entity, "observation_ts": "2026-06-22T12:00:00Z"}
            ],
            "missing_policy": "keep_null",
        },
    )
    assert build.status_code == 200
    row = build.json()["rows"][0]
    assert row["features"]["declared_income"] == 6_300_000  # PIT: report <= observation
    assert row["feature_metadata"]["declared_income"]["status"] == "ok"


def test_local_backend_offline_only_backfill(monkeypatch):
    pytest.importorskip("minio")
    pytest.importorskip("psycopg")
    pytest.importorskip("redis")

    import json

    from fastapi.testclient import TestClient

    from fintech_feature_platform.api.app import create_app
    from fintech_feature_platform.api.backend import build_backend
    from fintech_feature_platform.api.backfill import run_raw_json_backfill
    from fintech_feature_platform.api.settings import load_settings
    from fintech_feature_platform.fs_core.models import EntityKey

    _apply_schema()
    monkeypatch.setenv("FSP_BACKEND", "local")

    user_id = uuid.uuid4().hex
    entity = {"user_id": user_id, "application_id": "A1"}
    entity_key = EntityKey.from_mapping(entity, key_order=["user_id", "application_id"])

    backend = build_backend(load_settings())
    line = json.dumps(
        {
            "entity": entity,
            "inline_sources": {
                "credit_report": {
                    "report_type": "credit_report",
                    "report_ts": "2026-06-22T10:00:00Z",
                    "payload": {"declared_income": 7_400_000, "monthly_obligations": 800_000},
                }
            },
            "context": {"source_file": "backfill.jsonl"},
        }
    )
    summary = run_raw_json_backfill(
        backend=backend,
        view="user_credit_risk",
        view_version=1,
        requested_features=["declared_income"],
        lines=[line],
        fail_fast=True,
    )
    assert summary.rows_succeeded == 1

    # raw persisted + offline history exists
    report_ref = summary.results[0].report_refs["credit_report"]
    meta = backend.metas.get_meta(report_ref)
    assert backend.payloads.get_payload(meta.storage_uri)["declared_income"] == 7_400_000
    assert len(backend.offline.get(entity_key, feature_name="declared_income")) >= 1
    # online latest NOT updated by backfill
    assert backend.online.get(
        "user_credit_risk", 1, entity_key, "declared_income", 1
    ) is None

    # PIT feature dataset can be built after backfill (offline-backed)
    client = TestClient(create_app())
    build = client.post(
        "/v1/training-datasets/build",
        json={
            "view": "user_credit_risk",
            "view_version": 1,
            "features": ["declared_income"],
            "observations": [
                {"entity": entity, "observation_ts": "2026-06-22T12:00:00Z"}
            ],
            "missing_policy": "keep_null",
        },
    )
    assert build.status_code == 200
    assert build.json()["rows"][0]["features"]["declared_income"] == 7_400_000


def test_local_backend_table_import_export(monkeypatch, tmp_path):
    pytest.importorskip("minio")
    pytest.importorskip("psycopg")
    pytest.importorskip("redis")

    from fintech_feature_platform.api.backend import build_backend
    from fintech_feature_platform.api.feature_dataset_export import write_dataset
    from fintech_feature_platform.api.settings import load_settings
    from fintech_feature_platform.api.table_import import (
        TableFeatureImportConfig,
        run_table_feature_import,
    )
    from fintech_feature_platform.fs_core.models import EntityKey
    from fintech_feature_platform.fs_core.training import (
        TrainingObservation,
        build_training_dataset,
    )

    _apply_schema()
    monkeypatch.setenv("FSP_BACKEND", "local")

    user_id = uuid.uuid4().hex
    entity = {"user_id": user_id, "application_id": "A1"}
    entity_key = EntityKey.from_mapping(entity, key_order=["user_id", "application_id"])

    backend = build_backend(load_settings())
    config = TableFeatureImportConfig.model_validate(
        {
            "view": "user_credit_risk",
            "view_version": 1,
            "entity_columns": ["user_id", "application_id"],
            "data_ts_column": "as_of_ts",
            "features": {"declared_income": {"column": "declared_income", "dtype": "int"}},
        }
    )
    import_rows = [
        {**entity, "as_of_ts": "2026-06-22T10:00:00Z", "declared_income": 5_500_000}
    ]
    summary = run_table_feature_import(
        backend=backend, config=config, rows=import_rows,
        write_mode="offline_and_online",
    )
    assert summary.rows_succeeded == 1
    # offline history + online latest both updated
    offline_rows_after_first = len(
        backend.offline.get(entity_key, feature_name="declared_income")
    )
    assert offline_rows_after_first >= 1
    online = backend.online.get("user_credit_risk", 1, entity_key, "declared_income", 1)
    assert online is not None and online.value == 5_500_000

    # idempotent rerun: identical import does NOT grow offline history in Postgres
    rerun = run_table_feature_import(
        backend=backend, config=config, rows=import_rows, write_mode="offline_only"
    )
    assert rerun.features_written == 0 and rerun.duplicates_skipped == 1
    assert len(
        backend.offline.get(entity_key, feature_name="declared_income")
    ) == offline_rows_after_first

    # PIT dataset + export to JSONL and CSV
    view = next(v for v in backend.registry.feature_views if v.name == "user_credit_risk")
    dataset = build_training_dataset(
        offline=backend.offline,
        view=view,
        view_version=1,
        feature_names=["declared_income"],
        observations=[
            TrainingObservation(entity=entity, observation_ts=datetime(2026, 6, 22, 12, tzinfo=UTC))
        ],
    )
    assert dataset.rows[0].features["declared_income"] == 5_500_000
    jsonl_path = tmp_path / "ds.jsonl"
    csv_path = tmp_path / "ds.csv"
    write_dataset(dataset, str(jsonl_path), "jsonl")
    write_dataset(dataset, str(csv_path), "csv")
    assert jsonl_path.read_text().strip()
    assert "feature.declared_income" in csv_path.read_text()


def test_connection_provider_selection(monkeypatch):
    """pool size 0 -> legacy per-op provider; >0 -> psycopg_pool pool."""
    from fintech_feature_platform.api.local_backend import (
        _build_connection_provider,
        _PerOpConnectionProvider,
    )
    from fintech_feature_platform.api.settings import load_settings

    monkeypatch.setenv("FSP_DB_POOL_SIZE", "0")
    provider = _build_connection_provider(load_settings())
    assert isinstance(provider, _PerOpConnectionProvider)
    assert callable(provider.connection)  # same interface as ConnectionPool

    pytest.importorskip("psycopg_pool")
    from psycopg_pool import ConnectionPool

    monkeypatch.setenv("FSP_DB_POOL_SIZE", "3")
    pool = _build_connection_provider(load_settings())
    try:
        assert isinstance(pool, ConnectionPool)
        assert pool.max_size == 3
        # Pooled connections must match connect_postgres() config (dict rows).
        from psycopg.rows import dict_row

        assert pool.kwargs["row_factory"] is dict_row
    finally:
        pool.close(timeout=1)  # background workers; no server needed to close
