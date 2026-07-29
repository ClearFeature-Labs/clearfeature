import os
import sys
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.models import EntityKey, RawReportMeta
from fintech_feature_platform.fs_core.raw.postgres_meta_repository import (
    PostgresRawReportMetaRepository,
    _meta_to_params,
    _row_to_meta,
)

_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)


def _meta() -> RawReportMeta:
    return RawReportMeta(
        report_ref="rep_1",
        report_type="credit_report",
        entity_type="application",
        entity_key=EntityKey.from_mapping(
            {"user_id": "1", "application_id": "A1"},
            key_order=["user_id", "application_id"],
        ),
        report_ts=_TS,
        payload_size_bytes=123,
        content_hash="sha256:abc",
        storage_uri="s3://raw-reports/credit/rep_1.json.gz",
        created_at=_TS,
    )


class _FakeCursor:
    def __init__(self, row) -> None:
        self._row = row
        self.executed: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None) -> None:
        self.executed.append((sql, params))

    def fetchone(self):
        return self._row


class _FakeConnection:
    def __init__(self, row=None) -> None:
        self.cursor_obj = _FakeCursor(row)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1


def test_module_imports_without_psycopg():
    assert "psycopg" not in sys.modules


def test_meta_to_params_maps_all_fields():
    params = _meta_to_params(_meta())
    assert params["report_ref"] == "rep_1"
    assert params["report_type"] == "credit_report"
    assert params["entity_type"] == "application"
    assert params["entity_key"] == '[["user_id", "1"], ["application_id", "A1"]]'
    assert params["report_ts"] == _TS
    assert params["payload_size_bytes"] == 123
    assert params["content_hash"] == "sha256:abc"
    assert params["storage_uri"] == "s3://raw-reports/credit/rep_1.json.gz"
    assert params["format"] == "json"
    assert params["compression"] == "gzip"
    assert params["created_at"] == _TS


def test_row_to_meta_reconstructs_from_parsed_jsonb():
    # psycopg returns JSONB already parsed into a Python list.
    row = dict(_meta_to_params(_meta()))
    row["entity_key"] = [["user_id", "1"], ["application_id", "A1"]]
    meta = _row_to_meta(row)
    assert meta == _meta()
    assert meta.entity_key.encode() == "user_id=1|application_id=A1"


def test_meta_params_row_round_trip():
    original = _meta()
    row = dict(_meta_to_params(original))  # entity_key is a JSON string here
    assert _row_to_meta(row) == original


def test_get_meta_selects_and_maps_row():
    row = dict(_meta_to_params(_meta()))
    conn = _FakeConnection(row=row)
    repo = PostgresRawReportMetaRepository(conn)

    result = repo.get_meta("rep_1")

    assert result == _meta()
    sql, params = conn.cursor_obj.executed[0]
    assert "SELECT" in sql and "raw_reports_meta" in sql
    assert params == {"report_ref": "rep_1"}


def test_get_meta_missing_row_raises_key_error():
    repo = PostgresRawReportMetaRepository(_FakeConnection(row=None))
    with pytest.raises(KeyError):
        repo.get_meta("missing")


def test_get_meta_malformed_row_raises_value_error():
    row = dict(_meta_to_params(_meta()))
    row["entity_key"] = "not valid json{"
    repo = PostgresRawReportMetaRepository(_FakeConnection(row=row))
    with pytest.raises(ValueError):
        repo.get_meta("rep_1")


def test_add_executes_upsert_and_commits():
    conn = _FakeConnection()
    repo = PostgresRawReportMetaRepository(conn)

    repo.add(_meta())

    sql, params = conn.cursor_obj.executed[0]
    assert "INSERT INTO raw_reports_meta" in sql
    assert params["report_ref"] == "rep_1"
    assert conn.commits == 1


def test_sql_uses_parameters_not_interpolation():
    conn = _FakeConnection()
    repo = PostgresRawReportMetaRepository(conn)
    repo.add(_meta())
    sql, params = conn.cursor_obj.executed[0]
    # Values travel as parameters; they must not be baked into the SQL text.
    assert "%(report_ref)s" in sql
    assert "rep_1" not in sql
    assert params["report_ref"] == "rep_1"


def test_add_is_upsert_shaped():
    conn = _FakeConnection()
    repo = PostgresRawReportMetaRepository(conn)
    repo.add(_meta())
    sql, _ = conn.cursor_obj.executed[0]
    assert "ON CONFLICT (report_ref) DO UPDATE" in sql


@pytest.mark.skipif(
    os.getenv("FSP_POSTGRES_INTEGRATION") != "1",
    reason="set FSP_POSTGRES_INTEGRATION=1 (and run local Postgres) to enable",
)
def test_live_postgres_round_trip():
    pytest.importorskip("psycopg")
    from fintech_feature_platform.fs_core.raw.postgres_meta_repository import (
        connect_postgres,
    )

    dsn = os.getenv(
        "FSP_POSTGRES_DSN",
        "postgresql://fsp:fsp_dev_password@localhost:5432/fsp",
    )
    conn = connect_postgres(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS raw_reports_meta ("
                "report_ref TEXT PRIMARY KEY, report_type TEXT NOT NULL, "
                "entity_type TEXT NOT NULL, entity_key JSONB NOT NULL, "
                "report_ts TIMESTAMPTZ NOT NULL, payload_size_bytes BIGINT NOT NULL, "
                "content_hash TEXT NOT NULL, storage_uri TEXT NOT NULL, "
                "format TEXT NOT NULL, compression TEXT NOT NULL, "
                "created_at TIMESTAMPTZ NOT NULL)"
            )
        conn.commit()
        repo = PostgresRawReportMetaRepository(conn)
        repo.add(_meta())
        assert repo.get_meta("rep_1") == _meta()
    finally:
        conn.close()
