import json
import os
import sys
import uuid
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.stores.offline import OfflineFeatureRecord
from fintech_feature_platform.fs_core.stores.postgres_offline import (
    PostgresOfflineStore,
    _result_to_params,
    _row_to_record,
)

_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)


def _key(user_id: str = "1") -> EntityKey:
    return EntityKey.from_mapping(
        {"user_id": user_id, "application_id": "A1"},
        key_order=["user_id", "application_id"],
    )


def _result(
    name="declared_income",
    version=1,
    *,
    key=None,
    value=3_500_000,
    max_input_data_ts=None,
    input_fingerprint=None,
    value_hash=None,
):
    return FeatureResult(
        ref=FeatureRef(name, version),
        entity_key=key or _key(),
        value=value,
        data_ts=_TS,
        calc_ts=_TS,
        max_input_data_ts=max_input_data_ts,
        input_fingerprint=input_fingerprint,
        value_hash=value_hash,
    )


def _row_from_params(params: dict) -> dict:
    # Simulate what psycopg returns for a stored row: JSONB columns come back parsed.
    return {
        "entity_key": json.loads(params["entity_key"]),
        "view": params["view"],
        "view_version": params["view_version"],
        "feature_name": params["feature_name"],
        "feature_version": params["feature_version"],
        "value_json": json.loads(params["value_json"]),
        "data_ts": params["data_ts"],
        "calc_ts": params["calc_ts"],
        "max_input_data_ts": params["max_input_data_ts"],
        "input_fingerprint": params["input_fingerprint"],
        "value_hash": params["value_hash"],
    }


class _FakeCopy:
    """Records COPY-streamed rows; optionally raises on the Nth write_row (atomicity)."""

    def __init__(self, sql, sink, raise_on_row=None) -> None:
        self.sql = sql
        self._sink = sink
        self._raise_on_row = raise_on_row
        self._n = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def write_row(self, row) -> None:
        self._n += 1
        if self._raise_on_row is not None and self._n == self._raise_on_row:
            raise RuntimeError("simulated COPY failure")
        self._sink.append(tuple(row))


class _FakeCursor:
    def __init__(self, rows=(), copy_raise_on_row=None) -> None:
        self._rows = list(rows)
        self.executed: list[tuple] = []
        self.executemany_calls: list[tuple] = []
        self.copy_calls: list[str] = []
        self.copied_rows: list[tuple] = []
        self._copy_raise_on_row = copy_raise_on_row

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None) -> None:
        self.executed.append((sql, params))

    def executemany(self, sql, seq_params) -> None:
        self.executemany_calls.append((sql, list(seq_params)))

    def copy(self, sql) -> _FakeCopy:
        self.copy_calls.append(sql)
        return _FakeCopy(sql, self.copied_rows, self._copy_raise_on_row)

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, rows=(), copy_raise_on_row=None) -> None:
        self.cursor_obj = _FakeCursor(rows, copy_raise_on_row=copy_raise_on_row)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1


def test_module_imports_without_psycopg():
    assert "psycopg" not in sys.modules


def test_result_to_params_maps_all_fields():
    params = _result_to_params("user_credit_risk", 1, _result())
    assert params["entity_key"] == '[["user_id", "1"], ["application_id", "A1"]]'
    assert params["entity_key_encoded"] == "user_id=1|application_id=A1"
    assert params["view"] == "user_credit_risk"
    assert params["view_version"] == 1
    assert params["feature_name"] == "declared_income"
    assert params["feature_version"] == 1
    assert params["value_json"] == "3500000"
    assert params["data_ts"] == _TS
    assert params["calc_ts"] == _TS


def test_result_to_params_rejects_non_json_value():
    with pytest.raises(ValueError):
        _result_to_params("v", 1, _result(value=object()))


def test_none_value_becomes_json_null_not_sql_null():
    params = _result_to_params("v", 1, _result(value=None))
    assert params["value_json"] == "null"  # JSON null payload, not Python None / SQL NULL


def test_row_to_record_reconstructs():
    params = _result_to_params("user_credit_risk", 1, _result())
    record = _row_to_record(_row_from_params(params))
    assert record == OfflineFeatureRecord("user_credit_risk", 1, _result())


def test_params_row_round_trip_for_each_value_type():
    for value in (3_500_000, 0.23, "ok", None, {"a": 1}, [1, 2]):
        result = _result(value=value)
        record = _row_to_record(_row_from_params(_result_to_params("v", 1, result)))
        assert record == OfflineFeatureRecord("v", 1, result)


def test_append_inserts_once_and_commits():
    conn = _FakeConnection()
    PostgresOfflineStore(conn).append("v", 1, _result())
    assert len(conn.cursor_obj.executed) == 1
    sql, _ = conn.cursor_obj.executed[0]
    assert "INSERT INTO features_offline" in sql
    assert conn.commits == 1


def test_append_many_uses_single_copy_and_commits_once():
    # Bulk path : one COPY streaming all rows, one commit — not a per-row loop.
    conn = _FakeConnection()
    results = [_result("declared_income"), _result("monthly_obligations", value=700_000)]
    PostgresOfflineStore(conn).append_many("v", 1, results)
    assert conn.cursor_obj.copy_calls == [
        "COPY features_offline (" + ", ".join((
            "entity_key", "entity_key_encoded", "view", "view_version",
            "feature_name", "feature_version", "value_json", "data_ts", "calc_ts",
            "max_input_data_ts", "input_fingerprint", "value_hash",
            "model_uri", "model_digest", "model_output_name", "bundle_digest",
            "available_at", "availability_source",
        )) + ") FROM STDIN"
    ]
    assert len(conn.cursor_obj.copied_rows) == 2  # both rows streamed in one COPY
    assert conn.cursor_obj.executemany_calls == []  # not the old per-row loop
    assert conn.commits == 1


def test_append_many_copy_row_carries_d9_metadata():
    conn = _FakeConnection()
    result = _result(
        max_input_data_ts=datetime(2024, 8, 26, 12, tzinfo=UTC),
        input_fingerprint="sha256:fp", value_hash="sha256:vh",
    )
    PostgresOfflineStore(conn).append_many("v", 1, [result])
    row = conn.cursor_obj.copied_rows[0]
    # COPY column order: ...(index 9,10,11) = max_input_data_ts, input_fingerprint, value_hash
    assert row[9] == datetime(2024, 8, 26, 12, tzinfo=UTC)
    assert row[10] == "sha256:fp"
    assert row[11] == "sha256:vh"


def test_append_many_is_atomic_on_failure():
    # A COPY write failure mid-stream must raise and never commit (all-or-nothing).
    conn = _FakeConnection(copy_raise_on_row=2)
    results = [_result("a"), _result("b"), _result("c")]
    with pytest.raises(RuntimeError, match="COPY"):
        PostgresOfflineStore(conn).append_many("v", 1, results)
    assert conn.commits == 0


def test_append_many_empty_is_noop():
    conn = _FakeConnection()
    PostgresOfflineStore(conn).append_many("v", 1, [])
    assert conn.cursor_obj.copy_calls == []
    assert conn.cursor_obj.executed == []
    assert conn.commits == 0


def test_get_filters_by_entity_key_encoded_and_orders_by_row_id():
    rows = [_row_from_params(_result_to_params("v", 1, _result()))]
    conn = _FakeConnection(rows=rows)
    records = PostgresOfflineStore(conn).get(_key())
    assert len(records) == 1
    sql, params = conn.cursor_obj.executed[0]
    assert "entity_key_encoded = %(entity_key_encoded)s" in sql
    assert "ORDER BY row_id ASC" in sql
    assert params == {"entity_key_encoded": "user_id=1|application_id=A1"}


def test_get_supports_optional_filters():
    conn = _FakeConnection(rows=[])
    PostgresOfflineStore(conn).get(
        _key(),
        feature_name="declared_income",
        feature_version=1,
        view="user_credit_risk",
        view_version=1,
    )
    sql, params = conn.cursor_obj.executed[0]
    for column in ("view", "view_version", "feature_name", "feature_version"):
        assert f"{column} = %({column})s" in sql
    assert params["feature_name"] == "declared_income"
    assert params["view"] == "user_credit_risk"


def test_get_returns_empty_list_when_no_rows():
    conn = _FakeConnection(rows=[])
    assert PostgresOfflineStore(conn).get(_key()) == []


def test_sql_is_parametrized_and_omits_values():
    conn = _FakeConnection()
    PostgresOfflineStore(conn).append("v", 1, _result())
    sql, params = conn.cursor_obj.executed[0]
    assert "%(entity_key_encoded)s" in sql
    assert "user_id=1|application_id=A1" not in sql
    assert params["entity_key_encoded"] == "user_id=1|application_id=A1"


# --- get_pit SQL  --------------------------------------------------

from datetime import timedelta  # noqa: E402

_OBS = datetime(2024, 8, 26, 13, tzinfo=UTC)


def _get_pit(conn, *, observation_ts=_OBS, safety_gap=timedelta(0)):
    return PostgresOfflineStore(conn).get_pit(
        _key(),
        feature_name="declared_income",
        feature_version=1,
        view="user_credit_risk",
        view_version=1,
        observation_ts=observation_ts,
        safety_gap=safety_gap,
    )


def test_get_pit_query_applies_safety_gap_and_availability():
    conn = _FakeConnection(rows=[])
    _get_pit(conn, safety_gap=timedelta(hours=2))
    sql, params = conn.cursor_obj.executed[0]
    # Both PIT clocks are pushed into SQL (effective availability — trusted
    # available_at wins, legacy rows keep the conservative calc_ts fallback).
    assert "data_ts <= %(cutoff)s" in sql
    assert "COALESCE(available_at, calc_ts) <= %(observation_ts)s" in sql
    assert "ORDER BY data_ts DESC, calc_ts DESC, row_id DESC LIMIT 1" in sql
    # cutoff = observation_ts - safety_gap
    assert params["cutoff"] == _OBS - timedelta(hours=2)
    assert params["observation_ts"] == _OBS


def test_get_pit_returns_reconstructed_record():
    rows = [_row_from_params(_result_to_params("user_credit_risk", 1, _result()))]
    conn = _FakeConnection(rows=rows)
    record = _get_pit(conn)
    assert record == OfflineFeatureRecord("user_credit_risk", 1, _result())


def test_get_pit_returns_none_when_no_eligible_row():
    conn = _FakeConnection(rows=[])
    assert _get_pit(conn) is None


def test_get_pit_rejects_negative_safety_gap():
    with pytest.raises(ValueError, match="safety_gap"):
        _get_pit(_FakeConnection(rows=[]), safety_gap=timedelta(seconds=-1))


def test_get_malformed_row_raises_value_error():
    bad_row = _row_from_params(_result_to_params("v", 1, _result()))
    bad_row["entity_key"] = "not valid json{"
    conn = _FakeConnection(rows=[bad_row])
    with pytest.raises(ValueError):
        PostgresOfflineStore(conn).get(_key())


@pytest.mark.skipif(
    os.getenv("FSP_POSTGRES_INTEGRATION") != "1",
    reason="set FSP_POSTGRES_INTEGRATION=1 (and run local Postgres) to enable",
)
def test_live_postgres_append_only_history():
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
                "CREATE TABLE IF NOT EXISTS features_offline ("
                "row_id BIGSERIAL PRIMARY KEY, entity_key JSONB NOT NULL, "
                "entity_key_encoded TEXT NOT NULL, view TEXT NOT NULL, "
                "view_version INTEGER NOT NULL, feature_name TEXT NOT NULL, "
                "feature_version INTEGER NOT NULL, value_json JSONB NOT NULL, "
                "data_ts TIMESTAMPTZ NOT NULL, calc_ts TIMESTAMPTZ NOT NULL)"
            )
        conn.commit()
        # Unique entity per run so append-only accumulation does not affect assertions.
        key = _key(user_id=uuid.uuid4().hex)
        store = PostgresOfflineStore(conn)
        store.append("user_credit_risk", 1, _result(key=key, value=1))
        store.append("user_credit_risk", 1, _result(key=key, value=2))
        records = store.get(key, feature_name="declared_income")
        assert [r.result.value for r in records] == [1, 2]
    finally:
        conn.close()


# --- D9 metadata columns  ------------------------------------------

_MAX_TS = datetime(2024, 8, 26, 12, tzinfo=UTC)


def test_result_to_params_maps_d9_metadata():
    params = _result_to_params(
        "v", 1,
        _result(max_input_data_ts=_MAX_TS, input_fingerprint="sha256:fp",
                value_hash="sha256:vh"),
    )
    assert params["max_input_data_ts"] == _MAX_TS
    assert params["input_fingerprint"] == "sha256:fp"
    assert params["value_hash"] == "sha256:vh"


def test_d9_metadata_round_trips_through_row():
    original = _result(
        max_input_data_ts=_MAX_TS, input_fingerprint="sha256:fp", value_hash="sha256:vh"
    )
    record = _row_to_record(_row_from_params(_result_to_params("v", 1, original)))
    assert record.result.max_input_data_ts == _MAX_TS
    assert record.result.input_fingerprint == "sha256:fp"
    assert record.result.value_hash == "sha256:vh"


def test_pre_d9_row_without_metadata_reads_as_none():
    # Old rows (or rows selected before the 005 migration) have no D9 columns.
    row = _row_from_params(_result_to_params("v", 1, _result()))
    for column in ("max_input_data_ts", "input_fingerprint", "value_hash"):
        row.pop(column)
    record = _row_to_record(row)
    assert record.result.max_input_data_ts is None
    assert record.result.input_fingerprint is None
    assert record.result.value_hash is None
