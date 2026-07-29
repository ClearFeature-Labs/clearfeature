"""Postgres-backed offline feature store.

Implements the ``InMemoryOfflineStore`` shape (``append`` / ``append_many`` /
``get``) over a tall, append-only ``features_offline`` table (see
``infra/postgres/002_features_offline.sql``). Recomputing a feature appends a new
row; nothing is overwritten or deleted, and ``get`` returns matching rows in append
order (``row_id ASC``), or ``[]`` when nothing matches.

D9 metadata (``max_input_data_ts``/``input_fingerprint``/``value_hash``, see
``infra/postgres/005_features_offline_d9.sql``) is stored nullable — legacy rows and
externally supplied results have none.

``EntityKey`` is stored as ordered JSONB pairs plus an encoded lookup key; feature
values are stored as JSONB (strict ``json.dumps`` — a non-JSON-serializable value
raises ``ValueError``); datetimes as ``TIMESTAMPTZ``. No ``psycopg`` import at module
load time, so this module imports — and its unit tests run — without the optional
``postgres`` extra. The caller owns the connection lifetime.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from typing import Any

from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.stores.offline import OfflineFeatureRecord

_SELECT_COLUMNS = (
    "entity_key",
    "view",
    "view_version",
    "feature_name",
    "feature_version",
    "value_json",
    "data_ts",
    "calc_ts",
    "max_input_data_ts",
    "input_fingerprint",
    "value_hash",
    "model_uri",
    "model_digest",
    "model_output_name",
    "bundle_digest",
    "available_at",
    "availability_source",
)

# Allowlist of columns that get(...) may filter on (besides the always-applied
# entity_key_encoded). Values are always passed as parameters, never interpolated.
_OPTIONAL_FILTERS = {
    "view": "view",
    "view_version": "view_version",
    "feature_name": "feature_name",
    "feature_version": "feature_version",
}

_INSERT_SQL = """
INSERT INTO features_offline (
    entity_key, entity_key_encoded, view, view_version,
    feature_name, feature_version, value_json, data_ts, calc_ts,
    max_input_data_ts, input_fingerprint, value_hash,
    model_uri, model_digest, model_output_name, bundle_digest, available_at,
    availability_source
) VALUES (
    %(entity_key)s::jsonb, %(entity_key_encoded)s, %(view)s, %(view_version)s,
    %(feature_name)s, %(feature_version)s, %(value_json)s::jsonb, %(data_ts)s, %(calc_ts)s,
    %(max_input_data_ts)s, %(input_fingerprint)s, %(value_hash)s,
    %(model_uri)s, %(model_digest)s, %(model_output_name)s, %(bundle_digest)s,
    %(available_at)s, %(availability_source)s
)
"""

# Column order for the bulk COPY path (row_id is BIGSERIAL, auto-assigned).
_COPY_COLUMNS = (
    "entity_key", "entity_key_encoded", "view", "view_version",
    "feature_name", "feature_version", "value_json", "data_ts", "calc_ts",
    "max_input_data_ts", "input_fingerprint", "value_hash",
    "model_uri", "model_digest", "model_output_name", "bundle_digest", "available_at",
    "availability_source",
)
_COPY_SQL = (
    f"COPY features_offline ({', '.join(_COPY_COLUMNS)}) FROM STDIN"
)


class PostgresOfflineStore:
    """Append-only offline feature history backed by Postgres."""

    def __init__(self, connection) -> None:
        self._connection = connection

    def append(self, view: str, view_version: int, result: FeatureResult) -> None:
        with self._connection.cursor() as cur:
            cur.execute(_INSERT_SQL, _result_to_params(view, view_version, result))
        self._connection.commit()

    def append_many(
        self, view: str, view_version: int, results: Iterable[FeatureResult]
    ) -> None:
        """Bulk-append offline rows via a single COPY in one transaction.

        Atomic at call level: every row streams inside one ``COPY ... FROM STDIN``, then
        one commit; if any row fails the ``with`` block exits before commit, so nothing
        is persisted (the batch worker treats that as an infra failure -> replay). This
        is the chunk-level write shape that keeps large ref-scoped jobs off the per-row
        path. ``entity_key``/``value_json`` are sent as JSON text (COPY parses them into
        jsonb); all D9 metadata columns are carried.
        """
        rows = [_result_to_copy_row(view, view_version, r) for r in results]
        if not rows:
            return  # no-op: nothing to insert, nothing to commit
        with self._connection.cursor() as cur, cur.copy(_COPY_SQL) as copy:
            for row in rows:
                copy.write_row(row)
        self._connection.commit()

    def get(
        self,
        entity_key: EntityKey,
        feature_name: str | None = None,
        feature_version: int | None = None,
        view: str | None = None,
        view_version: int | None = None,
    ) -> list[OfflineFeatureRecord]:
        conditions = ["entity_key_encoded = %(entity_key_encoded)s"]
        params: dict[str, Any] = {"entity_key_encoded": entity_key.encode()}
        for name, value in (
            ("view", view),
            ("view_version", view_version),
            ("feature_name", feature_name),
            ("feature_version", feature_version),
        ):
            if value is not None:
                column = _OPTIONAL_FILTERS[name]
                conditions.append(f"{column} = %({name})s")
                params[name] = value

        sql = (
            f"SELECT {', '.join(_SELECT_COLUMNS)} FROM features_offline "
            f"WHERE {' AND '.join(conditions)} ORDER BY row_id ASC"
        )
        with self._connection.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [_row_to_record(row) for row in rows]

    def get_pit(
        self,
        entity_key: EntityKey,
        *,
        feature_name: str,
        feature_version: int,
        view: str,
        view_version: int,
        observation_ts: datetime,
        safety_gap: timedelta = timedelta(0),
    ) -> OfflineFeatureRecord | None:
        """Latest PIT-eligible record via SQL — the same rule as ``fs_core.pit``.

        Eligibility is pushed into the query :
        ``data_ts <= observation_ts - safety_gap`` and
        ``COALESCE(available_at, calc_ts) <= observation_ts`` — trusted availability
        wins when present; legacy rows keep the conservative calc_ts fallback. The
        latest is ``ORDER BY data_ts DESC, calc_ts DESC, row_id DESC LIMIT 1``
        (row_id breaks exact ties to the last-written row, matching the in-memory
        append-order tie-break).
        """
        if safety_gap < timedelta(0):
            raise ValueError("safety_gap must be non-negative")
        params: dict[str, Any] = {
            "entity_key_encoded": entity_key.encode(),
            "view": view,
            "view_version": view_version,
            "feature_name": feature_name,
            "feature_version": feature_version,
            "cutoff": observation_ts - safety_gap,
            "observation_ts": observation_ts,
        }
        sql = (
            f"SELECT {', '.join(_SELECT_COLUMNS)} FROM features_offline "
            "WHERE entity_key_encoded = %(entity_key_encoded)s "
            "AND view = %(view)s AND view_version = %(view_version)s "
            "AND feature_name = %(feature_name)s "
            "AND feature_version = %(feature_version)s "
            "AND data_ts <= %(cutoff)s "
            "AND COALESCE(available_at, calc_ts) <= %(observation_ts)s "
            "ORDER BY data_ts DESC, calc_ts DESC, row_id DESC LIMIT 1"
        )
        with self._connection.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return _row_to_record(rows[0]) if rows else None


def _dump_value(value: Any) -> str:
    try:
        return json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"feature value is not JSON-serializable: {exc}") from exc


def _result_to_copy_row(
    view: str, view_version: int, result: FeatureResult
) -> tuple:
    """One COPY row tuple in ``_COPY_COLUMNS`` order.

    ``entity_key`` and ``value_json`` are JSON text — psycopg streams them as the column
    text and Postgres parses them into jsonb; datetimes/scalars adapt natively.
    """
    p = _result_to_params(view, view_version, result)
    return tuple(p[column] for column in _COPY_COLUMNS)


def _result_to_params(view: str, view_version: int, result: FeatureResult) -> dict[str, Any]:
    return {
        "entity_key": json.dumps([[name, value] for name, value in result.entity_key.parts]),
        "entity_key_encoded": result.entity_key.encode(),
        "view": view,
        "view_version": view_version,
        "feature_name": result.ref.name,
        "feature_version": result.ref.version,
        "value_json": _dump_value(result.value),
        "data_ts": result.data_ts,
        "calc_ts": result.calc_ts,
        "max_input_data_ts": result.max_input_data_ts,
        "input_fingerprint": result.input_fingerprint,
        "value_hash": result.value_hash,
        # F3 model lineage + bundle digest ( live finding: these were silently
        # dropped by the Postgres store; nullable — F1/F2/legacy rows have none).
        "model_uri": result.model_uri,
        "model_digest": result.model_digest,
        "model_output_name": result.model_output_name,
        "bundle_digest": result.bundle_digest,
        "available_at": result.available_at,
        "availability_source": result.availability_source,
    }


def _row_to_record(row: Mapping[str, Any]) -> OfflineFeatureRecord:
    try:
        parts = row["entity_key"]
        if isinstance(parts, str):
            parts = json.loads(parts)
        entity_key = EntityKey(tuple((str(p[0]), str(p[1])) for p in parts))
        result = FeatureResult(
            ref=FeatureRef(row["feature_name"], row["feature_version"]),
            entity_key=entity_key,
            value=row["value_json"],
            data_ts=row["data_ts"],
            calc_ts=row["calc_ts"],
            # Nullable D9 metadata (legacy rows have none).
            max_input_data_ts=row.get("max_input_data_ts"),
            input_fingerprint=row.get("input_fingerprint"),
            value_hash=row.get("value_hash"),
            # Nullable F3/bundle lineage (pre-009 rows have none).
            model_uri=row.get("model_uri"),
            model_digest=row.get("model_digest"),
            model_output_name=row.get("model_output_name"),
            bundle_digest=row.get("bundle_digest"),
            # Nullable availability clock (legacy rows have none).
            available_at=row.get("available_at"),
            availability_source=row.get("availability_source"),
        )
        return OfflineFeatureRecord(row["view"], row["view_version"], result)
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        raise ValueError(f"malformed features_offline row: {exc}") from exc
