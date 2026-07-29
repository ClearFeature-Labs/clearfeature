"""Local backend wiring: MinIO + Postgres + Valkey behind the API.

Built only when ``FSP_BACKEND=local`` (opt-in; memory remains the default). It uses
long-lived MinIO and Valkey clients (both thread-safe) and **one
``psycopg_pool.ConnectionPool`` per process** behind the thin repo
wrappers below (``FSP_DB_POOL_SIZE``, default 4; thread-safe for FastAPI's threadpool).
Setting ``FSP_DB_POOL_SIZE=0`` restores the legacy fresh-connection-per-operation
behavior — the documented rollback lever.

Pool semantics match the legacy path: ``pool.connection()`` commits on clean exit and
rolls back on exception (stores still commit explicitly; the extra empty COMMIT is a
no-op), and returned connections are reset, so no transaction state leaks between
operations. COPY and psycopg's automatic server-side prepared statements work unchanged
on pooled connections. Construction stays lazy-enough: the pool connects in background
threads and surfaces failures on first checkout, exactly where a fresh connection would
have failed before. Required extras: ``uv sync --extra storage --extra postgres --extra
online``; ``build_local_backend`` fails clearly with ``RuntimeError`` if any are
missing.
"""

from __future__ import annotations

import importlib.util
import json
from contextlib import closing
from datetime import datetime, timedelta
from typing import Any

from fintech_feature_platform.api.backend import (
    AppBackend,
    build_event_publisher,
    build_registry_and_udfs,
)
from fintech_feature_platform.api.readiness import (
    DEP_KAFKA,
    DEP_MINIO,
    DEP_POSTGRES,
    DEP_VALKEY,
    DependencyProbe,
)
from fintech_feature_platform.api.settings import Settings
from fintech_feature_platform.api.storage_uri import build_s3_storage_uri
from fintech_feature_platform.fs_core.feature_store import FeatureStore
from fintech_feature_platform.fs_core.observability.catalog import (
    fully_qualified_feature_ids,
)
from fintech_feature_platform.fs_core.observability.prometheus_recorder import (
    PrometheusMetricsRecorder,
)
from fintech_feature_platform.fs_core.observability.timing import TimedStore
from fintech_feature_platform.fs_core.raw.minio_payload_store import (
    MinIOPayloadStore,
    connect_minio,
    connect_minio_probe,
)
from fintech_feature_platform.fs_core.raw.postgres_meta_repository import (
    PostgresRawReportMetaRepository,
    connect_postgres,
)
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.stores.batch_metadata import (
    PostgresBatchMetadataStore,
)
from fintech_feature_platform.fs_core.stores.batch_status import (
    ValkeyBatchJobStatusStore,
)
from fintech_feature_platform.fs_core.stores.metadata import PostgresMetadataStore
from fintech_feature_platform.fs_core.stores.postgres_offline import PostgresOfflineStore
from fintech_feature_platform.fs_core.stores.request_result import (
    ValkeyRequestResultStore,
)
from fintech_feature_platform.fs_core.stores.request_status import (
    ValkeyRequestStatusStore,
)
from fintech_feature_platform.fs_core.stores.source_dataset import (
    PostgresSourceDatasetStore,
)
from fintech_feature_platform.fs_core.stores.valkey_online import (
    ValkeyOnlineStore,
    connect_valkey,
    connect_valkey_probe,
)

_REQUIRED_PACKAGES = ("minio", "psycopg", "redis")


def _require_local_deps() -> None:
    missing = [p for p in _REQUIRED_PACKAGES if importlib.util.find_spec(p) is None]
    if missing:
        raise RuntimeError(
            f"local backend requires missing package(s) {missing}; install with: "
            "uv sync --extra dev --extra storage --extra postgres --extra online"
        )


def _local_serialize_payload(payload: Any) -> bytes:
    # Strict canonical JSON: non-serializable payloads raise (surfaced as HTTP 400).
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


class _PerOpConnectionProvider:
    """Legacy provider: a fresh connection per checkout (``FSP_DB_POOL_SIZE=0``).

    Kept as the rollback lever for the earlier pooling change: same interface as
    ``psycopg_pool.ConnectionPool`` (``.connection()`` context manager), same behavior
    as the legacy wrappers (open, use, close; the server rolls back on error).
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def connection(self):
        return closing(connect_postgres(self._dsn))


def _build_readiness_pg_pool(settings: Settings):
    """Readiness-only Postgres pool, built ONCE.

    The business pool cannot provide a readiness deadline: its checkout bound is 30 s
    and its connections carry data-plane semantics (no short keepalives). This tiny
    pool (``max_size=1``, ``min_size=0`` — connects on demand, so an outage causes no
    background reconnect churn) bounds every phase of the ``SELECT 1`` probe:
    checkout 2 s (``PoolTimeout``), connection establishment ``connect_timeout=2``,
    server-side execution ``statement_timeout=2000``, ACKed-but-silent network stalls
    via short TCP keepalives (~3 s), unACKed stalls via ``tcp_user_timeout=2500`` ms
    (Linux; conninfo-accepted elsewhere). Business connections keep libpq/OS
    defaults — readiness policy must never leak into the data plane.
    """
    from psycopg_pool import ConnectionPool

    return ConnectionPool(
        settings.postgres_dsn,
        kwargs={
            "connect_timeout": 2,
            "options": "-c statement_timeout=2000",
            "keepalives": 1,
            "keepalives_idle": 1,
            "keepalives_interval": 1,
            "keepalives_count": 2,
            "tcp_user_timeout": 2500,
        },
        min_size=0,
        max_size=1,
        timeout=2.0,
        open=True,
    )


def _build_connection_provider(settings: Settings):
    """One Postgres connection provider per process.

    ``db_pool_size > 0`` -> a real ``psycopg_pool.ConnectionPool`` (min_size=1, opened
    eagerly in background threads; failures surface on first checkout). ``0`` -> the
    legacy per-operation provider.
    """
    if settings.db_pool_size <= 0:
        return _PerOpConnectionProvider(settings.postgres_dsn)
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool

    return ConnectionPool(
        settings.postgres_dsn,
        # Pooled connections MUST be configured exactly like connect_postgres() ones:
        # every store reads rows as dicts (row_factory=dict_row).
        kwargs={"row_factory": dict_row},
        min_size=1,
        max_size=settings.db_pool_size,
        open=True,
    )


class _PostgresMetaRepo:
    """Postgres metadata repo over the process connection provider."""

    def __init__(self, pg) -> None:
        self._pg = pg

    def get_meta(self, report_ref: str):
        with self._pg.connection() as conn:
            return PostgresRawReportMetaRepository(conn).get_meta(report_ref)

    def add(self, meta) -> None:
        with self._pg.connection() as conn:
            PostgresRawReportMetaRepository(conn).add(meta)

    def upsert(self, meta, *, correction_context=None) -> bool:
        with self._pg.connection() as conn:
            return PostgresRawReportMetaRepository(conn).upsert(
                meta, correction_context=correction_context
            )


class _PostgresBatchMetadataRepo:
    """Postgres durable batch metadata store over the process connection provider.

    Each method checks out one connection so the store's internal read-modify-write
    (chunk upsert + job-status derivation) shares a single connection/transaction.
    """

    def __init__(self, pg) -> None:
        self._pg = pg

    def upsert_job(self, job) -> None:
        with self._pg.connection() as conn:
            PostgresBatchMetadataStore(conn).upsert_job(job)

    def upsert_chunk_requested(self, chunk) -> None:
        with self._pg.connection() as conn:
            PostgresBatchMetadataStore(conn).upsert_chunk_requested(chunk)

    def upsert_chunk_processed(self, chunk) -> None:
        with self._pg.connection() as conn:
            PostgresBatchMetadataStore(conn).upsert_chunk_processed(chunk)

    def get_job(self, job_id: str):
        with self._pg.connection() as conn:
            return PostgresBatchMetadataStore(conn).get_job(job_id)

    def list_chunks(self, job_id: str):
        with self._pg.connection() as conn:
            return PostgresBatchMetadataStore(conn).list_chunks(job_id)


class _PostgresMetadataRepo:
    """Postgres metadata store over the process connection provider."""

    def __init__(self, pg) -> None:
        self._pg = pg

    def upsert_request(self, metadata) -> None:
        with self._pg.connection() as conn:
            PostgresMetadataStore(conn).upsert_request(metadata)

    def append_event(self, event) -> bool:
        with self._pg.connection() as conn:
            return PostgresMetadataStore(conn).append_event(event)

    def get_request(self, request_id: str):
        with self._pg.connection() as conn:
            return PostgresMetadataStore(conn).get_request(request_id)


class _PostgresOfflineRepo:
    """Postgres offline store over the process connection provider."""

    def __init__(self, pg) -> None:
        self._pg = pg

    def append(self, view: str, view_version: int, result) -> None:
        with self._pg.connection() as conn:
            PostgresOfflineStore(conn).append(view, view_version, result)

    def append_many(self, view: str, view_version: int, results) -> None:
        with self._pg.connection() as conn:
            PostgresOfflineStore(conn).append_many(view, view_version, results)

    def get(
        self,
        entity_key,
        feature_name=None,
        feature_version=None,
        view=None,
        view_version=None,
    ):
        with self._pg.connection() as conn:
            return PostgresOfflineStore(conn).get(
                entity_key,
                feature_name=feature_name,
                feature_version=feature_version,
                view=view,
                view_version=view_version,
            )

    def get_pit(
        self,
        entity_key,
        *,
        feature_name,
        feature_version,
        view,
        view_version,
        observation_ts,
        safety_gap=timedelta(0),
    ):
        with self._pg.connection() as conn:
            return PostgresOfflineStore(conn).get_pit(
                entity_key,
                feature_name=feature_name,
                feature_version=feature_version,
                view=view,
                view_version=view_version,
                observation_ts=observation_ts,
                safety_gap=safety_gap,
            )


class _PostgresSourceDatasetRepo:
    """Postgres source-dataset manifest/item store over the process provider."""

    def __init__(self, pg) -> None:
        self._pg = pg

    def upsert_manifest(self, manifest) -> None:
        with self._pg.connection() as conn:
            PostgresSourceDatasetStore(conn).upsert_manifest(manifest)

    def add_items(self, items) -> None:
        with self._pg.connection() as conn:
            PostgresSourceDatasetStore(conn).add_items(items)

    def get_manifest(self, manifest_id: str):
        with self._pg.connection() as conn:
            return PostgresSourceDatasetStore(conn).get_manifest(manifest_id)

    def list_items(self, manifest_id: str):
        with self._pg.connection() as conn:
            return PostgresSourceDatasetStore(conn).list_items(manifest_id)


def build_local_backend(settings: Settings) -> AppBackend:
    """Wire MinIO + Postgres + Valkey behind the API (dev/local mode, opt-in)."""
    _require_local_deps()
    metrics = PrometheusMetricsRecorder()
    registry, udfs = build_registry_and_udfs(metrics=metrics)
    metrics.bind_dynamic_domain("feature_id", fully_qualified_feature_ids(registry))

    # Storage-boundary timing : platform entry points only, with the
    # closed store/operation/result dimensions — never per-SDK-call instrumentation.
    minio_client = connect_minio(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    payloads = TimedStore(
        MinIOPayloadStore(minio_client, create_bucket=True),
        metrics, "minio", {"get_payload": "read", "put": "write"},
    )
    pg = _build_connection_provider(settings)
    metas = TimedStore(
        _PostgresMetaRepo(pg), metrics, "postgres",
        {"get_meta": "read", "add": "write", "upsert": "write"},
    )
    offline = TimedStore(
        _PostgresOfflineRepo(pg), metrics, "postgres",
        {"append": "append", "append_many": "append", "get_pit": "read"},
    )
    valkey_client = connect_valkey(
        settings.valkey_host, settings.valkey_port, db=settings.valkey_db
    )
    online = TimedStore(
        ValkeyOnlineStore(valkey_client),
        metrics, "valkey", {"write": "write", "write_many": "write", "get": "read"},
    )

    resolver = ReportResolver(payloads, metas)
    store = FeatureStore(registry, udfs, resolver, offline, online, metrics=metrics)

    events = build_event_publisher(settings)

    # Readiness probes : one bounded,
    # side-effect-free check per dependency category. Each probe has a SHORT
    # readiness-specific deadline enforced by a probe-only client built ONCE here
    # (never per /ready request) — data-plane clients keep their own timeout/retry
    # policy untouched (readiness != data-plane; see api/readiness.py). Role
    # filtering lives in api/readiness.py. The Kafka probe exists only when Kafka is
    # actually configured (FSP_EVENTS=kafka); its 2 s deadline is enforced by
    # librdkafka on the existing producer.
    bucket = settings.minio_bucket
    probe_minio = connect_minio_probe(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    probe_valkey = connect_valkey_probe(
        settings.valkey_host, settings.valkey_port, db=settings.valkey_db
    )
    probe_pg = _build_readiness_pg_pool(settings)

    def _check_postgres() -> None:
        with probe_pg.connection() as conn:
            conn.execute("SELECT 1")

    probes = [
        DependencyProbe(DEP_POSTGRES, _check_postgres),
        DependencyProbe(DEP_MINIO, lambda: probe_minio.bucket_exists(bucket)),
        DependencyProbe(DEP_VALKEY, probe_valkey.ping),
    ]
    if settings.events == "kafka":
        probes.append(DependencyProbe(DEP_KAFKA, events.ping))
    readiness_probes = tuple(probes)

    def make_storage_uri(report_type: str, report_ref: str, report_ts: datetime) -> str:
        return build_s3_storage_uri(bucket, report_type, report_ref, report_ts)

    def make_feature_store(request_resolver: ReportResolver) -> FeatureStore:
        return FeatureStore(registry, udfs, request_resolver, offline, online, metrics=metrics)

    return AppBackend(
        registry=registry,
        store=store,
        payloads=payloads,
        metas=metas,
        online=online,
        offline=offline,
        make_storage_uri=make_storage_uri,
        raw_format="json",
        raw_compression="gzip",
        serialize_payload=_local_serialize_payload,
        make_feature_store=make_feature_store,
        events=events,
        status=ValkeyRequestStatusStore(
            connect_valkey(
                settings.valkey_host, settings.valkey_port, db=settings.valkey_db
            ),
            settings.request_status_ttl_s,
        ),
        # Request-scoped hybrid results share the status TTL: they expire together.
        results=ValkeyRequestResultStore(
            connect_valkey(
                settings.valkey_host, settings.valkey_port, db=settings.valkey_db
            ),
            settings.request_status_ttl_s,
        ),
        metadata=_PostgresMetadataRepo(pg),
        batch_status=ValkeyBatchJobStatusStore(
            connect_valkey(
                settings.valkey_host, settings.valkey_port, db=settings.valkey_db
            ),
            settings.request_status_ttl_s,
        ),
        batch_meta=_PostgresBatchMetadataRepo(pg),
        source_datasets=_PostgresSourceDatasetRepo(pg),
        metrics=metrics,
        readiness_probes=readiness_probes,
    )
