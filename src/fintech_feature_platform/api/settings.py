"""Backend settings for the API, parsed from environment variables.

Simple env parsing only — no config framework, no pydantic-settings. ``backend``
selects the backend mode (``memory`` is the default; ``local`` is parsed here but
only wired in). The remaining fields are consulted only in local mode and
default to the local ``docker-compose.yml`` values.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

_BACKENDS = ("memory", "local")
_EVENTS = ("memory", "kafka")
_TRUE = {"true", "1", "yes", "on"}
_FALSE = {"false", "0", "no", "off"}

# Artifact-binding policy.
ENV_DEVELOPMENT = "development"
BINDING_LEGACY_COMPATIBLE = "legacy-compatible"
BINDING_REQUIRED = "required"
_BINDINGS = (BINDING_LEGACY_COMPATIBLE, BINDING_REQUIRED)
_BUNDLE_STAGES = ("live", "shadow")
_DEFAULT_ARTIFACT_MANIFEST = "/etc/clearfeature/feature-artifact.json"


@dataclass(frozen=True)
class Settings:
    backend: str = "memory"
    # Registry seam : path to the registry YAML and a "module:callable" UDF
    # provider. None (default) keeps the built-in demo registry + UDFs — fully backward
    # compatible. Set both to serve a different feature contract (e.g. the credit demo).
    registry_path: str | None = None
    udf_provider: str | None = None
    # Artifact-binding runtime verification. ``environment`` mirrors
    # ``FSP_ENVIRONMENT`` (only ``development`` is special). ``artifact_binding`` is the
    # policy; when a promoted bundle is configured (bundle_store + pointer_store + bundle_env)
    # the runtime resolves it and fail-closed-verifies any bound feature_artifact.
    environment: str = ENV_DEVELOPMENT
    artifact_binding: str = BINDING_LEGACY_COMPATIBLE
    # Per-process OBSERVABILITY port : one lightweight HTTP server
    # serving BOTH `/metrics` (Prometheus text) and `/ready` (role readiness).
    # 0 = DISABLED (the default): the process exposes neither endpoint. Env contract
    # with explicit precedence: FSP_OBSERVABILITY_PORT (primary) >
    # FSP_METRICS_PORT (retained compatibility fallback, same semantics) > 0.
    # Readiness and metrics are distinct CAPABILITIES sharing one deliberate port —
    # a deployment that wants readiness but no Prometheus simply never scrapes.
    observability_port: int = 0
    feature_artifact_manifest: str = _DEFAULT_ARTIFACT_MANIFEST
    bundle_store: str | None = None
    pointer_store: str | None = None
    bundle_env: str | None = None
    bundle_stage: str = "live"
    postgres_dsn: str = "postgresql://fsp:fsp_dev_password@localhost:5432/fsp"
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False
    minio_bucket: str = "raw-reports"
    valkey_host: str = "localhost"
    valkey_port: int = 6379
    valkey_db: int = 0
    # Event publishing: ``memory`` (default; no broker) or ``kafka``.
    events: str = "memory"
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_topic_prefix: str = "fp"
    kafka_client_id: str = "fintech-feature-platform"
    # Online worker consumer.
    kafka_consumer_group: str = "fsp-online-worker"
    kafka_auto_offset_reset: str = "earliest"
    kafka_poll_timeout_ms: int = 1000
    # Deployment-owned topic provisioning : desired
    # partition count and replication factor for the canonical fp.* topics. Applied by
    # api/kafka_init_runner.py (one-shot), never by API/worker processes. Partitions can
    # grow, never shrink. Production guidance: partitions ≈ 2× worker ceiling; RF=3 with
    # ≥3 brokers (local single-broker Redpanda must stay at 1).
    kafka_topic_partitions: int = 4
    kafka_replication_factor: int = 1
    # Offline writer consumer.
    kafka_offline_writer_group: str = "fsp-offline-writer"
    # Metadata writer consumer.
    kafka_metadata_writer_group: str = "fsp-metadata-writer"
    # Model score writer consumer.
    kafka_model_score_writer_group: str = "fsp-model-score-writer"
    # Reactive propagation worker consumer.
    kafka_propagation_worker_group: str = "fsp-propagation-worker"
    # Batch worker consumer + scope caps.
    kafka_batch_worker_group: str = "fsp-batch-worker"
    batch_max_chunk_size: int = 100
    # Inline scope stays conservatively capped (payloads travel in the Kafka event).
    batch_max_items: int = 1000
    # Dataset-scoped (manifest) jobs carry refs only + bulk-append offline per chunk, so
    # they allow far larger item counts. Not a proven throughput number, just no tiny cap.
    batch_max_manifest_items: int = 5_000_000
    # Process-level Postgres pool size : the local backend
    # builds one psycopg_pool.ConnectionPool per process with this max size. 0 = the
    # legacy fresh-connection-per-operation behavior (the documented rollback lever).
    db_pool_size: int = 4
    # Per-runtime DB pool budgets are realized in DEPLOYMENT, not here: each compose
    # service sets FSP_DB_POOL_SIZE to its runtime's budget (api=10, batch=4, ...) —
    # the process itself cannot know which runtime it is. (The four never-consumed
    # FSP_<role>_DB_POOL_SIZE settings were removed in.)
    # Batch pause + rate limits (online SLO wins; batch is throttled/preemptible).
    batch_rate_limit_items_per_sec: float = 0.0   # 0 = unlimited
    batch_rate_limit_chunks_per_sec: float = 0.0  # 0 = unlimited
    batch_pause_enabled: bool = False
    batch_max_consumer_lag: int = 0
    batch_pause_sleep_ms: int = 500
    # Mode-2 guarded online refresh from batch (token-bucket + D9 write guard).
    batch_online_refresh_enabled: bool = False
    batch_online_refresh_tokens_per_sec: float = 0.0  # 0 = unlimited
    batch_online_refresh_burst: float = 0.0
    batch_online_refresh_max_features_per_chunk: int = 0  # 0 = unbounded per chunk
    # Attempt-limit DLQ : Nth failed attempt is dead-lettered.
    kafka_max_attempts: int = 5
    # Request status store TTL; short-lived operational state.
    request_status_ttl_s: int = 86400
    # Hybrid feature-request wait bounds.
    hybrid_max_wait_ms: int = 5000
    hybrid_poll_ms: int = 50
    # Absolute-deadline ceiling for online requests : the request's
    # deadline_ms is clamped to [1, this] before expires_at = event_ts + deadline_ms.
    # A sanity guardrail against absurd deadlines, not a full admission controller.
    online_max_deadline_ms: int = 60000


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    env = os.environ if env is None else env
    defaults = Settings()

    backend = env.get("FSP_BACKEND", defaults.backend)
    if backend not in _BACKENDS:
        raise ValueError(
            f"unknown FSP_BACKEND {backend!r}; expected one of {_BACKENDS}"
        )

    events = env.get("FSP_EVENTS", defaults.events)
    if events not in _EVENTS:
        raise ValueError(f"unknown FSP_EVENTS {events!r}; expected one of {_EVENTS}")

    artifact_binding = env.get("FSP_ARTIFACT_BINDING", defaults.artifact_binding)
    if artifact_binding not in _BINDINGS:
        raise ValueError(
            f"unknown FSP_ARTIFACT_BINDING {artifact_binding!r}; expected one of {_BINDINGS}"
        )
    bundle_stage = env.get("FSP_BUNDLE_STAGE", defaults.bundle_stage)
    if bundle_stage not in _BUNDLE_STAGES:
        raise ValueError(
            f"unknown FSP_BUNDLE_STAGE {bundle_stage!r}; expected one of {_BUNDLE_STAGES}"
        )

    return Settings(
        backend=backend,
        # Empty string counts as unset (compose passes `${FSP_REGISTRY_PATH:-}`).
        registry_path=env.get("FSP_REGISTRY_PATH") or None,
        udf_provider=env.get("FSP_UDF_PROVIDER") or None,
        environment=env.get("FSP_ENVIRONMENT", defaults.environment),
        artifact_binding=artifact_binding,
        observability_port=_parse_int(
            # Precedence: FSP_OBSERVABILITY_PORT wins; FSP_METRICS_PORT is the
            # backward-compatible fallback. Empty
            # string counts as unset (compose may pass `${FSP_OBSERVABILITY_PORT:-}`).
            env.get("FSP_OBSERVABILITY_PORT") or env.get("FSP_METRICS_PORT") or None,
            defaults.observability_port,
        ),
        feature_artifact_manifest=env.get(
            "FSP_FEATURE_ARTIFACT_MANIFEST", defaults.feature_artifact_manifest
        ),
        bundle_store=env.get("FSP_BUNDLE_STORE") or None,
        pointer_store=env.get("FSP_POINTER_STORE") or None,
        bundle_env=env.get("FSP_BUNDLE_ENV") or None,
        bundle_stage=bundle_stage,
        postgres_dsn=env.get("FSP_POSTGRES_DSN", defaults.postgres_dsn),
        minio_endpoint=env.get("FSP_MINIO_ENDPOINT", defaults.minio_endpoint),
        minio_access_key=env.get("FSP_MINIO_ACCESS_KEY", defaults.minio_access_key),
        minio_secret_key=env.get("FSP_MINIO_SECRET_KEY", defaults.minio_secret_key),
        minio_secure=_parse_bool(env.get("FSP_MINIO_SECURE"), defaults.minio_secure),
        minio_bucket=env.get("FSP_MINIO_BUCKET", defaults.minio_bucket),
        valkey_host=env.get("FSP_VALKEY_HOST", defaults.valkey_host),
        valkey_port=_parse_int(env.get("FSP_VALKEY_PORT"), defaults.valkey_port),
        valkey_db=_parse_int(env.get("FSP_VALKEY_DB"), defaults.valkey_db),
        events=events,
        kafka_bootstrap_servers=env.get(
            "FSP_KAFKA_BOOTSTRAP_SERVERS", defaults.kafka_bootstrap_servers
        ),
        kafka_topic_prefix=env.get("FSP_KAFKA_TOPIC_PREFIX", defaults.kafka_topic_prefix),
        kafka_client_id=env.get("FSP_KAFKA_CLIENT_ID", defaults.kafka_client_id),
        kafka_consumer_group=env.get(
            "FSP_KAFKA_CONSUMER_GROUP", defaults.kafka_consumer_group
        ),
        kafka_auto_offset_reset=env.get(
            "FSP_KAFKA_AUTO_OFFSET_RESET", defaults.kafka_auto_offset_reset
        ),
        kafka_topic_partitions=_parse_int(
            env.get("FSP_KAFKA_TOPIC_PARTITIONS"), defaults.kafka_topic_partitions
        ),
        kafka_replication_factor=_parse_int(
            env.get("FSP_KAFKA_REPLICATION_FACTOR"), defaults.kafka_replication_factor
        ),
        kafka_poll_timeout_ms=_parse_int(
            env.get("FSP_KAFKA_POLL_TIMEOUT_MS"), defaults.kafka_poll_timeout_ms
        ),
        kafka_offline_writer_group=env.get(
            "FSP_KAFKA_OFFLINE_WRITER_GROUP", defaults.kafka_offline_writer_group
        ),
        kafka_metadata_writer_group=env.get(
            "FSP_KAFKA_METADATA_WRITER_GROUP", defaults.kafka_metadata_writer_group
        ),
        kafka_model_score_writer_group=env.get(
            "FSP_KAFKA_MODEL_SCORE_WRITER_GROUP", defaults.kafka_model_score_writer_group
        ),
        kafka_propagation_worker_group=env.get(
            "FSP_KAFKA_PROPAGATION_WORKER_GROUP",
            defaults.kafka_propagation_worker_group,
        ),
        kafka_batch_worker_group=env.get(
            "FSP_KAFKA_BATCH_WORKER_GROUP", defaults.kafka_batch_worker_group
        ),
        batch_max_chunk_size=_parse_int(
            env.get("FSP_BATCH_MAX_CHUNK_SIZE"), defaults.batch_max_chunk_size
        ),
        batch_max_items=_parse_int(
            env.get("FSP_BATCH_MAX_ITEMS"), defaults.batch_max_items
        ),
        batch_max_manifest_items=_parse_int(
            env.get("FSP_BATCH_MAX_MANIFEST_ITEMS"), defaults.batch_max_manifest_items
        ),
        db_pool_size=_parse_int(env.get("FSP_DB_POOL_SIZE"), defaults.db_pool_size),
        batch_rate_limit_items_per_sec=_parse_float(
            env.get("FSP_BATCH_RATE_LIMIT_ITEMS_PER_SEC"),
            defaults.batch_rate_limit_items_per_sec,
        ),
        batch_rate_limit_chunks_per_sec=_parse_float(
            env.get("FSP_BATCH_RATE_LIMIT_CHUNKS_PER_SEC"),
            defaults.batch_rate_limit_chunks_per_sec,
        ),
        batch_pause_enabled=_parse_bool(
            env.get("FSP_BATCH_PAUSE_ENABLED"), defaults.batch_pause_enabled
        ),
        batch_max_consumer_lag=_parse_int(
            env.get("FSP_BATCH_MAX_CONSUMER_LAG"), defaults.batch_max_consumer_lag
        ),
        batch_pause_sleep_ms=_parse_int(
            env.get("FSP_BATCH_PAUSE_SLEEP_MS"), defaults.batch_pause_sleep_ms
        ),
        batch_online_refresh_enabled=_parse_bool(
            env.get("FSP_BATCH_ONLINE_REFRESH_ENABLED"),
            defaults.batch_online_refresh_enabled,
        ),
        batch_online_refresh_tokens_per_sec=_parse_float(
            env.get("FSP_BATCH_ONLINE_REFRESH_TOKENS_PER_SEC"),
            defaults.batch_online_refresh_tokens_per_sec,
        ),
        batch_online_refresh_burst=_parse_float(
            env.get("FSP_BATCH_ONLINE_REFRESH_BURST"),
            defaults.batch_online_refresh_burst,
        ),
        batch_online_refresh_max_features_per_chunk=_parse_int(
            env.get("FSP_BATCH_ONLINE_REFRESH_MAX_FEATURES_PER_CHUNK"),
            defaults.batch_online_refresh_max_features_per_chunk,
        ),
        kafka_max_attempts=_parse_int(
            env.get("FSP_KAFKA_MAX_ATTEMPTS"), defaults.kafka_max_attempts
        ),
        request_status_ttl_s=_parse_int(
            env.get("FSP_REQUEST_STATUS_TTL_S"), defaults.request_status_ttl_s
        ),
        hybrid_max_wait_ms=_parse_int(
            env.get("FSP_HYBRID_MAX_WAIT_MS"), defaults.hybrid_max_wait_ms
        ),
        hybrid_poll_ms=_parse_int(env.get("FSP_HYBRID_POLL_MS"), defaults.hybrid_poll_ms),
        online_max_deadline_ms=_parse_int(
            env.get("FSP_ONLINE_MAX_DEADLINE_MS"), defaults.online_max_deadline_ms
        ),
    )


def _parse_bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    raise ValueError(f"invalid boolean value {raw!r}")


def _parse_int(raw: str | None, default: int) -> int:
    if raw is None:
        return default
    return int(raw)


def _parse_float(raw: str | None, default: float) -> float:
    if raw is None:
        return default
    return float(raw)
