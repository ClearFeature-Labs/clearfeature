# API Contracts

This document defines the target API contracts for the Kafka-first MVP.

These contracts are implementation targets. Not every endpoint must be implemented in the first task, but new implementation work must not introduce APIs that contradict this document.

## Current as-built contract (read this first)

Everything below in this file predates authentication and the availability clock;
where it conflicts, the following rules and the executable inventory win
(`ENDPOINT_POLICY` in `api/app.py` / `examples/.../model_service.py`, exported in the
audit):

- **Observability endpoints **: `GET /health` (liveness), `GET /ready`
  (role readiness, 200/503, bounded categories only), per-process `GET /metrics`
  (Prometheus text on `FSP_OBSERVABILITY_PORT`; legacy fallback `FSP_METRICS_PORT`),
  and the legacy operator JSON snapshot `GET /v1/observability/metrics`. Contract:
  `docs/21_observability_contract.md`.
- **Online deadline & retry contract **: `deadline_ms` (default **1000**, clamped
  to `FSP_ONLINE_MAX_DEADLINE_MS` = 60000) becomes an absolute `expires_at` stamped on
  the event; workers reason only from `expires_at`. A request finishing as
  `deadline_expired` performed **no compute and no online/offline write**; the event
  was committed, the outcome is terminal and truthful, and the request will never
  complete later. **Retry is safe**: submit a new request (new `request_id`; reuse
  your own `correlation_id` if you track one) — D9 freshness CAS + offline dedup make
  overlapping deliveries idempotent. Warm p50 is ~60 ms / p99 ~110 ms, so the 1 s
  default has ample steady-state headroom; expect a transient `deadline_expired`
  window of seconds during worker restarts/consumer rebalances (graceful shutdown
  keeps it short). Recommended client pattern: bounded retries with backoff, e.g.
  3 attempts at 0 s / 1 s / 5 s, treating `deadline_expired` (and only it) as
  retryable-by-design. Do NOT blindly retry generic HTTP 500s.
- **Authentication **: every endpoint except the public probes `GET /health` and
  `GET /ready` requires
  `Authorization: Bearer <api-key>`. Roles: `service` (feature reads/consistency,
  feature-request submit/compute/status, model scores, credit decision) and
  `operator` (superset: + metrics, values-free lineage, batch submission/status,
  source ingestion/import + manifests, training-dataset build). Missing/invalid key
  → 401; insufficient role → 403. Secure mode is the default and fail-closed; docs
  (`/docs`, `/openapi.json`) are disabled in it. Guide:
  `docs/security/minimum_security.md`.
- **Compute model**: request-triggered, event-driven computation (not continuous
  stream ingestion). Scaled batch jobs are manifest-scoped
  (`scope.type=source_dataset_manifest`); the client/DWH extraction selects the
  cohort, ingestion records it. One documented exception to "raw payloads are never
  sent through Kafka": **inline batch jobs (≤ 1,000 items) carry payloads in the
  chunk event** (alpha convenience; the scale path is ref-only). "Kafka" throughout
  this document means the Kafka-compatible protocol (topics, consumer groups,
  at-least-once delivery); the deployed broker is **Redpanda** — there is no Apache
  Kafka container.
- **Time contract **: four clocks —
  `report_ts` (business/as-of, required), `available_at` (availability to the bank:
  optional trusted field on operator ingestion rows and inline batch sources;
  **server-stamped accept time** on online requests — service callers cannot
  backdate), `created_at`/ingested, `calc_ts`. PIT eligibility:
  `data_ts <= obs - safety_gap AND COALESCE(available_at, calc_ts) <= obs`.
  Lineage answers `available_at` / `availability_source` /
  `availability_effective`, values-free.
- **Status truthfulness **: `metadata_write_status` transitions
  `pending → written` once the metadata writer durably commits the request's
  terminal projection. **PostgreSQL is the durable source of truth; Valkey is
  operational and may lag** — `GET /v1/feature-requests/{id}` reconciles a pending
  status against the durable projection and read-repairs monotonically, so a lost
  operational update can never leave a permanent false `pending`.
- **Availability corrections are audited **: every accepted trusted
  `available_at` correction writes one append-only row to
  `raw_report_availability_changes` (old/new values + provenance + origin ids),
  atomically with the metadata update; replays add nothing.
- Rejected ingestion rows are persisted in `source_dataset_items` (status
  `rejected` + error); structurally poison Kafka messages go to the non-lossy
  `fp.dlq` topic; per-item batch failures land in
  `batch_chunks.first_errors_json`.

## Core Rules

The active MVP architecture is Kafka-first:

```text
Feature API
  -> stores raw reports in MinIO/S3
  -> publishes self-contained Kafka compute events
  -> workers compute features
  -> Valkey is written first
  -> Postgres metadata and offline feature history are written asynchronously
```

Important rules:

* `report_ref` is the public raw-report identifier.
* `object_key` / `storage_uri` is internal platform metadata.
* Public API responses must not expose `storage_uri` by default.
* Kafka events carry self-contained report descriptors so workers do not need Postgres to locate raw reports.
* Raw payloads are never sent through Kafka.
* Online compute does not wait for Postgres metadata writes.
* Online feature writes are Valkey-first.
* Offline history and metadata are asynchronous, retryable, and replayable.
* Synchronous legacy endpoints may exist temporarily for compatibility, but they are not the target Kafka-first MVP path.

---

## 1. Feature Requests API

### 1.1 POST /v1/feature-requests

Purpose:

Submit an asynchronous online feature computation request.

The caller sends an entity key, requested feature groups or features, and raw reports or existing `report_ref` values. The platform stores new raw reports in MinIO/S3, builds a self-contained Kafka compute event, publishes it, and returns `accepted`.

This endpoint is the default entry point for Kafka-first online feature computation.

**Feature groups.** `requested_feature_groups` is now expanded by the Feature Planner: each group is defined per `FeatureView` in the registry (`feature_groups: {group_name: [feature, …]}`) and expands to its explicit output features, merged with any explicit `requested_features` (deduped; explicit first, then group order). The request is validated **before publish** — an unknown view/feature/group returns **400** and nothing is stored or published. The Kafka event carries the **raw** `requested_features` + `requested_feature_groups`; the worker re-plans from the registry. Dependencies are computed by `ComputeCore` as needed but are **not materialized** in V1 (only the requested output features are written online/offline); reusable-dependency materialization is deferred. `/v1/features/latest` still takes explicit feature names only.

Request example with inline reports:

```json
{
  "entity_type": "application",
  "entity_key": {
    "user_id": "123",
    "application_id": "A1"
  },
  "requested_feature_groups": [
    "pd_model_input_v1"
  ],
  "reports": [
    {
      "source_name": "bureau",
      "schema_version": "v1",
      "report_ts": "2026-06-27T10:00:00Z",
      "payload": {
        "example": "large bureau report"
      }
    },
    {
      "source_name": "application_form",
      "schema_version": "v1",
      "report_ts": "2026-06-27T10:00:05Z",
      "payload": {
        "age": 35,
        "region": "Tashkent"
      }
    }
  ],
  "write_policy": "online_first",
  "priority": "online",
  "deadline_ms": 1000,
  "idempotency_key": "application:A1:pd_model_input_v1:request_hash"
}
```

Request example with existing reports:

```json
{
  "entity_type": "application",
  "entity_key": {
    "user_id": "123",
    "application_id": "A1"
  },
  "requested_feature_groups": [
    "pd_model_input_v1"
  ],
  "reports": [
    {
      "report_ref": "rep_bureau_123"
    },
    {
      "report_ref": "rep_application_form_456"
    }
  ],
  "write_policy": "online_first",
  "priority": "online",
  "deadline_ms": 1000,
  "idempotency_key": "application:A1:pd_model_input_v1:request_hash"
}
```

Response example:

```json
{
  "request_id": "freq_01HYX2C4P2ZK9G7R8B2N",
  "job_id": "job_01HYX2C4P2ZK9G7R8B2N",
  "status": "accepted",
  "status_url": "/v1/feature-requests/freq_01HYX2C4P2ZK9G7R8B2N",
  "report_refs": [
    "rep_bureau_123",
    "rep_application_form_456"
  ]
}
```

Side effects:

1. New raw reports are compressed and stored in MinIO/S3.
2. `report_ref` values are generated or resolved.
3. A self-contained Kafka event is published to `fp.feature-compute.online`.
4. Request status is initialized in the low-latency status store.
5. Postgres metadata is written asynchronously by the Metadata Writer.

Acceptance boundary:

```text
The API may return accepted only after:
  MinIO/S3 writes succeeded
  AND Kafka publish was acknowledged
```

Postgres metadata acknowledgement is not required before returning `accepted`.

Kafka event must include report descriptors:

```json
{
  "report_ref": "rep_bureau_123",
  "source_name": "bureau",
  "schema_version": "v1",
  "report_ts": "2026-06-27T10:00:00Z",
  "object_key": "raw-reports/bureau/2026/06/27/rep_bureau_123.json.gz",
  "content_hash": "sha256:...",
  "size_bytes": 1430000,
  "compression": "gzip",
  "format": "json"
}
```

`object_key` is internal event metadata. It is not a public API contract.
`content_hash` is sha256 over the uncompressed payload bytes as received.

Request rules:

* At most one report per `source_name` per request; duplicates are rejected with 400.
* `deadline_ms` is measured from the published event's `event_ts`
  (`expires_at = event_ts + deadline_ms`); on expiry the worker publishes
  `deadline_expired` and does not write Valkey.
* If online consumer lag implies the deadline cannot be met, the API may reject
  with 429 (admission control) or degrade a hybrid request to pending, instead
  of accepting work that will expire in the queue.

---

### 1.2 POST /v1/feature-requests/compute

Purpose:

Submit a hybrid online feature computation request.

The endpoint behaves like `/v1/feature-requests`, but waits briefly for the online worker result. If the result is ready within `wait_timeout_ms`, the endpoint returns computed features. Otherwise it returns `accepted` or `pending` with `request_id`.

Request example:

```json
{
  "entity_type": "application",
  "entity_key": {
    "user_id": "123",
    "application_id": "A1"
  },
  "requested_feature_groups": [
    "pd_model_input_v1"
  ],
  "reports": [
    {
      "source_name": "bureau",
      "schema_version": "v1",
      "report_ts": "2026-06-27T10:00:00Z",
      "payload": {
        "example": "large bureau report"
      }
    }
  ],
  "write_policy": "online_first",
  "priority": "online",
  "deadline_ms": 1000,
  "wait_timeout_ms": 700,
  "idempotency_key": "application:A1:pd_model_input_v1:request_hash"
}
```

Completed response example:

```json
{
  "request_id": "freq_01HYX2C4P2ZK9G7R8B2N",
  "job_id": "job_01HYX2C4P2ZK9G7R8B2N",
  "status": "completed",
  "entity_type": "application",
  "entity_key": {
    "user_id": "123",
    "application_id": "A1"
  },
  "feature_view": "pd_model_input",
  "view_version": 1,
  "features": {
    "bureau_overdue_days_max:v1": {
      "value": 12,
      "data_ts": "2026-06-27T10:00:00Z",
      "calc_ts": "2026-06-27T10:00:01Z",
      "feature_version": 1
    },
    "income_card_avg_3m:v1": {
      "value": 3500000,
      "data_ts": "2026-06-27T10:00:00Z",
      "calc_ts": "2026-06-27T10:00:01Z",
      "feature_version": 1
    }
  },
  "missing_features": [],
  "stale_features": [],
  "online_write_status": "written",
  "offline_write_status": "pending"
}
```

Completed semantics:

* `completed` returns the request-scoped computed values from this request's
  `FeatureWriteSet` — never a re-read of Valkey latest.
* `online_write_status = skipped_stale` is still `completed`: the caller
  receives the values computed from its own reports while the online store
  keeps the fresher vector. Request-scoped results and `/v1/features/latest`
  are different contracts; decisions must be made on the submitted reports.

Pending response example:

```json
{
  "request_id": "freq_01HYX2C4P2ZK9G7R8B2N",
  "job_id": "job_01HYX2C4P2ZK9G7R8B2N",
  "status": "pending",
  "status_url": "/v1/feature-requests/freq_01HYX2C4P2ZK9G7R8B2N"
}
```

Important:

Hybrid waiting must use the low-latency request status/result store, not Postgres metadata.

---

### 1.3 GET /v1/feature-requests/{request_id}

Purpose:

Return feature request status and write status.

Response example:

```json
{
  "request_id": "freq_01HYX2C4P2ZK9G7R8B2N",
  "job_id": "job_01HYX2C4P2ZK9G7R8B2N",
  "status": "completed",
  "entity_type": "application",
  "entity_key": {
    "user_id": "123",
    "application_id": "A1"
  },
  "requested_feature_groups": [
    "pd_model_input_v1"
  ],
  "online_write_status": "written",
  "offline_write_status": "pending",
  "metadata_write_status": "pending",
  "created_at": "2026-06-27T10:00:00Z",
  "started_at": "2026-06-27T10:00:00Z",
  "finished_at": "2026-06-27T10:00:01Z",
  "error": null
}
```

Possible request statuses:

```text
accepted
queued
running
completed
pending
failed
deadline_expired
cancelled
```

Possible online write statuses:

```text
written
skipped_stale
failed_retryable
disabled
```

Possible offline write statuses:

```text
pending
written
retrying
failed_dlq
disabled
```

Status store semantics:

* The worker writes the terminal status/result to the low-latency status store
  only after downstream FeatureWriteSet/status events are acknowledged, so
  `completed` implies the offline/audit write is durably enqueued.
* Status entries have an explicit TTL (>= 15 minutes). This endpoint reads the
  status store first and falls back to the Postgres metadata projection after
  TTL. The projection is eventually consistent and may briefly lag; callers
  must tolerate `metadata_write_status = pending`.

---

## 2. Latest Features API

### 2.1 POST /v1/features/latest

Purpose:

Read latest online features from Valkey.

This is the default public contract for low-latency feature serving. Direct Valkey reads may be used only as a trusted internal optimization, not as the public API contract.

Request example:

```json
{
  "entity_type": "application",
  "entity_key": {
    "user_id": "123",
    "application_id": "A1"
  },
  "feature_view": "pd_model_input",
  "view_version": 1,
  "features": [
    "bureau_overdue_days_max:v1",
    "income_card_avg_3m:v1",
    "pd_base_age:v1"
  ],
  "max_staleness_ms": 86400000
}
```

Response example:

```json
{
  "entity_type": "application",
  "entity_key": {
    "user_id": "123",
    "application_id": "A1"
  },
  "feature_view": "pd_model_input",
  "view_version": 1,
  "features": {
    "bureau_overdue_days_max:v1": {
      "value": 12,
      "data_ts": "2026-06-27T10:00:00Z",
      "calc_ts": "2026-06-27T10:00:01Z",
      "feature_version": 1,
      "freshness_status": "fresh"
    },
    "income_card_avg_3m:v1": {
      "value": 3500000,
      "data_ts": "2026-06-27T10:00:00Z",
      "calc_ts": "2026-06-27T10:00:01Z",
      "feature_version": 1,
      "freshness_status": "fresh"
    }
  },
  "missing_features": [],
  "stale_features": [],
  "freshness_status": "fresh"
}
```

If some values are missing or stale, the API must return that explicitly:

```json
{
  "entity_type": "application",
  "entity_key": {
    "user_id": "123",
    "application_id": "A1"
  },
  "feature_view": "pd_model_input",
  "view_version": 1,
  "features": {},
  "missing_features": [
    "bureau_overdue_days_max:v1"
  ],
  "stale_features": [
    {
      "feature": "income_card_avg_3m:v1",
      "data_ts": "2026-06-20T10:00:00Z",
      "max_staleness_ms": 86400000
    }
  ],
  "freshness_status": "missing_or_stale"
}
```

The decision service decides fallback behavior:

```text
accept
reject
manual review
fallback model
trigger refresh
```

---

## 3. Model Score Writeback API

### 3.1 POST /v1/model-scores

Purpose:

Write externally computed model scores or decision results back to the Feature Platform as features.

The Feature Platform does not run online model cascades. ML/Decision services own online model inference, fallback logic, and decision policy. When a score must be reused, audited, used for training, or served later, it is written back through this API.

Each target feature must be **registered in the view with `kind: model_score`** (externally written, never computed by `ComputeCore`; a compute request targeting one is rejected). Writing to a computed (`udf`) feature is rejected. `idempotency_key` is required and becomes the `score_write_id` (and the `FeatureWriteSet` `run_id`); online freshness is `data_ts`-based CAS; offline history is persisted by the existing Offline Writer.

`data_ts` for a score must be the max `data_ts` of the model's input features
(data freshness), not the wall-clock decision time — otherwise CAS ordering
between recomputed scores and their inputs breaks, and PIT joins over score
history report wrong availability.

Request example:

```json
{
  "entity_type": "application",
  "entity_key": {"user_id": "123", "application_id": "A1"},
  "view": "user_credit_risk",
  "view_version": 1,
  "idempotency_key": "model_score:pd_model:v4:application:A1:request_hash",
  "write_online": true,
  "write_offline": true,
  "scores": [
    {
      "feature": "pd_score",
      "value": 0.037,
      "data_ts": "2026-06-27T10:00:00Z",
      "model_name": "pd_model",
      "model_version": "v4",
      "source_request_id": "freq_01HYX2C4P2ZK9G7R8B2N"
    }
  ]
}
```

Response (`202 Accepted`):

```json
{"score_write_id": "model_score:pd_model:v4:application:A1:request_hash", "status": "accepted"}
```

Validation (all `400`, nothing published) unless noted: unknown view/view_version; unknown feature; feature not `kind="model_score"`; both write flags false; empty scores. Missing required fields (`data_ts`, `model_name`, `model_version`, `idempotency_key`) are schema errors (`422`).

Side effects (asynchronous; the endpoint only publishes):

1. Publishes `ModelScoreWriteRequested` to `fp.model-score.write`. No synchronous store write.
2. The score writer writes online (Valkey) first (CAS by `data_ts`) when `write_online`.
3. It republishes `FeatureOfflineWriteRequested` so the Offline Writer persists history when `write_offline`.
4. The Metadata Writer audits the write (model lineage only — **no score values**).

The platform does not execute the model. There is no `GET /v1/model-scores/{id}` in V1.

---

## 4. Batch Jobs API

### 4.1 POST /v1/batch/jobs

Purpose:

Submit an async batch feature computation over a bounded scope.

**V1 (implemented).** Scope is `inline` only: each item carries its `entity_key` + inline raw
sources (same shape as `/v1/features` inline sources). The API validates `view`/`view_version`
+ planner-expands `requested_features`/`requested_feature_groups`, deterministically slices
`scope.items` into chunks (`chunk_id = {job_id}:{index}`), creates a job status, and publishes
one `BatchChunkRequested` per chunk to `fp.feature-compute.batch`. **No synchronous compute.**
`idempotency_key` is required and becomes `job_id`. Offline write is **always** on
(offline-first); `write_online` is optional (default false). `chunk_size` is capped by
`FSP_BATCH_MAX_CHUNK_SIZE`; total items by `FSP_BATCH_MAX_ITEMS`.

Request example (V1):

```json
{
  "view": "user_credit_risk",
  "view_version": 1,
  "requested_feature_groups": ["pd_model_input_v1"],
  "scope": {
    "type": "inline",
    "items": [
      {
        "entity_type": "application",
        "entity_key": {"user_id": "u1", "application_id": "A1"},
        "inline_sources": {
          "credit_report": {
            "report_type": "credit_report",
            "report_ts": "2026-01-01T00:00:00Z",
            "payload": {"declared_income": 100000, "monthly_obligations": 700000}
          }
        }
      }
    ]
  },
  "write_online": false,
  "chunk_size": 100,
  "idempotency_key": "batch-2026-01-01"
}
```

Response (`202 Accepted`):

```json
{"job_id": "batch-2026-01-01", "status": "accepted", "chunk_count": 1, "total_items": 1}
```

Validation (all `400`, nothing published): unsupported scope type, empty scope, total items
over cap, `chunk_size` out of `1..cap`, unknown view/version, unknown feature/group. Missing
`idempotency_key` is a schema error (`422`). A partial publish failure marks the job
`publish_failed` and returns `503` (re-submit with the same `idempotency_key` is idempotent
via stable `chunk_id` + offline dedup + online CAS).

The batch worker computes each chunk item via the shared `persist_and_compute` (same
`ComputeCore` semantics as online), with per-item error accounting; it commits the chunk
offset only after processing (structural/invalid → DLQ; infra failure → replay).

**Deferred:** `report_refs`/date-range/object-store/SQL scopes; job types beyond inline
feature compute; a durable Postgres job/chunk projection; `write_offline=false`.

---

### 4.2 GET /v1/batch/jobs/{job_id}

Purpose:

Return operational batch job status (Valkey/InMemory; per-chunk state; no feature values).

Response example (V1):

```json
{
  "job_id": "batch-2026-01-01",
  "status": "completed_with_errors",
  "view": "user_credit_risk",
  "view_version": 1,
  "requested_features": ["declared_income"],
  "requested_feature_groups": ["pd_model_input_v1"],
  "total_items": 250,
  "chunk_count": 3,
  "write_online": false,
  "completed_chunks": 3,
  "failed_chunks": 0,
  "failed_items": 2,
  "chunks": {"batch-2026-01-01:0": {"status": "completed", "ok_items": 100, "failed_items": 0}},
  "created_at": "2026-06-27T10:00:00Z",
  "updated_at": "2026-06-27T10:00:05Z",
  "finished_at": "2026-06-27T10:00:05Z",
  "error_summary": {}
}
```

`completed_chunks`/`failed_chunks`/`failed_items` are derived from per-chunk state (replay-safe).
Job status: `accepted` → `running` → `completed` / `completed_with_errors` / `failed` (or
`publish_failed`). `404` for an unknown/expired `job_id`.

---

## 5. Raw Report Metadata API

### 5.1 GET /v1/raw-reports/{report_ref}/meta

Purpose:

Return raw report metadata without loading raw payload.

This endpoint reads the asynchronous Postgres metadata projection. It is not required for online workers to compute features.

Response example:

```json
{
  "report_ref": "rep_bureau_123",
  "source_name": "bureau",
  "schema_version": "v1",
  "entity_type": "application",
  "entity_key": {
    "user_id": "123",
    "application_id": "A1"
  },
  "report_ts": "2026-06-27T10:00:00Z",
  "payload_size_bytes": 1430000,
  "content_hash": "sha256:...",
  "format": "json",
  "compression": "gzip",
  "metadata_status": "written"
}
```

Important:

This public endpoint does not return `storage_uri` by default.

---

## 6. Deferred APIs

The following APIs are useful but not required for the first Kafka-first implementation slice.

### 6.1 Dataset Build API

```text
POST /v1/datasets/build
GET /v1/datasets/{dataset_id}/manifest
```

Purpose:

Build point-in-time training datasets from offline history.

Rule:

```text
feature.data_ts <= observation_ts - safety_gap
```

---

### 6.2 Consistency Checks API

```text
POST /v1/consistency-checks
GET /v1/consistency-checks/{check_id}
```

Purpose:

Compare online latest values with offline latest values for sampled entities.

---

### 6.3 Catalog / Admin APIs

```text
GET /v1/catalog/views
GET /v1/catalog/views/{view}
GET /v1/catalog/features/{feature}
GET /v1/admin/jobs
GET /v1/admin/dlq
```

Purpose:

Expose registry, feature views, jobs, DLQ, and operational metadata.

These APIs are deferred. Do not build a UI or complex admin surface in the MVP implementation slice.

---

## 7. Legacy / Compatibility Endpoints

**Removed.** The legacy synchronous compute routes no longer exist:

```text
POST /v1/raw-reports             # removed  — use POST /v1/feature-requests
POST /v1/features/compute        # removed  — use the Kafka-first request API
POST /v1/features/compute-direct # removed  — use the Kafka-first request API
```

Online compute is Kafka-first only (`POST /v1/feature-requests[/compute]`). The inline
persist+compute helper (`api/direct_compute.persist_and_compute`) survives as an **internal
library** for the raw-JSON backfill / table import runner and the acceptance script — it is
not an HTTP endpoint.

The following older API names are not the Kafka-first target contract either:

```text
POST /stream/features
GET /features
POST /features/vector
POST /batch/run
GET /batch/jobs/{job_id}
GET /catalog/*
```

New implementation work should target the `/v1/...` APIs defined in this document. If a
legacy endpoint is kept temporarily, it must be documented as compatibility-only and must
not introduce a second production compute path.

---

## 8. Error Response Format

Use a consistent error structure:

```json
{
  "error_code": "FEATURE_NOT_FOUND",
  "message": "Feature final_pd is not defined in view pd_model_input:v1",
  "details": {
    "feature_view": "pd_model_input",
    "view_version": 1,
    "feature": "final_pd"
  },
  "request_id": "freq_01HYX2C4P2ZK9G7R8B2N"
}
```

Common error codes:

```text
INVALID_REQUEST
INVALID_ENTITY_KEY
INVALID_REPORT_DESCRIPTOR
REPORT_PAYLOAD_TOO_LARGE
REPORT_NOT_FOUND
FEATURE_GROUP_NOT_FOUND
FEATURE_NOT_FOUND
MISSING_REQUIRED_SOURCE
DEPENDENCY_CYCLE
KAFKA_PUBLISH_FAILED
ONLINE_WRITE_FAILED
OFFLINE_WRITE_FAILED
DEADLINE_EXPIRED
INTERNAL_ERROR
```

---

## 9. Status Codes

Recommended status codes:

```text
200 OK
  synchronous/hybrid request completed

202 Accepted
  async request accepted or hybrid request still pending

400 Bad Request
  invalid request shape or validation failure

404 Not Found
  report_ref, request_id, job_id, feature, or feature group not found

409 Conflict
  idempotency conflict or incompatible duplicate request

413 Payload Too Large
  raw report payload exceeds configured limit

422 Unprocessable Entity
  valid JSON but semantically invalid feature request

429 Too Many Requests
  admission control: online queue lag makes the requested deadline unachievable

500 Internal Server Error
  unexpected server-side failure

503 Service Unavailable
  required dependency unavailable, for example Kafka or MinIO
```
