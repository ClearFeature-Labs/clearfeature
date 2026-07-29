# Known limitations (beta)

Honest list of what is gated, in-memory, deferred, or a client-side responsibility.
Nothing here contradicts a beta promise; each item states its impact and path forward.

## Gated / manual evidence (needs local services)

- **Postgres COPY/jsonb smoke is gated**: the real bulk-writer proof runs only with
  `FSP_POSTGRES_INTEGRATION=1` + a local Postgres (`tests/fs_core/stores/
  test_postgres_offline_store.py`); same for Kafka/Valkey/MinIO integration gates and
  `scripts/run_local_backend_smoke.sh`. The default suite and the acceptance runner are
  deliberately Docker-free.
- **20–30M-scale run is arithmetic/checklist only**: no local execution of a real 20M-item
  job; walkthrough 4 carries the tier-M arithmetic and the T1 trigger checklist instead.

## Deferred hardening (documented in the owning tasks)

- **In-memory debounce** : propagation debounce is per-process; a crash between
  observe and flush replays safely (idempotent) but re-does work. Durable/timed debounce
  is post-beta hardening.
- **F3 in the propagation daemon defers**: the beta daemon wires no model runner, so F3
  wave units are counted `skipped` (explicit, visible in wave accounting). Nightly F3
  batches call `compute_model_feature_batch` with their runner directly.
- **No value→source lineage index** : lineage resolves `report_refs` only from
  caller-supplied refs or a `manifest_id`; otherwise it returns an explicit
  `source_report_refs_not_available` gap. Auto-derivation is future lineage hardening.
- **fsctl process metrics**: publish/promote/rollback run in a separate CLI process, so
  the in-process metrics recorder does not count them; promotion **records** are the audit
  trail instead.
- **No DB connection pooling** in the local backend (per-store connections); adequate for
  tier S, revisit for sustained tier-M load.
- **Inline batch mode carries payloads in Kafka events by design** (alpha convenience,
  capped at 1000 items). The beta-scale path is the ref-only manifest job; inline stays
  for small ad-hoc batches only.

## Security baseline

- **API-key auth is the pilot baseline, not enterprise IAM**: fail-closed
  `Authorization: Bearer` with service/operator roles on both HTTP services,
  separate per-service key registries, authenticated model-service→API calls, docs
  disabled in secure mode (`docs/security/minimum_security.md`). Still external to
  the platform: **TLS termination and rate limiting (reverse proxy)**, secrets
  manager, OAuth/OIDC/SSO, mTLS, signed audit logs. Keys are env-delivered; rotation
  = overlapping keys + restart. The development bypass exists but is startup-gated
  to `FSP_ENVIRONMENT=development` and loudly logged.

## Resolved since beta

- **Historical availability**: the beta had no availability clock (calc_ts strictness
  only). Trusted ``available_at`` is supported end to end (PIT
  ``COALESCE(available_at, calc_ts)``); operator ingestion may backdate, online
  requests cannot; legacy rows keep the conservative fallback.
- **`metadata_write_status`** now truthfully transitions ``pending → written`` after
  the durable metadata projection (it previously stayed pending forever).

## Client-side responsibilities (by design)

- **No real MLflow runner**: the `ModelRunner` seam + digest verification are in place and
  tested with the deterministic fake; loading real artifacts from MLflow is a deployment
  concern (M4 constraints already encoded).
- **No production Grafana/Prometheus deployment and no alert routing**: O1 metrics exist
  (endpoint + instrumentation); the monitoring/alerting stack is the client's choice
. "Metrics exist **and alert**" therefore reads: metrics exist
  and are alert-*able* via the snapshot endpoint.
- **No hosted registry / real VCS approval API**: bundles + pointers are local filesystem;
  approvals are recorded from `--approved-by` input. Bank-grade approval *rules* are
  enforced; identity verification integrates with the client's forge post-beta.
- **pyarrow optional**: Parquet dataset export needs the optional dependency (JSONL/CSV
  work without it).

## Escalation

If a limitation above blocks a concrete client scenario, escalate to the platform owner
with the walkthrough number + failing expectation; do not work around a listed limitation
by disabling a guard (D9, pause, budget, lifecycle) — the guards are the product.
