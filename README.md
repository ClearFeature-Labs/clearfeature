# ClearFeature

**ClearFeature is a lightweight, open-core feature platform for credit-risk, fraud,
scoring and decision systems: one shared Python compute core serves the same feature
code online and in batch, with point-in-time correctness and artifact-bound,
fail-closed deployments.**

## The problem it solves

Teams building decision systems duplicate feature logic between batch SQL and online
services, leak future data into training sets, and cannot prove that the feature code
they tested is the code production serves. ClearFeature stores raw reports once,
carries only metadata and references on a Kafka-compatible broker, computes every
feature through one engine, and pins the served Python code to an immutable,
cryptographically verified artifact.

## Core capabilities

- **External Feature Projects** — customer-owned repositories (registry YAML + pure
  Python UDFs + golden tests) scaffolded by `fsctl init`; you never edit platform
  source to ship a feature.
- **One compute core** — identical feature values across local runs, online requests,
  batch jobs and offline history for the same inputs and time context (F1 source
  features, F2 dependency graphs, F3 model features, external model-score writeback).
- **Point-in-time correctness** — dual-clock availability (`available_at` + `data_ts`),
  append-only availability corrections, leakage-safe training datasets, and a
  freshness write guard (no stale online overwrites; idempotent replay).
- **Tested code == served code** — immutable content-addressed feature wheels; the
  promoted bundle pins the wheel SHA-256; workers verify installed code byte-for-byte
  at startup and fail closed on any mismatch.
- **Governed promotion** — shadow → live with profiles, unique approvers, shadow
  soak, audited overrides, and rollback (`fsctl promote` / `fsctl rollback`).
- **Built-in observability** — per-process Prometheus `/metrics`, `/health` liveness,
  role-aware `/ready` readiness, structured JSON logs.

## Quick start

```bash
# USE CLEARFEATURE — build your first Feature Project (no platform source needed):
#   docs/20_feature_project_quickstart.md
python -m fintech_feature_platform.cli.fsctl init --name my-features
cd my-features && uv pip install -e .
python -m fintech_feature_platform.cli.fsctl validate
python -m fintech_feature_platform.cli.fsctl test
python -m fintech_feature_platform.cli.fsctl run-local --feature activity_score \
  --entity-json my_features/tests/entity.json
```

## CLI overview

`fsctl` (also `python -m fintech_feature_platform.cli.fsctl`) provides:
`init` (scaffold) · `validate` (registry + bundle digest) · `test` (golden cases
through the real compute core) · `run-local` (compute one feature locally) ·
`publish` (immutable artifact-bound bundle) · `promote` / `rollback` (governed
pointers) · `image-context` (verified worker-image build context). All commands emit
stable machine-readable JSON and discover defaults from `feature_project.yaml`.

## Architecture at a glance

Raw reports land once in S3-compatible object storage; the broker (Redpanda /
Kafka-compatible) carries metadata and references only. An HTTP API accepts online
feature requests (explicit per-request deadline, documented safe-retry contract) and
batch jobs (chunked, durable status). Six single-purpose workers — online compute,
batch compute, offline writer, metadata writer, propagation (dependent recompute),
model-score writer — all execute the same compute core. PostgreSQL holds offline
history and metadata; Valkey serves latest online values behind a freshness guard.
See `docs/product/architecture_one_pager.md`.

## Supported Python

Python **3.12** (declared in `pyproject.toml`; CI runs 3.12).

## Run the stack (OPERATE CLEARFEATURE)

```bash
cp .env.example .env            # dev defaults; replace credentials beyond localhost
# REQUIRED: the API is fail-closed — generate keys first (see Authentication):
export FSP_API_KEYS='[{"key_id":"ops","role":"operator","secret":"'"$(openssl rand -hex 32)"'"}]'
docker compose up -d --wait
curl -s localhost:8000/health   # liveness
curl -s localhost:8000/ready    # readiness (dependency capability)
docker compose down             # stop; add -v to reset data
```

## Authentication

Every endpoint except the public probes `GET /health` and `GET /ready` requires
`Authorization: Bearer <api-key>`. Two roles: `service` (data plane) and `operator`
(superset). The API refuses to start in the default `api_key` mode without
`FSP_API_KEYS`. Details: `docs/security/minimum_security.md`.

## Health, readiness, metrics, logging

`GET /health` = liveness; `GET /ready` = role-aware readiness (200/503, bounded
dependency categories, no secrets); per-process `GET /metrics` = Prometheus text
(enable with `FSP_OBSERVABILITY_PORT`); structured JSON service logs
(`FSP_LOG_FORMAT`, `FSP_LOG_LEVEL`). Full contract, including the Community vs
Enterprise observability boundary: `docs/21_observability_contract.md`.

## Community trust and security boundaries

- Community UDF authors are **trusted**: UDFs execute **in-process** with worker
  permissions — there is **no sandbox**.
- One compatible Feature Project / package set per worker image.
- Artifact binding is fail-closed; promotion requires named approvers.

## Known MVP limitations

Single-node Docker Compose deployment (no HA/Kubernetes); item-by-item F1/F2 batch
execution (scale horizontally with worker replicas); bring-your-own dashboards and
alerting; bounded, environment-specific scale claims. Full list:
`docs/beta_acceptance/known_limitations.md` and `docs/beta_acceptance/non_goals.md`.

## Documentation

- USE: `docs/20_feature_project_quickstart.md`, `docs/demo/credit_decision_demo.md`
- OPERATE: `docs/deployment/`, `docs/runbooks/`, `docs/21_observability_contract.md`,
  `docs/security/minimum_security.md`
- CONTRACTS: `docs/03_api_contracts.md`, `docs/architecture/community_enterprise_boundary.md`
- RELEASE: `docs/beta_acceptance/release_notes.md`

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Security reporting

See [`SECURITY.md`](SECURITY.md).



## License

Apache License 2.0 — see [`LICENSE`](LICENSE). © ClearFeature Labs.
