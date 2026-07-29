# User Workflows

How each role works with the platform day to day. Exact validated commands live in
the operator runbook (the runbooks in `../runbooks/`, runbooks A–R) — this
document explains the *shape* of each journey.

## Data Scientist — define and ship a feature

1. **Add or update the registry definition** — one YAML entry: name, version, type,
   inputs (source fields and/or other features), group membership.
2. **Write one pure calculation function** — Python, `(sources, deps) -> value`, no
   I/O, no framework.
3. **Add deterministic test data and expected results** — fixture rows plus golden
   expected values.
4. **Run local validation** — `uv run make verify` recomputes every golden through
   the real engine; registry validation catches cycles, depth and pin errors.
5. **Publish an immutable bundle** — the registry + functions snapshot gets a
   cryptographic digest (`fsctl publish`).
6. **Promote the bundle** — an explicit, recorded operation (`fsctl promote`).
7. **Run batch or online calculations** — the same definition now serves both.
8. **Inspect lineage and results** — values-free lineage answers version, inputs,
   hashes, availability provenance and model digest.
9. **Roll back if required** — re-promote the previous bundle; history keeps the
   versions it was computed with.

**Edit:** the registry YAML, the feature functions module, fixtures/tests.
**Never edit:** `src/fintech_feature_platform/` (engine, stores, workers, API) —
feature code cannot crash the platform: a broken function fails its own item with
recorded evidence, nothing more.

## Data engineer — load a selected cohort

1. **Select the cohort in the DWH** — any SQL; the platform never scans a bucket to
   decide membership.
2. **Map entity keys and timestamps** — each row: entity key fields, `report_ts`
   (business/as-of time), payload.
3. **Provide trusted `available_at` where the source can assert it** — this is what
   makes historical backfills historically usable; rows without it stay valid but
   conservatively excluded from earlier observations.
4. **Submit the rows for ingestion** (JSONL or DWH-JSON API) — the platform
   deduplicates by content, rejects malformed rows with durable reasons, and audits
   any trusted-availability correction append-only.
5. **Receive a manifest identifier** — the cohort's durable handle.
6. **Submit a batch job against that manifest** — chunked automatically, split
   across workers, safe to resubmit (exact replays are no-ops).
7. **Track written, rejected and failed items** — manifest counts, job/chunk
   statuses with bounded first-error samples, all durable.

## Application developer — consume features

1. **Obtain an API key** (service role) from the operator.
2. **Request latest features** (`POST /v1/features/latest`) or **request-driven
   computation** (`POST /v1/feature-requests`, or `/compute` to submit-and-wait) —
   always with `Authorization: Bearer <key>`.
3. **Receive a request identifier or the completed result** — acceptance means the
   raw report is stored and the compute event is acknowledged.
4. **Poll status when required** (`GET /v1/feature-requests/{id}`) — online,
   offline and metadata write statuses are truthful and durable-backed.
5. **Pass the feature vector to the model service** — the demo decision service
   shows the pattern, including its own upstream service key.
6. **Handle controlled responses** — missing features, stale features, deadline
   expiry and failures are explicit, bounded statuses; never silent defaults.

## Operator — run the platform

Start/stop the stack (profile-aware), rotate keys with overlapping validity,
inspect health/statuses/Kafka lag, read rejected-record and DLQ evidence
(DLQ — dead-letter queue), query the availability-correction audit, trace one
request end to end through statuses and events, and restart workers safely
(rebalance + idempotent replay). Observability is built in: per-process
Prometheus `/metrics`, `/health` + `/ready` probes and structured JSON logs
(`docs/21_observability_contract.md`), plus the on-demand APIs/snapshots/SQL/
runbooks; ready-made dashboards/alert packs remain backlog. Every command, with expected output: runbooks A–R in
the runbooks in `../runbooks/`.
