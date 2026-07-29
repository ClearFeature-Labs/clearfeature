# Runbook — replay / rerun

The platform is replay-safe by construction: offline history dedups exact rows (fingerprint-
aware), and online writes are D9-guarded (`(data_ts, max_input_data_ts)` + `input_fingerprint`),
so re-processing an event or re-running a batch never double-writes or overwrites a fresher
value.

## Symptoms
- A consumer left messages uncommitted after a transient failure (store/publisher outage).
- A batch job finished `completed_with_errors`, or a chunk failed on infra.
- An import (table / DWH) needs to be re-run after a fix.

## Metrics to check
- `offline_append_errors_total`, `online_request_errors_total` (spikes → transient infra).
- `batch_pause_events_total` / `batch_rate_limited_events_total` (batch throttled, not broken —
  it will resume; do not force).
- Kafka consumer lag for the affected group.

## Commands / API
- Batch rerun: re-submit the same `POST /v1/batch/jobs` (same scope). Dedup makes overlapping
  rows no-ops; only genuinely new/changed rows append.
- Import rerun: re-run `fsctl`-independent import (`run_table_feature_import` /
  `run_dwh_feature_import`) — identical rows are skipped, counts show `duplicate`.
- Online replay: nothing to do — an uncommitted event is re-polled automatically.

## Safe remediation
1. Confirm the failure was **transient** (infra), not structural (structural → DLQ, see
   `dlq_triage.md`).
2. Restore the dependency (Postgres/MinIO/Kafka/Valkey).
3. Let the consumer replay, or re-submit the batch/import. Verify via
   `offline_append_rows_total` climbing and `...errors_total` flat.

## What not to do
- Do **not** reset consumer offsets to "reprocess everything" as a first response — targeted
  replay + dedup is safer and cheaper.
- Do **not** bypass the D9 guard to "force" an online value; a historical rerun must not beat a
  fresher online value.

## Escalation
- Replay does not converge (errors keep climbing after the dependency is healthy): capture the
  error strings from `BatchChunkProcessed.first_errors` / request status and escalate to the
  owner — this is a deterministic bug, not a transient.
