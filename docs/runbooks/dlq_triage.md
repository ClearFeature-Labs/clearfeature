# Runbook — DLQ triage

Dead-letter queue (`fp.dlq`) holds source messages a consumer classified as **structural
poison** (deserialization failure or a structurally-invalid event) — never transient infra
errors, which stay uncommitted and replay. Payloads are captured base64 for inspection; do not
log them (they may contain feature values).

## Symptoms
- `dlq_events_total` counter rising (by `stage` / `status`).
- A consumer group's lag flat while its input topic grows (a poison message is not the cause of
  replay-looping in beta — poison is committed after DLQ — but a spike signals bad producers).
- Downstream projections (offline history, batch job status) missing rows for known inputs.

## Metrics to check
- `GET /v1/observability/metrics` → `counters["dlq_events_total{stage=...,status=...}"]`.
- Compare against `offline_append_rows_total`, `batch_items_total`, `feature_updates_total` to
  see which stage is shedding messages.

## Commands / API
- Inspect the DLQ topic with your Kafka console consumer (out of band); each `DeadLetterEvent`
  carries `source_topic`, `failure_stage`, `failure_status`, `error`, and best-effort ids.
- Reprocess after a producer fix: replay the original bytes to the source topic (see
  `replay_rerun.md`).

## Safe remediation
1. Identify the `failure_status` (`deserialization_failed` vs `invalid_event`).
2. Fix the **producer** or registry (e.g. an unknown view/feature, a malformed event).
3. Re-emit corrected events to the source topic. Offline dedup + online CAS make replay
   idempotent, so re-emitting a corrected-but-overlapping batch is safe.

## What not to do
- Do **not** hand-edit and re-inject a poison message's bytes into the source topic without
  fixing the root cause — it will DLQ again.
- Do **not** log `source_payload_b64` (may contain feature values).
- Do **not** disable poison-DLQ to "unblock" a partition; that reintroduces the replay-loop that
  DLQ exists to prevent.

## Escalation
- DLQ rate > a few messages/min sustained, or any `invalid_event` referencing a **live** feature
  view: page the feature-platform owner — a live registry/producer contract is broken.
