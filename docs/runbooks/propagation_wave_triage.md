# Runbook — propagation wave triage

When a feature with **reactive** dependents is written, the platform emits a values-free
`FeatureUpdated` and the reactive worker debounces + runs an **offline-only** recompute wave
. Waves are bounded child runs with counts-only accounting.

## Symptoms
- Dependent (F2/F3) features look stale relative to their inputs.
- Reactive consumer lag on `fp.feature-updates` growing.
- `propagation_pending_waves` gauge stuck high; `propagation_lag_seconds` climbing.

## Metrics to check
- `feature_updates_total{source=...}` — are updates being emitted at all?
- `propagation_debounced_total` — high coalescing is normal (that's the point); a flat 0 with
  many updates for the same entity may mean the debounce window is mis-tuned.
- `propagation_waves_total`, `propagation_wave_items_total{outcome=computed|skipped|failed}`.
- `propagation_lag_seconds` (histogram summary): how stale inputs were at recompute time.
- `dlq_events_total{stage="propagation_worker"}` — poison updates being shed.

## Commands / API
- `GET /v1/observability/metrics` for the counters/gauges above.
- Lineage a stale dependent value (`POST /v1/lineage/feature-value`) to see its `data_ts` /
  `input_fingerprint` / `bundle_digest` and confirm whether a recompute actually ran.

## Safe remediation
1. `skipped` high → a required input is missing/stale in offline history (PIT-ineligible). Backfill
   the input, then the next update triggers a clean recompute.
2. `failed` high → deterministic per-entity error (e.g. an F3 dependent needs its model runner, or
   a dependent reads a raw source a wave can't provide). Check the wave accounting; fix the
   feature definition or supply a model runner for F3 waves.
3. Lag climbing with healthy compute → the reactive worker is under-provisioned; scale it. Waves
   are offline-only, so this never affects online serving.

## What not to do
- Do **not** switch propagation to eager/immediate recompute to "catch up" — debounce + waves are
  what keep this bounded.
- Do **not** enable online writes in a wave; waves are offline-only by default (online refresh is
  the separate guarded Mode-2 path — see `mode2_guarded_refresh.md`).

## Escalation
- Wave `failed` count referencing a **live** dependent, or lag that does not drain after scaling:
  escalate to the owner — a live derived feature is not converging.
