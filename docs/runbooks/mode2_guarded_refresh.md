# Runbook — guarded Mode-2 online refresh + COPY/Postgres smoke

Batch is **offline-first**. A batch job may optionally push its computed values online, but only
as a **guarded Mode-2 refresh** : secondary to the offline append, budgeted by a
per-chunk feature cap + token bucket, and D9-guarded so a historical value can never overwrite a
fresher online one. It is globally off by default and must be requested explicitly
(`online_refresh_mode="guarded"`, else the job is rejected 400).

## Symptoms
- Online `/latest` values lag freshly-computed batch outputs (expected if refresh is off).
- `BatchChunkProcessed.online_refresh_counts` shows many `rate_limited` or `failed`.

## Metrics / signals to check
- `batch_rows_written_total` (offline, primary) vs the chunk's `online_refresh_*` counts
  (attempted / written / written_recompute / skipped_stale / noop / rate_limited / failed).
- `batch_pause_events_total` / `batch_rate_limited_events_total` — batch is being held back so it
  never starves online serving (online SLO wins).

## Commands / API
- Enable per job via the batch submit contract (`online_refresh_mode="guarded"` +
  `online_refresh_max_features`). Leave off unless a caller genuinely needs fresher online values
  from a batch.

## Safe remediation
1. `skipped_stale` high → correct: the online value was already fresher (D9 rejected the write).
   No action.
2. `rate_limited` high → the refresh budget is intentionally small; raise the cap only if online
   infra has headroom. Offline correctness is unaffected.
3. `failed` high → online store (Valkey) issue; refresh is secondary and never fails the chunk —
   fix Valkey, the offline history is already durable.

## What not to do
- Do **not** make batch depend on online infra: offline append is primary and must succeed on its
  own; a refresh failure must never fail the chunk.
- Do **not** raise the refresh budget to the point that batch starves online worker capacity.

## COPY / Postgres bulk-writer smoke (from)
The bulk offline writer uses Postgres `COPY` for chunk-level appends. To smoke it locally:

```bash
bash scripts/run_local_backend_smoke.sh   # brings up the local Postgres-backed path
```

Check: rows land in the offline tall table, `offline_append_rows_total` climbs, and an identical
rerun appends **zero** new rows (dedup). If COPY errors, verify the schema migrations under
`infra/postgres/` are applied and the DSN is reachable; the writer must fail loudly, never
silently drop rows.

## Escalation
- Guarded refresh writing to online despite `skipped_stale` expectations, or batch starving online
  latency: escalate — the isolation guarantee (batch never beats online) is the invariant here.
