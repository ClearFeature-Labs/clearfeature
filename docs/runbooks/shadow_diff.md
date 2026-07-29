# Runbook — shadow diff

A `shadow` feature computes and writes **offline** but is never served online (
lifecycle; the online planner rejects `shadow`). Before promoting it to `live` (`fsctl promote`,
see promotion rules), compare its outputs against the current `live` feature — by **hashes and
counts only**, never by reading values.

## Symptoms / when to run
- A new feature version is in `shadow` and its shadow soak (bank: 7 days) is elapsing.
- You need evidence that the new logic changes outputs, and for how many entities, before
  promotion.

## Metrics to check
- `feature_updates_total`, `propagation_wave_items_total{outcome="computed"}` — confirm the
  shadow feature is actually being computed offline for the entity population.

## Commands / API
- Use the shadow-diff helper (`fs_core/observability/shadow_diff.diff_shadow_vs_live`) over the
  offline store for the entity set under test. It returns:
  `total_compared`, `same_hash`, `different_hash`, `missing_live`, `missing_shadow`, and a small
  sample of **entity refs** (no values).

## Safe remediation / interpretation
1. `different_hash` near 0 → the new logic is output-equivalent; low-risk promotion.
2. `different_hash` high but expected (intended behavior change) → confirm with the feature owner
   that the change is intended; proceed with the normal approval profile.
3. `missing_shadow` high → the shadow feature is not being computed for many entities (a
   backfill/scope gap) — fix before promoting.

## What not to do
- Do **not** expose or export feature values to compare them — compare `value_hash` only.
- Do **not** promote a shadow feature to `live` on a green diff alone; the approval profile
  (approvers + shadow soak) still applies.

## Escalation
- `missing_live` unexpectedly high (live feature absent for entities that should have it): this
  is a live-serving gap — escalate to the owner independent of the shadow promotion.
