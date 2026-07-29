# Product Overview — a five-minute read

For: Chief Data Officer, Head of Data Science, CTO, Head of Risk, ML platform lead.

## The problem in banks today

Every model team answers the same four questions with custom pipelines: *What was
true about this customer at decision time? Is production computing the same value we
trained on? Can we rerun last month without corrupting history? Where did this
number come from?* In practice: training SQL and serving code drift apart; a
backfill quietly mixes in data the bank did not yet have at the historical moment
("future leakage"); replays and retries create duplicate rows; and when an auditor
or model validator asks about one score, the answer is archaeology.

## The solution

One platform where a feature is **defined once, versioned, computed by one shared
engine, and served three ways** — as a historical training dataset, as a batch
result over a selected cohort, and as a low-latency online value. The platform
tracks four separate clocks for every value — business time (`report_ts`), when the
fact became **available to the bank** (`available_at`), when the platform ingested
it, and when it was computed — so historical questions get historically honest
answers. Every calculation is replay-safe by construction, and every value can be
traced to its inputs, versions and model digest without exposing the values
themselves.

## Who uses it

- **Data Scientists** define features as one registry entry + one pure Python
  function with tests, and promote immutable versioned bundles.
- **Data engineers** select cohorts in the bank's DWH (data warehouse) and hand the
  extraction to ingestion; the platform records, deduplicates and rejects rows with
  durable evidence.
- **Application developers** call an authenticated API for latest features or
  request-driven computation and pass the vector to their model service.
- **Operators** run a small set of services with health checks, statuses, key
  rotation and documented runbooks.

Details: [user_workflows.md](user_workflows.md).

## The three killer features

1. **Historically correct datasets.** Trusted backfills reconstruct what was
   available at a past decision moment; anything without a trusted availability
   claim is conservatively excluded. Live-proven: a 2024 report ingested today is
   eligible for a 2024 observation only when the bank's extraction asserted its
   historical availability — and blocked otherwise.
2. **One calculation contract.** The same compute core and feature definitions run
   batch, online, dependency recomputation, training datasets and golden tests. The
   credit demo shows the online decision service and the batch pipeline producing
   **bit-identical model scores** on all seven customer segments.
3. **Replay-safe processing.** Exact reruns write zero new rows; older data never
   overwrites newer online values; a genuine recomputation is kept as an auditable
   new version; a worker failure mid-job recovers with zero duplicates.

## Verified current capabilities (audited, not aspirational)

Batch jobs that split one job across multiple workers (3.16× at four workers in the
local benchmark); online reads and request-driven computation that stay responsive
during batch load (p95 64 ms in the same benchmark); append-only offline history;
feature versions with a dependency graph and bundle promotion/rollback; model output
as a versioned feature; values-free lineage; fail-closed API-key authentication with
service/operator roles; durable evidence for every rejected record and failure — an
audit found **no silent-loss path**. Full matrix: `../beta_acceptance/known_limitations.md`.

## Commercial value

Shorter model time-to-production (one definition instead of three
implementations), regulator-grade answers (lineage, versioning, replay
determinism, honest time semantics), and lower operational risk (idempotent
processing, durable failure evidence) — deployable **on the bank's own
infrastructure**, with no SaaS dependency.

## Current limitations (stated plainly)

The validated deployment today is a single-node Docker Compose stack — credible for
demos, pilots and serious development, not high availability. Observability is
built in as Prometheus-compatible primitives: per-process `/metrics` exposition,
`/health` + `/ready` probes, structured JSON service logs, and the on-demand
status/lineage APIs — you attach your own Prometheus/log collector
(`docs/21_observability_contract.md`). Not yet included: ready-made dashboards
and alerting. Kubernetes/HA, corporate identity integration and continuous external
stream connectors are planned or customer-driven work — the platform today does
request-triggered event processing, not continuous stream ingestion. The demo's
credit decision thresholds are illustrative, not a bank credit policy. Full list:
`../beta_acceptance/known_limitations.md` and `../beta_acceptance/non_goals.md`.

## Recommended next step

Run the credit-decision demo end to end (`../demo/credit_decision_demo.md`), then
scaffold your own Feature Project with the quickstart
(`../20_feature_project_quickstart.md`).
