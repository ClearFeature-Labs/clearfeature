# ClearFeature Community Beta — Release Notes

ClearFeature is a lightweight, open-core feature platform for credit-risk, fraud,
scoring and decision systems: raw reports are stored once in object storage, a
Kafka-compatible broker carries only metadata and references, and online inference
and batch materialization execute the **same** Python feature code through one shared
compute core.

## What the Community beta supports

- **External Feature Projects**: customer-owned feature repositories scaffolded with
  `fsctl init` — registry YAML + pure Python UDFs + golden tests; developed with a
  plain editable install and never by editing platform source.
- **Feature-as-code lifecycle**: `fsctl validate / test / run-local / publish /
  promote / rollback / image-context`; immutable content-addressed feature wheels;
  governed shadow→live promotion (profiles, unique approvers, shadow soak, audited
  overrides) and rollback.
- **Artifact-bound serving**: the promoted bundle pins the exact tested wheel by
  SHA-256; workers verify the installed code byte-for-byte at startup and **fail
  closed** on any mismatch (tested code == served code).
- **Online + batch on one engine**: Kafka-first online requests with an explicit
  per-request deadline and a documented safe-retry contract; chunked batch jobs with
  durable status; identical feature values across run-local, online, batch and
  offline history for the same inputs and time context.
- **Point-in-time correctness**: dual-clock availability (`available_at` +
  `data_ts`), append-only availability corrections, leakage-safe training datasets,
  and a freshness write guard (no stale online overwrites; idempotent replay).
- **Observability**: per-process Prometheus `/metrics`, `/health` liveness and
  role-aware `/ready` readiness, structured JSON service logs
  (`docs/21_observability_contract.md`).
- **Security minimum**: fail-closed bearer-key authentication with service/operator
  roles; fail-closed startup on missing keys.

## Trust and security boundaries

Community UDF authors are **trusted**: UDFs execute in-process with worker
permissions; there is **no sandbox**. One compatible Feature Project / package set
per worker image is the current Community model.

## Known limitations and non-goals

See `known_limitations.md` and `non_goals.md` in this directory. Highlights: the
F1/F2 batch path evaluates entities one by one (horizontal worker scaling is the
supported lever; measured on a single laptop-class node: ~200+ entities/s per worker
end-to-end, ~3.7× with four workers); single-node Docker Compose deployment (no HA,
no Kubernetes manifests); dashboards/alerting packs are not included — bring your own
Prometheus/Grafana; scale claims are bounded by the documented test environments.
