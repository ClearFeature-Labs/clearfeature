# Deployment Options

Four options, in increasing order of operational commitment. Options A and B are
validated today; Option C is a configuration exercise on existing seams; Option D is
planned engineering work — not a ready installation package.

## Option A — Local demonstration (validated)

Docker Compose on a single machine: PostgreSQL, Valkey, MinIO, Redpanda, the Feature
API, six workers, and the optional demo decision service. Synthetic data,
one-command demos and smokes, ephemeral API keys generated per run. **Not
production** — this is the evaluation and development runtime. Start:
`../deployment/docker_compose.md`.

## Option B — Paid pilot (validated runtime + documented bank-side duties)

The same single-node stack on one controlled virtual machine or client environment,
hardened for a pilot:

- reverse proxy with TLS in front of the two HTTP services (TLS — encrypted network
  transport; provided by the bank's proxy, documented expectation);
- fail-closed API-key authentication with service/operator roles and documented
  rotation (built in);
- real credentials in the environment file; localhost-bound ports by default;
- client-provided backups (volume/pg_dump level) and host monitoring;
- documented resource sizing for the pilot cohort;
- additive database migrations with a proven upgrade path.

## Option C — Client-managed dependencies (configuration path)

All stateful services are reached through standard environment configuration, so the
platform services can connect to the client's existing infrastructure: a
Kafka-compatible broker (the shipped Compose stack runs Redpanda; any broker that
speaks the Kafka client/topic/consumer-group API works), PostgreSQL, S3-compatible
object storage, and a Redis-compatible online store. The application containers stay
identical. Validation against a specific client stack is part of pilot/production
onboarding — the seams exist and are the same ones the Compose stack uses.

## Option D — Production enterprise path (planned work)

Horizontally scaled APIs and workers; highly available PostgreSQL, multi-broker
Kafka, durable object storage, online-store HA or a documented
rebuild-from-offline strategy; Kubernetes or equivalent orchestration; permanent
monitoring and alerting; restore and failover drills; corporate identity
integration (OIDC — OpenID Connect — with the bank's identity provider). The
service contracts are deliberately shaped for this migration
(`../deployment/docker_compose.md` §8), and the concrete work items are scoped in
the project roadmap — **label this as engineering to be contracted,
never as an existing installation**.
