# Deployment — Docker Compose baseline (production-light)

## 1. What this is (and is not)

This Compose file is a **production-light / staging / serious-dev** runtime: robust
single-node services with healthchecks, restarts, persistence, bounded logs, and moderate
resource limits. It is **not HA production** — every stateful service (Postgres, Redpanda,
Valkey, MinIO) is a single instance with a single volume.

**Do not "convert this file to prod".** Production keeps the *service contracts* (the
`FSP_*` env seams, healthchecks, one-image-many-commands app model) and replaces the
stateful components with HA equivalents (§8).

## 2. How to run

```bash
cp .env.example .env          # adjust; replace dev credentials
docker compose up -d --build
docker compose ps             # wait for (healthy)
docker compose logs -f api
bash scripts/run_compose_smoke.sh   # optional end-to-end smoke
docker compose stop           # stop, keep volumes
docker compose down           # remove containers, keep volumes
docker compose down -v        # DESTROYS data volumes — deliberate action only
```

Services: `postgres`, `valkey`, `minio`, `redpanda` (infra) + one-shot `topic-init` +
`api` and six workers (`online-worker`, `offline-writer`, `metadata-writer`,
`model-score-writer`, `batch-worker`, `propagation-worker`) sharing one image, each
running its real runner module with `--forever` (bounded-round daemon mode; same Kafka
consumer across rounds, so no rebalance churn). The broker is **Redpanda** using the
**Kafka-compatible API** (standard Kafka clients, topics and consumer groups talk to
it — there is no Apache Kafka container). Per-service roles and classification:
the service list in `docker-compose.yml`; startup/profiles: `compose_profiles.md`.

Demo-only : `demo-model-service` — an external credit-decision scorer on the
same image, gated behind the compose profile `demo` (`docker compose --profile demo up`),
so the default stack is unchanged. It deliberately receives NO store/broker credentials
(only `FSP_FEATURE_API_URL` + the registry path for the model digest pin): it consumes
the Feature API HTTP contract only. Port `127.0.0.1:${MODEL_SERVICE_PORT:-8090}`.
Driver: `scripts/run_credit_online_demo.sh`.

## 2b. Authentication

The `api` container starts in `FSP_SECURITY_MODE=api_key` and **refuses to start
without `FSP_API_KEYS`** (JSON list of `{key_id, role, secret}`; roles `service` /
`operator`, operator ⊇ service; header `Authorization: Bearer <key>`). All repo
scripts generate ephemeral per-run keys when none are supplied, so every smoke/demo
exercises authentication. OpenAPI/docs are disabled in api_key mode; `/health` stays
public and minimal. The development-only bypass (`FSP_SECURITY_MODE=disabled`,
requires `FSP_ENVIRONMENT=development`) prints a loud warning and is rejected in any
other environment. `demo-model-service` has its OWN key registry
(`FSP_MODEL_SERVICE_API_KEYS` on the host) and presents its OWN service key upstream
(`FSP_FEATURE_API_KEY`). Full guide: `docs/security/minimum_security.md`. TLS and
rate limiting remain reverse-proxy responsibilities for a pilot.

## 3. Connection rules

| Client | Address |
|---|---|
| container → Postgres | `postgres:5432` |
| container → Valkey | `valkey:6379` |
| container → MinIO | `minio:9000` |
| container → Kafka | `redpanda:19092` (internal listener) |
| host → Postgres | `localhost:${POSTGRES_PORT:-5432}` |
| host → Kafka | `localhost:${REDPANDA_KAFKA_PORT:-19092}` (external listener) |
| host → API | `http://localhost:${API_PORT:-8000}` |

The app containers get real `FSP_*` settings (see `api/settings.py`) wired to the
Docker-network names in `docker-compose.yml` (`x-app-env`). Host-side tools use the
localhost equivalents listed in `.env.example`.

## 4. Why Redpanda has two listeners

A Kafka broker tells clients where to connect via its *advertised* address. One address
cannot serve both worlds: containers cannot reach `localhost:19092` (that's their own
loopback), and the host cannot reach `redpanda:19092` (Docker-network DNS). So the broker
runs two listeners — `internal://redpanda:19092` advertised to in-network clients and
`external://localhost:${REDPANDA_KAFKA_PORT}` advertised through the published port.
Advertising only `localhost` (the old dev file) silently breaks every containerized
consumer.

## 4b. Kafka topics are deployment-owned

Topics are provisioned explicitly — the platform does not rely on broker auto-creation
(auto-created topics silently get 1 partition, capping every consumer group at one
active consumer). The one-shot `topic-init` service (renamed from `kafka-init` in so the operator-facing name does not read like a second broker) creates
the 8 canonical `fp.*`
topics (single source: `fs_core/events/topics.py::ALL_TOPICS`) before any API/worker
starts (`service_completed_successfully` gating).

- **Local defaults**: `FSP_KAFKA_TOPIC_PARTITIONS=4`, `FSP_KAFKA_REPLICATION_FACTOR=1`
  (single broker — never set RF>1 here). 4 partitions demonstrate up to four active
  consumers per group without making the stack heavy.
- **Production guidance** (not code): partitions ≈ 2× the expected worker ceiling per
  topic; RF=3 with ≥3 brokers.
- **Grow-only**: rerunning is a no-op; raising the setting expands partitions
  (`bash scripts/create_kafka_topics.sh` after editing `.env`); lowering it never
  shrinks anything — the run reports "partitions cannot be reduced". Partition growth
  re-maps future keys; the disorder window is absorbed by D9 CAS / offline dedup /
  idempotent metadata projection. Prefer a quiet window anyway.
- Scaling consumers: `docker compose up -d --scale batch-worker=4 --wait`. Remember
  each replica opens its own DB pool (`FSP_DB_POOL_SIZE`) — keep the sum of pools under
  `POSTGRES_MAX_CONNECTIONS`.

## 5. Why host ports bind to 127.0.0.1

`"127.0.0.1:5432:5432"` instead of `"5432:5432"`: Docker's port publishing bypasses most
host firewalls, so a `0.0.0.0` bind exposes dev-credentialed Postgres/MinIO/Kafka to the
local network. Localhost-only binding keeps the stack reachable from the machine only;
anything wider is a deliberate production decision with real credentials and TLS.

## 6. Persistence

Named volumes: `pgdata` (`/var/lib/postgresql/data`), `valkeydata` (`/data`, AOF with
`appendfsync everysec`, `noeviction` — the online store must fail loudly, not evict),
`miniodata`, `redpandadata`. Postgres auto-applies `infra/postgres/*.sql` (001–008, in
filename order) on **first init of an empty volume**; migrations added later are applied
manually (`psql "$FSP_POSTGRES_DSN" -f infra/postgres/00X_*.sql`). Note: this file sets
the Compose project name `fsp-dev`, so volumes are `fsp-dev_pgdata` etc. — volumes created
by the old unnamed file (`<dirname>_pgdata`) are not migrated automatically.

## 7. Smoke checks

```bash
bash scripts/run_local_backend_smoke.sh   # THE end-to-end smoke: health + schema + real
                                          # Kafka-first request + Valkey/Postgres/lineage
                                          # (docs/deployment/local_backend_smoke.md)
bash scripts/run_compose_smoke.sh         # quick: up --wait + /health + metrics, leaves running
# One store/broker at a time (host-side pytest against compose infra):
FSP_POSTGRES_INTEGRATION=1 uv run python -m pytest tests/fs_core/stores/test_postgres_offline_store.py
# Existing volume missing schema? migrations are idempotent:
bash scripts/apply_postgres_migrations.sh
```

Default `make verify` / the beta acceptance runner remain Docker-free.

## 8. Kubernetes / on-prem HA migration

Easy (mechanical — the contracts are already shaped for it):

| Compose concept | Kubernetes equivalent |
|---|---|
| `api` / worker services | Deployments (one per command), HPA later |
| `x-app-env` (`FSP_*`) | ConfigMaps + Secrets |
| healthchecks | liveness/readiness probes (same commands) |
| named volumes | PersistentVolumeClaims |
| `deploy.resources` | requests/limits |
| `depends_on: service_healthy` | initContainers / probe gating |

Hard (real engineering, not translation): Postgres HA (Patroni/operator or managed;
plus partitioning per docs/13 tier M); Redpanda 3+ brokers with real rack awareness;
Valkey HA or an explicit rebuild-from-offline strategy (the online store is a derived
cache of offline history — rebuild is the honest DR plan); MinIO distributed mode or an
external S3; backup/restore drills; TLS everywhere + a secrets manager; monitoring/alerting
(metrics endpoint exists, the stack is the client's); failover tests.

Sizing/scale-out beyond tier M is trigger-gated: see the T1/T2/T3 table in
`docs/beta_acceptance/walkthrough_energy_backfill_t1.md` — do not adopt Spark/ClickHouse
et al. without a measured trigger.
