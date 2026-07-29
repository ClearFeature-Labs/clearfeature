# Local Infrastructure (dev only)

This directory documents the local infrastructure baseline used by future backend
implementations. The services are defined in the repository-root `docker-compose.yml`.

**These services are not yet wired into `fs_core` or the API.** They exist so the
upcoming backend tasks (Postgres metadata repository / offline store, Valkey online
store, MinIO payload store) have a known local target to connect to.

## Services

| Service  | Purpose (future)                                   | Host port(s)        |
|----------|----------------------------------------------------|---------------------|
| postgres | `raw_reports_meta`, `features_offline`, jobs, etc. | `5432`              |
| valkey   | online feature store (CAS by `data_ts`)            | `6379`              |
| minio    | raw report payload storage                         | `9000` (API), `9001` (console) |

## Default dev credentials

These are **not secret** — local development only.

```text
POSTGRES_USER=fsp
POSTGRES_PASSWORD=fsp_dev_password
POSTGRES_DB=fsp

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
```

Valkey runs without authentication (local dev).

## Overriding values

`docker-compose.yml` uses `${VAR:-default}` interpolation, so it runs with no
configuration. To override, create a local `.env` file at the repository root
(Compose reads it automatically). `.env` is git-ignored.

```text
# .env (local, not committed)
POSTGRES_PASSWORD=something_else
MINIO_ROOT_PASSWORD=something_else_long
```

There is intentionally **no committed `.env.example`** in this task: `.gitignore`
ignores `.env.*`, and `.gitignore` is out of scope here. The defaults above are the
source of truth.

## Start / stop

```bash
docker compose config                          # validate the file (no containers)
docker compose up -d postgres valkey minio     # start in the background
docker compose ps                              # show status / health
docker compose down                            # stop (keeps named volumes)
docker compose down -v                         # stop and delete data volumes
```

Data persists in named volumes: `pgdata`, `valkeydata`, `miniodata`.

If a port is already in use (e.g. `Bind for 0.0.0.0:6379 failed: port is already
allocated`), stop whatever is using it locally, or override the host port via a
local `.env` (Compose reads it) before `docker compose up`.

If you change the Postgres major version, the existing `pgdata` volume (written by
the old version) is incompatible — postgres will log `database files are
incompatible with server` and exit. Reset it with `docker compose down -v` (this
deletes local data).

## Healthchecks

- **postgres** — `pg_isready` (reports `healthy` once the DB accepts connections).
- **valkey** — `valkey-cli ping`.
- **minio** — `mc ready local` (the bundled MinIO client). The console is at
  `http://localhost:9001`; the health endpoint at `http://localhost:9000/minio/health/live`.

## MinIO payload store (backend, not yet wired)

`fs_core.raw.minio_payload_store.MinIOPayloadStore` implements the payload-store
contract against MinIO, addressed by `s3://bucket/key`. It is **not wired into the
API yet** — the app still uses the in-memory store. Its unit tests use a fake client
and need neither Docker nor the `minio` SDK, so `uv run make verify` stays
Docker-free.

Optional live round-trip against the local MinIO above:

```bash
uv sync --extra storage           # installs the minio SDK
docker compose up -d minio
FSP_MINIO_INTEGRATION=1 uv run pytest tests/fs_core/raw/test_minio_payload_store.py
docker compose down
```

Without `FSP_MINIO_INTEGRATION=1` the live test is skipped.

## Postgres metadata repository (backend, not yet wired)

`fs_core.raw.postgres_meta_repository.PostgresRawReportMetaRepository` implements the
raw-metadata seam (`get_meta(report_ref)` + `add(meta)` as an upsert) against a
`raw_reports_meta` table. The schema lives in `infra/postgres/001_raw_reports_meta.sql`
and is **not auto-applied** by docker-compose — apply it manually. It is **not wired
into the API yet**. Its unit tests use a fake connection and need neither Docker nor
the `psycopg` SDK, so `uv run make verify` stays Docker-free.

Apply the schema and run the optional live round-trip against the local Postgres:

```bash
uv sync --extra postgres          # installs psycopg
docker compose up -d postgres
# apply the schema (psql example):
#   psql postgresql://fsp:fsp_dev_password@localhost:5432/fsp \
#     -f infra/postgres/001_raw_reports_meta.sql
FSP_POSTGRES_INTEGRATION=1 uv run pytest tests/fs_core/raw/test_postgres_meta_repository.py
docker compose down
```

Without `FSP_POSTGRES_INTEGRATION=1` the live test is skipped. (The live test also
creates the table itself, so applying the SQL first is optional.)

## Postgres offline feature store (backend, not yet wired)

`fs_core.stores.postgres_offline.PostgresOfflineStore` implements the offline store
seam (`append`/`append_many`/`get`) over a tall, **append-only** `features_offline`
table (schema in `infra/postgres/002_features_offline.sql`, **not auto-applied**).
Recomputation appends new rows; `get` returns matching rows in append order or `[]`.
Feature values must be JSON-serializable. It is **not wired into the API yet**; unit
tests use a fake connection and need neither Docker nor `psycopg`.

Optional live round-trip against the local Postgres:

```bash
uv sync --extra postgres
docker compose up -d postgres
# apply the schema (psql example):
#   psql postgresql://fsp:fsp_dev_password@localhost:5432/fsp \
#     -f infra/postgres/002_features_offline.sql
FSP_POSTGRES_INTEGRATION=1 uv run pytest tests/fs_core/stores/test_postgres_offline_store.py
docker compose down
```

Without `FSP_POSTGRES_INTEGRATION=1` the live test is skipped. (It also creates the
table itself, so applying the SQL first is optional.)

## Valkey online feature store (backend, not yet wired)

`fs_core.stores.valkey_online.ValkeyOnlineStore` implements the online store seam
(`write`/`write_many`/`get`) against Valkey. One hash per entity+view
(`fs:online:{view}:v{view_version}:{entity_key_encoded}`, field
`{feature_name}:v{feature_version}`); writes use an **atomic Lua compare-and-set** so
only a strictly newer `data_ts` overwrites (equal/older is skipped). Freshness compares
integer `data_ts_epoch_us`. `entity_type` is not yet part of the key (the current seam
does not carry it). It is **not wired into the API yet**; unit tests use a fake client
and need neither Docker nor `redis`.

Optional live round-trip against the local Valkey:

```bash
uv sync --extra online             # installs the redis client
docker compose up -d valkey
FSP_VALKEY_INTEGRATION=1 uv run pytest tests/fs_core/stores/test_valkey_online_store.py
docker compose down
```

Without `FSP_VALKEY_INTEGRATION=1` the live test is skipped.

## API backend mode (FSP_BACKEND)

The FastAPI app selects its backend via `FSP_BACKEND` (default `memory`). Memory mode
is fully in-process and needs none of the services above. `FSP_BACKEND=local` wires the
real backends — MinIO payloads, Postgres metadata + offline history, Valkey online —
behind the same routes. Local mode is **opt-in and dev-only** (not production config).

Local-mode env vars (`FSP_POSTGRES_DSN`, `FSP_MINIO_*`, `FSP_VALKEY_*`) default to the
services above. Notes:

- Required extras: `uv sync --extra dev --extra storage --extra postgres --extra online`.
- Postgres uses **short-lived per-operation connections** (thread-safe, no pooling).
- The MinIO bucket is **auto-created** (`create_bucket=True`).
- Schema SQL is **not auto-applied** at startup — apply it manually first:
  `psql "$FSP_POSTGRES_DSN" -f infra/postgres/001_raw_reports_meta.sql` and
  `... -f infra/postgres/002_features_offline.sql`.

Run the app in local mode:

```bash
uv sync --extra api --extra storage --extra postgres --extra online
docker compose up -d postgres minio valkey
FSP_BACKEND=local uv run uvicorn fintech_feature_platform.api.app:app --reload
```

## Local backend smoke

A one-command, opt-in live smoke runs the full real-backend round trip (ingest →
compute → latest → history) and tears the infra down again. It is **not** part of
`make verify` (default tests stay Docker-free) and requires Docker + the extras:

```bash
bash scripts/run_local_backend_smoke.sh
```

It runs `uv sync --extra dev --extra storage --extra postgres --extra online`, brings
up `postgres minio valkey`, runs `tests/api/test_local_backend_wiring.py` with
`FSP_BACKEND=local FSP_LOCAL_BACKEND_INTEGRATION=1` (the test self-applies the schema),
and `docker compose down`s on exit. Reminder: schema SQL
(`infra/postgres/001_raw_reports_meta.sql`, `002_features_offline.sql`) is **not**
auto-applied by app startup; the smoke/integration test applies it itself, and the
MinIO bucket is auto-created in local mode.

## Troubleshooting: Valkey host port 6379 in use

If `docker compose up` fails to bind Valkey (`Bind for 0.0.0.0:6379 failed: port is
already allocated`), find and stop the conflicting process/container:

```bash
docker ps --format 'table {{.Names}}\t{{.Ports}}' | grep 6379
lsof -nP -iTCP:6379 -sTCP:LISTEN
```

Then stop it (`docker stop <name>`), or stop your local Redis/Valkey. Notes:

- `FSP_VALKEY_PORT` changes the **client** port the app connects to, not what
  `docker-compose.yml` publishes (currently fixed at host `6379:6379`).
- To publish a different host port you would change `docker-compose.yml` intentionally
  in a future task; it is left unchanged here.

## Intentionally deferred

- No database schema / DDL (`raw_reports_meta`, `features_offline`, …).
- No MinIO bucket creation (`raw-reports`).
- No Kafka, Prometheus, Grafana, kafka-ui, or catalog UI.

Schema and bucket creation belong to the specific backend-integration tasks that own
those contracts. This baseline only runs the storage engines.
