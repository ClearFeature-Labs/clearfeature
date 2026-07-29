# Real local backend smoke

One command that proves the production-light Compose runtime works **end to end through
real containers** — Postgres, Valkey, MinIO, Redpanda, the API, and all six `--forever`
workers.

```bash
bash scripts/run_local_backend_smoke.sh
```

## 1. What it proves

- `docker compose config` is valid and `up -d --build --wait` reaches green healthchecks.
- The Postgres **schema is present** (8 required tables from `infra/postgres/*.sql`).
- The API answers `/health` and serves a **bounded, values-free** metrics snapshot.
- A real request flows through the **Kafka-first chain**:
  API → Redpanda (`redpanda:19092` internal listener) → online-worker → Valkey (`written`)
  → offline event → offline-writer → Postgres.
- The F2-derived value reads back from the online store (`debt_to_income_ratio = 0.2`).
- Offline history rows exist in `features_offline` (checked via psql in the container).
- The lineage endpoint answers provenance **values-free** (hashes/timestamps; no
  payload/object_key/storage_uri, and the feature value itself never appears).
- All six workers run with **restart count 0** (`--forever` daemon mode holds; no
  restart/rebalance churn).

## 2. What it does NOT prove

- Not HA, failover, or durability under crash (single-node stateful services).
- Not scale (no 20M items; T1 arithmetic lives in the beta acceptance pack).
- Not security (dev credentials, no TLS ).
- Not full semantic correctness — that is the Docker-free suite (960+ tests) and
  `scripts/run_beta_acceptance.py` (46 checks). This smoke proves the *runtime wiring*.

## 3. How to run

```bash
bash scripts/run_local_backend_smoke.sh                  # default: build, smoke, stop
bash scripts/run_local_backend_smoke.sh --no-build       # reuse the existing image
bash scripts/run_local_backend_smoke.sh --keep-running   # leave the stack up afterwards
bash scripts/run_local_backend_smoke.sh --down           # remove containers afterwards
bash scripts/run_local_backend_smoke.sh --timeout-seconds 300
```

Authentication : the stack is fail-closed (`api_key` mode). The smoke
generates an **ephemeral operator key** per run and sends `Authorization: Bearer` on
every protected call; supply your own via `FSP_API_KEYS` + `FSP_CLIENT_API_KEY` to
override. `/health` stays public. See `docs/security/minimum_security.md`.

Defaults are safe: **volumes are never deleted**; on PASS the stack is stopped (restart
with `docker compose up -d`); on FAIL it is left running for debugging.

### Optional clean run (destructive, explicit)

```bash
bash scripts/run_local_backend_smoke.sh --clean-volumes
# equivalent to: docker compose down -v && bash scripts/run_local_backend_smoke.sh
```

This destroys all data volumes first, so Postgres initdb re-applies every migration on a
fresh volume — the full "new machine" experience.

## 4. How migrations work

- Postgres initdb applies `infra/postgres/*.sql` (001–008, filename order) **only on the
  first initialization of an EMPTY volume** (`/docker-entrypoint-initdb.d`).
- An **existing volume** gets nothing automatically. The smoke verifies the 8 required
  tables and fails with a clear message if any is missing. Then either:
  - `bash scripts/apply_postgres_migrations.sh` — applies all migrations manually; safe to
    re-run (every file uses `CREATE/ALTER ... IF NOT EXISTS`); or
  - re-run the smoke with `--clean-volumes` (destructive).
- Required tables: `raw_reports_meta`, `features_offline`, `feature_requests`,
  `request_events`, `batch_jobs`, `batch_chunks`, `source_dataset_manifests`,
  `source_dataset_items`.

## 5. Expected PASS output

```text
Real local backend smoke: PASS

Checked:
  [PASS] compose config valid
  [PASS] containers up + healthchecks green (docker compose up --wait)
  [PASS] Postgres schema present (8 required tables)
  [PASS] API /health
  [PASS] metrics endpoint bounded + values-free
  [PASS] Kafka-first chain completed (API -> Redpanda -> online-worker; online=written)
  [PASS] offline event consumed (offline-writer wrote history)
  [PASS] online latest read from Valkey (debt_to_income_ratio=0.2, F2-derived)
  [PASS] offline history rows in Postgres (2 rows for smoke entity)
  [PASS] lineage answers provenance, values-free (hashes/timestamps only)
  [PASS] all 6 workers running with restart count 0 (--forever daemon mode holds)

GATED: destructive clean-volume migration smoke not run by default (use --clean-volumes)
```

## 6. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| First request ends `deadline_expired` | A just-started online worker spends its first seconds joining the Kafka consumer group; the smoke uses a 60s deadline and retries once against the warm consumer. If it persists: `docker compose logs online-worker`. |
| Containers can't reach Kafka | The broker must advertise `internal://redpanda:19092` to containers and `external://localhost:$REDPANDA_KAFKA_PORT` to the host — two listeners, never `localhost` on the internal one (see `docs/deployment/docker_compose.md` §4). |
| `Postgres tables missing` | The volume predates the schema (initdb only runs on first empty-volume init). Run `scripts/apply_postgres_migrations.sh` or `--clean-volumes`. |
| Stale volumes / weird old data | The compose project is `fsp-dev`; volumes from the legacy unnamed file (`<dirname>_pgdata`) are not migrated. `docker volume ls` to find them; the smoke's deterministic entity is `smoke_0072_*`. |
| MinIO bucket errors | The app creates `raw-reports` on first use; check `docker compose logs api` and MinIO credentials in `.env`. |
| Worker restart loop | Workers must run their runner with `--forever`; a drain-until-idle worker exits each idle poll and restarts. Check `docker compose ps` STATUS and `docker inspect --format '{{.RestartCount}}' <container>`. |
| Port already in use | Host bindings are `127.0.0.1:<port>`; adjust `POSTGRES_PORT`/`VALKEY_PORT`/`API_PORT`/... in `.env`. |

## 7. How this differs from other checks

| Check | Scope | Needs Docker |
|---|---|---|
| `uv run make verify` (unit tests) | semantic correctness, in-memory seams | no |
| `scripts/run_beta_acceptance.py` | the four beta walkthroughs, condensed, in-process | no |
| **this smoke** | the real container runtime and its service contracts | yes |
| gated integration tests (`FSP_*_INTEGRATION=1`) | one store/broker at a time, via pytest | yes |
| real production HA | failover, backups, TLS, monitoring | out of scope (see `docker_compose.md` §8) |
