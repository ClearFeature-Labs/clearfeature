# Compose Startup, Profiles & Supported Commands

The broker is **Redpanda** using the **Kafka-compatible API** — standard Kafka
clients, topics and consumer groups talk to it. There is no Apache Kafka container.

## Default startup — a useful generic platform

```bash
docker compose up -d
```

starts the full generic platform: the 4 infra services (postgres, valkey, minio,
redpanda), the one-shot `topic-init` (creates topics, exits 0, gates the rest), and
all six long-running workers plus the API. This is the intended public experience:
one command yields a platform that can do **online serving and batch materialization**
out of the box. Authentication is fail-closed — set `FSP_API_KEYS` first (or the
explicit development bypass); see `../security/minimum_security.md`.

The synthetic demo scorer is the **only** service that is opt-in:

```bash
docker compose --profile demo up -d      # adds demo-model-service (:8090)
```

## Why the platform core is NOT split into batch/model profiles
 evaluated splitting the workers into `batch` / `model` compose profiles
and **recommends against it**:

- **A generic platform should be able to run batch jobs.** Moving `batch-worker`
  behind a profile would make the default `docker compose up` unable to process a
  submitted batch job — a worse, more surprising default, not a clearer one.
- **The workers are idle-cheap when their capability is unused.** They consume
  nothing until an event of their type arrives, so leaving them running costs almost
  nothing (each has a small `deploy.resources` limit).
- **Profiles add startup-ordering fragility.** Every app service already gates on
  `topic-init: service_completed_successfully`; conditionally omitting workers via
  profiles complicates that graph for no operational gain. The task explicitly warns:
  *"Do not introduce profiles when they make startup dependencies fragile."*

So the compose file keeps **two** profiles only: the implicit default (everything
except demo) and `demo`.

## Capability groups (conceptual, not compose profiles)

For understanding — not for `--profile` flags — the services group by capability
(full detail: the service list in `docker-compose.yml`):

| Capability group | Services |
|---|---|
| Infrastructure | postgres, valkey, minio, redpanda |
| Bootstrap (one-shot) | topic-init |
| Online serving core | api, online-worker, offline-writer, metadata-writer |
| Batch materialization | batch-worker |
| Reactive dependency propagation | propagation-worker |
| External-score writeback | model-score-writer |
| Demo (opt-in) | demo-model-service |

## Omitting an optional worker when you truly want to

An online-only deployment that never runs batch jobs, reactive propagation or
external-score writeback can omit those workers explicitly — no profile needed:

```bash
docker compose up -d --wait \
  --scale batch-worker=0 \
  --scale propagation-worker=0 \
  --scale model-score-writer=0
```
 verified live that the platform then **starts healthy** and its online
serving, latest reads, offline persistence and truthful metadata status all stay
correct; the only lost capability is that submitted batch jobs never progress past
`accepted` (see the matrix §4). Scale a worker back up (`--scale batch-worker=4`) to
restore and parallelize it.

## Supported commands (exact behavior)

| Command | Starts | Notes |
|---|---|---|
| `docker compose up -d` | infra + topic-init + api + 6 workers | fail-closed auth; needs `FSP_API_KEYS` |
| `docker compose --profile demo up -d` | the above **+ demo-model-service** | demo scorer on :8090 |
| `docker compose up -d --scale batch-worker=N` | scale batch workers to N (0 omits) | single-job parallelism |
| `docker compose config` / `--profile demo config` | render effective config | validation, no start |
| `docker compose stop` / `docker compose --profile demo stop` | stop containers, keep volumes | pass `--profile demo` to also stop the demo scorer |
| `docker compose down -v` | remove containers **and volumes** | destructive; deliberate only |
| `bash scripts/create_kafka_topics.sh` | reruns `topic-init` one-shot | grow-only; after raising partitions |

Operator runbooks (start/stop/rotate/inspect): the runbooks in `../runbooks/`.
