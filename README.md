<p align="center">
  <img src="docs/Logo_ClearFeature.png" alt="ClearFeature logo" width="400">
</p>

# ClearFeature

**ClearFeature is a lightweight, open-core feature platform for credit-risk, fraud,
scoring and decision systems. Define feature logic once in Python, declare the DAG
once in YAML, and run the same tested feature code in historical batch, PIT-safe
training datasets, and online inference.**

> **One feature definition. One DAG. Same logic for training and inference.**

---

## Why ClearFeature?

Decision systems often implement the same feature twice:

```text
training / batch SQL             online service code
        │                               │
        └──────── different logic ──────┘
```

That creates three recurring problems:

- **train/serve skew** — batch and online values diverge;
- **data leakage** — training accidentally sees information that was not available
  at the historical decision time;
- **unverifiable releases** — the feature code tested by a team is not necessarily
  the code production is serving.

ClearFeature replaces that split with one shared compute path:

```text
                       Feature Project
                    Python UDFs + registry
                              │
                              ▼
                         ClearFeature
                              │
                ┌─────────────┴─────────────┐
                │                           │
              batch                       online
                │                           │
        feature history              live feature vector
                │                           │
        training dataset                  model
```

Raw reports are stored once, workers execute the same feature definitions, and
historical datasets are reconstructed with point-in-time availability.

---

## Feature Projects: your code stays separate

ClearFeature Core and your business features are separate projects:

```text
~/projects/
├── clearfeature/          # platform runtime: API, workers, storage integrations
└── risk-features/         # your Feature Project: code, registry, tests
```

A Feature Project is a normal Python package containing:

```text
risk-features/
├── feature_project.yaml
└── risk_features/
    ├── features.py
    ├── registry/features_v1.yaml
    └── tests/golden.yaml
```

You **do not edit ClearFeature Core to ship a feature**.

At deployment time:

```text
ClearFeature Core
      +
Feature Project
      =
Deployable Feature Runtime
```

The API, batch worker and online worker use the same runtime artifact, so they
cannot silently execute different feature code.

### How many Feature Projects should I create?

Do **not** create one project per feature.

A practical boundary is a related domain, team, or model family:

```text
risk-features/
fraud-features/
marketing-features/
```

A single Feature Project can contain hundreds of related features. Community
currently loads one compatible Feature Project/package environment per worker image;
independently released domains can use separate deployments.

---

## Core capabilities

- **External Feature Projects** — customer-owned Python packages with registry YAML,
  pure Python UDFs and golden tests, scaffolded by `fsctl init`.
- **One compute core** — the same feature definitions execute in local tests,
  historical batch jobs, online requests and offline history.
- **Feature DAGs** — direct source features (F1), dependent features (F2), and model
  features can be declared once and resolved by the engine.
- **Point-in-time correctness** — `data_ts` / `report_ts`, `available_at`,
  `calc_ts`, and `observation_ts` are kept distinct to prevent future-data leakage.
- **Offline + online stores** — PostgreSQL keeps historical feature values; Valkey
  serves latest online values behind freshness guards.
- **Raw reports stored once** — raw payloads live in S3-compatible object storage;
  Kafka-compatible transport carries references and metadata rather than large
  source payloads for landed batch workflows.
- **Tested code == served code** — immutable feature artifacts bind registry and
  Python code; workers verify the deployed artifact and fail closed on mismatch.
- **Idempotent execution** — safe replay for ingestion/materialization paths and
  freshness protection against stale online overwrites.
- **Governed promotion** — shadow → live promotion and rollback via `fsctl`.
- **Built-in observability** — `/health`, role-aware `/ready`, Prometheus metrics
  and structured JSON logs.

---

## 5-minute quick start

**Go from raw JSON to batch + online features in a few minutes:**

👉 **[Feature Project Quickstart](docs/20_feature_project_quickstart.md)**

The happy path is intentionally small:

```text
fsctl init
edit features.py + registry.yaml + golden.yaml
fsctl validate
fsctl test
fsctl ingest            # once per raw source
fsctl materialize
fsctl build-training-dataset
```

Then use the same Feature Project for online inference through:

```text
POST /v1/feature-requests/compute
```

Conceptually:

```text
write Python features
        +
declare the DAG in YAML
        ↓
ingest raw JSON
        ↓
  ┌─────────────────────────┐
  │                         │
batch                     online
  │                         │
training data          live features
  └─────────────────────────┘
```

Start with:

```bash
python -m fintech_feature_platform.cli.fsctl init \
  --name my-features \
  --dir ../my-features \
  --package my_features

cd ../my-features
uv pip install -e .

fsctl validate
fsctl test
```

Continue with the **[full walkthrough](docs/20_feature_project_quickstart.md)** to
ingest canonical JSONL, materialize a multi-source feature DAG, build a PIT-safe
training dataset, and compute the same features online.

---

## What happens under the hood?

### Batch

```text
canonical JSONL
      ↓
fsctl ingest
      ↓
raw reports → MinIO / S3-compatible storage
metadata    → PostgreSQL
      ↓
SourceDatasetManifest(s)
      ↓
fsctl materialize
      ↓
join sources by complete entity key
      ↓
resolve feature DAG
      ↓
batch workers
      ↓
feature history → PostgreSQL
```

No client-side join or custom F2 orchestration is required.

### Online

```text
raw reports in request
      ↓
ClearFeature API
      ↓
Kafka-compatible request path
      ↓
online worker
      ↓
same registry + same Python UDFs
      ↓
requested feature vector
      ↓
Valkey latest + PostgreSQL history
```

If an F2 feature depends on F1 features, ClearFeature computes the required
dependencies automatically. Intermediate dependencies are persisted only when they
are requested as outputs.

---

## Where does data live?

```text
Raw reports       → MinIO / S3-compatible object storage
Feature history   → PostgreSQL
Latest online     → Valkey
Jobs / metadata   → PostgreSQL
Worker events     → Redpanda / Kafka-compatible broker
```

`features.py` contains **calculation logic**, not feature values.

Historical values are intentionally retained so old training datasets and model
versions remain reproducible. A new feature version should normally be created
when feature logic changes materially instead of silently overwriting old history.

---

## CLI overview

`fsctl` (also `python -m fintech_feature_platform.cli.fsctl`) provides:

- `init` — scaffold an external Feature Project;
- `validate` — validate registry/project configuration and compute bundle metadata;
- `test` — execute golden cases through the real compute core;
- `run-local` — compute a feature locally during development;
- `ingest` — land canonical raw-report JSONL and create a source dataset manifest;
- `materialize` — execute one durable multi-manifest batch job for a feature DAG;
- `build-training-dataset` — build a point-in-time-safe training dataset;
- `publish` — build the immutable feature artifact;
- `image-context` — prepare the verified worker-image build context;
- `promote` / `rollback` — govern release pointers.

Data-workflow commands use the public HTTP API and authenticate via
`CLEARFEATURE_API_KEY`.

---

## Architecture at a glance

ClearFeature uses single-purpose services around one shared compute core:

```text
                         HTTP API
                            │
             ┌──────────────┼──────────────┐
             │              │              │
             ▼              ▼              ▼
       online worker   batch worker   propagation worker
             │              │              │
             └──────────────┼──────────────┘
                            │
                       Compute Core
                            │
       ┌────────────────────┼────────────────────┐
       ▼                    ▼                    ▼
 PostgreSQL              Valkey               MinIO
 history + metadata      latest online        raw reports
                            │
                       Redpanda/Kafka
                    events and references
```

Workers use the same feature artifact and registry. Raw reports land once in
object storage; landed batch workflows pass references through the broker rather
than duplicating raw payloads.

See [Architecture one-pager](docs/product/architecture_one_pager.md).

---

## Point-in-time correctness

ClearFeature keeps four clocks separate:

```text
report_ts / data_ts  = what time the data describes
available_at         = when your system could actually know it
calc_ts              = when ClearFeature computed the feature
observation_ts       = the historical decision / label time
```

A training value is eligible only if it belongs to the past **and** was available
by the observation time. This prevents a historical training row from using data
that arrived later.

See the [Feature Project Quickstart](docs/20_feature_project_quickstart.md) for a
small PIT-safe example.

---

## Supported Python

Python **3.12** (declared in `pyproject.toml`; CI runs 3.12).

---

## Run the stack — operate ClearFeature

For platform operators:

```bash
cp .env.example .env

# API auth is fail-closed: configure an operator key before startup.
export FSP_API_KEYS='[{"key_id":"ops","role":"operator","secret":"'"$(openssl rand -hex 32)"'"}]'

docker compose up -d --wait

curl -s localhost:8000/health
curl -s localhost:8000/ready

docker compose down
```

Use `docker compose down -v` only when you intentionally want to reset local
PostgreSQL, MinIO, Valkey and Redpanda data.

For an external Feature Project, the quickstart shows how to build its runtime
image and bind all ClearFeature application services to that same image.

---

## Authentication

Every endpoint except public probes `GET /health` and `GET /ready` requires:

```text
Authorization: Bearer <api-key>
```

Roles:

- `service` — data-plane service access;
- `operator` — superset used by administrative/data-workflow commands.

The API refuses to start in the default API-key mode without `FSP_API_KEYS`.

Never commit API keys to `feature_project.yaml`, registry files, or scripts.

See [Minimum security](docs/security/minimum_security.md).

---

## Health, readiness, metrics and logging

- `GET /health` — process liveness;
- `GET /ready` — role-aware dependency readiness (`200` / `503`);
- `GET /metrics` — Prometheus metrics when the observability port is enabled;
- structured JSON logs via `FSP_LOG_FORMAT` / `FSP_LOG_LEVEL`.

See the full [observability contract](docs/21_observability_contract.md).

---

## Community trust and security boundaries

Community intentionally keeps the execution model simple:

- Feature/UDF authors are **trusted**; Python UDFs execute in-process with worker
  permissions and are **not sandboxed**.
- One compatible Feature Project/package environment is loaded per worker image.
- Artifact binding is fail-closed for governed deployments.
- Canonical JSONL preparation/schema adaptation is owned by the caller or an
  upstream adapter; Community does not infer arbitrary business schemas.

---

## Current Community / MVP boundaries

ClearFeature Community currently targets a practical single-node / pilot workflow:

- Docker Compose deployment; no built-in HA/Kubernetes orchestration;
- scale-out through worker replicas rather than a built-in cluster manager;
- canonical JSONL ingestion rather than schema inference;
- very large ingestion/export workflows may require dataset partitioning;
- intermediate dependencies are not persisted unless requested as outputs;
- bring-your-own dashboards and alerting;
- trusted, non-sandboxed Python UDFs.

For the precise supported/non-goal contract, see:

- [Known limitations](docs/beta_acceptance/known_limitations.md)
- [Non-goals](docs/beta_acceptance/non_goals.md)

---

## Documentation

### Use ClearFeature

- **[5-minute Feature Project Quickstart](docs/20_feature_project_quickstart.md)** —
  JSON → Python/YAML features → batch → PIT training → online.
- [Credit decision demo](docs/demo/credit_decision_demo.md)

### Operate ClearFeature

- [Deployment](docs/deployment/)
- [Runbooks](docs/runbooks/)
- [Observability](docs/21_observability_contract.md)
- [Security](docs/security/minimum_security.md)

### Contracts and architecture

- [API contracts](docs/03_api_contracts.md)
- [Architecture one-pager](docs/product/architecture_one_pager.md)
- [Community / Enterprise boundary](docs/architecture/community_enterprise_boundary.md)

### Release

- [Release notes](docs/beta_acceptance/release_notes.md)

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security reporting

See [SECURITY.md](SECURITY.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE). © ClearFeature Labs.
