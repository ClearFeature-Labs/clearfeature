# ClearFeature in 5 Minutes

**From raw JSON to the same tested features in batch and online inference.**

ClearFeature lets you define feature logic **once** in Python, declare dependencies **once** in YAML, and run the same DAG for:

- historical batch materialization;
- point-in-time training datasets;
- online inference.

That is the core value of a feature platform: **one feature definition, consistent everywhere**.

The historical happy path is intentionally small:

```text
fsctl init
edit features.py + registry.yaml + golden.yaml
fsctl validate
fsctl test
fsctl ingest            # once per raw source
fsctl materialize
fsctl build-training-dataset
```

Everything below uses the **public CLI and HTTP API**. As a feature author you do not
query Postgres/MinIO directly, import platform internals, or join source manifests
client-side.

---

## How the pieces fit together

Keep the platform and your features separate:

```text
~/projects/
├── clearfeature/          # ClearFeature Core: API, workers, storage integrations
└── snapshot-ratio/        # your Feature Project: code, registry, tests
```

`clearfeature/` is the reusable engine. Your Feature Project contains your business feature logic.

At deployment time they are packaged together:

```text
ClearFeature Core
      +
Feature Project
      =
Deployable Feature Runtime
```

The same runtime image is used by the API, batch worker and online worker, so batch and online execute the **same feature code and registry**.

### How many Feature Projects should I create?

Do **not** create one project per feature. Use one project for a logically related domain, team, or model family, for example:

```text
risk-features/
fraud-features/
marketing-features/
```

A Feature Project can contain hundreds of related features.

This quickstart uses **one Feature Project per ClearFeature runtime**. If different domains later need independent releases or isolation, run separate deployments or combine them into a larger shared project.

---

## 0. Prerequisites

You need:

- Docker
- Python 3.12+
- `uv`
- a local ClearFeature checkout

From the ClearFeature repository:

```bash
uv sync
source .venv/bin/activate
```

---

## 1. Create a Feature Project

A Feature Project contains your feature code, registry and tests, while the ClearFeature platform itself stays unchanged.

Think of it as the deployable unit for one related feature domain.

```bash
fsctl init \
  --name snapshot-ratio \
  --dir ../snapshot-ratio \
  --package snapshot_ratio

cd ../snapshot-ratio
uv pip install -e .
```

The important files are:

```text
snapshot-ratio/
├── feature_project.yaml
└── snapshot_ratio/
    ├── features.py
    ├── registry/features_v1.yaml
    └── tests/golden.yaml
```

---

## 2. Write the features

Suppose we have two raw reports:

- `credit_bureau_report`
- `socdem_report`

and want:

- `active_monthly_payment`
- `monthly_income`
- `payment_to_income_ratio`

Replace `snapshot_ratio/features.py` with:

```python
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry


def active_monthly_payment(sources, deps):
    loans = sources["credit_bureau_report"]["loans"]
    return round(
        sum(x["monthly_payment"] for x in loans if x["status"] == "active"),
        2,
    )


def monthly_income(sources, deps):
    return float(sources["socdem_report"]["monthly_income"])


def payment_to_income_ratio(sources, deps):
    return round(
        deps["active_monthly_payment"] / deps["monthly_income"],
        6,
    )


def build_udfs():
    return UdfRegistry({
        "udf.demo.active_monthly_payment": active_monthly_payment,
        "udf.demo.monthly_income": monthly_income,
        "udf.demo.payment_to_income_ratio": payment_to_income_ratio,
    })
```

A feature is just:

```text
(sources, deps) -> value
```

Direct features read raw sources. Dependent features read other features.

**You never call feature functions manually.** ClearFeature reads the registry, builds the dependency DAG and calls functions in the correct order for both batch and online requests.

---

## 3. Declare the DAG

Replace `snapshot_ratio/registry/features_v1.yaml` with:

```yaml
registry_version: "demo-v1"

entities:
  application_snapshot:
    key_fields: ["user_id", "application_id", "report_id"]

sources:
  credit_bureau_report:
    type: "raw_report"
    report_type: "credit_bureau_report"
    ts_field: "report_ts"

  socdem_report:
    type: "raw_report"
    report_type: "socdem_report"
    ts_field: "report_ts"

feature_views:
  credit_model_inputs:
    entity: "application_snapshot"
    key_fields: ["user_id", "application_id", "report_id"]
    view_version: 1
    owner: "demo"
    status: "active"

    feature_groups:
      model_inputs:
        - active_monthly_payment
        - monthly_income
        - payment_to_income_ratio

    features:
      active_monthly_payment:
        kind: "udf"
        feature_version: 1
        udf: "udf.demo.active_monthly_payment"
        inputs: ["credit_bureau_report"]
        dtype: "float"
        status: "live"

      monthly_income:
        kind: "udf"
        feature_version: 1
        udf: "udf.demo.monthly_income"
        inputs: ["socdem_report"]
        dtype: "float"
        status: "live"

      payment_to_income_ratio:
        kind: "udf"
        feature_version: 1
        udf: "udf.demo.payment_to_income_ratio"
        inputs: []
        deps:
          - feature: active_monthly_payment
            version: 1
          - feature: monthly_income
            version: 1
        dtype: "float"
        status: "live"
```

The platform now knows this DAG:

```text
credit_bureau_report
        ↓
active_monthly_payment ──┐
                         ├──→ payment_to_income_ratio
monthly_income ──────────┘
        ↑
socdem_report
```

The entity key is the **complete key** — all declared fields, in this order.
Multi-source batch joins never use only a subset of it.

Add three tiny golden tests in `snapshot_ratio/tests/golden.yaml`:

```yaml
cases:
  - name: active_payment
    feature: active_monthly_payment
    sources:
      credit_bureau_report:
        loans:
          - {status: active, monthly_payment: 500.0}
          - {status: closed, monthly_payment: 999.0}
          - {status: active, monthly_payment: 250.0}
        report_ts: "2026-08-13T10:00:00+00:00"
    expected: {value: 750.0}

  - name: income
    feature: monthly_income
    sources:
      socdem_report:
        monthly_income: 3000.0
        report_ts: "2026-08-13T10:00:00+00:00"
    expected: {value: 3000.0}

  - name: ratio
    feature: payment_to_income_ratio
    deps: {active_monthly_payment: 750.0, monthly_income: 3000.0}
    expected: {value: 0.25}
```

Validate and run the golden cases:

```bash
fsctl validate
fsctl test
```

You want `valid: true` and all tests passing. Golden cases run through the real
compute core, so they test the same feature logic workers will execute.

---

## 4. Prepare raw JSONL

ClearFeature ingests **canonical JSONL**: one canonical JSON object per line.
It does not infer arbitrary business JSON schemas; adapt your source data before ingest.

Each row uses:

- `entity_key` — the complete canonical key;
- `event_ts` — required business/as-of timestamp;
- `payload` — the raw report stored in object storage;
- `available_at` — optional trusted availability time;
- `report_ref` — optional stable report id (otherwise content-addressed).

```bash
mkdir -p data output
```

`data/credit.jsonl`:

```json
{"entity_key":{"user_id":"u1","application_id":"a1","report_id":"r1"},"event_ts":"2026-08-13T10:00:00Z","available_at":"2026-08-13T10:01:00Z","payload":{"loans":[{"status":"active","monthly_payment":500.0},{"status":"active","monthly_payment":250.0},{"status":"closed","monthly_payment":1000.0}],"report_ts":"2026-08-13T10:00:00Z"}}
```

`data/socdem.jsonl`:

```json
{"entity_key":{"user_id":"u1","application_id":"a1","report_id":"r1"},"event_ts":"2026-08-13T10:00:00Z","available_at":"2026-08-13T10:01:00Z","payload":{"monthly_income":3000.0,"report_ts":"2026-08-13T10:00:00Z"}}
```

Expected result:

```text
active_monthly_payment  = 750
monthly_income          = 3000
payment_to_income_ratio = 0.25
```

`available_at` matters for point-in-time correctness:

```text
event_ts      = when the source says the data happened
available_at  = when your system could actually know it
```

Training uses `available_at` so a model cannot accidentally see future data.

---

## 5. Build and start ClearFeature

Package the Feature Project.

`publish` creates a versioned artifact of your tested feature code. `image-context` prepares a Docker image that combines that artifact and registry with ClearFeature Core:

```bash
fsctl publish --project .

fsctl image-context \
  --project . \
  --dir ./ctx \
  --base-image fsp-app:latest
```

Build the platform image:

```bash
cd ../clearfeature
docker build -t fsp-app:latest .
```

Build your Feature Project runtime:

```bash
cd ../snapshot-ratio
docker build -t snapshot-ratio-worker:dev ctx/
```

Create `clearfeature-external.yml` inside the **ClearFeature checkout**.

The base `docker-compose.yml` defines the platform services. This small override tells every application service to use the **same Feature Project image**, so API, batch and online cannot accidentally run different feature code:

```yaml
services:
  api: &feature_project
    image: snapshot-ratio-worker:dev
    build: !reset null
    environment:
      FSP_REGISTRY_PATH: /etc/clearfeature/registry.yaml
      FSP_UDF_PROVIDER: snapshot_ratio.features:build_udfs

  online-worker: *feature_project
  offline-writer: *feature_project
  metadata-writer: *feature_project
  model-score-writer: *feature_project
  batch-worker: *feature_project
  propagation-worker: *feature_project
```

Start the stack:

```bash
cd ../clearfeature

export CF_KEY="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
export FSP_API_KEYS="[{\"key_id\":\"demo\",\"role\":\"operator\",\"secret\":\"$CF_KEY\"}]"

export CLEARFEATURE_API_URL=http://127.0.0.1:8000
export CLEARFEATURE_API_KEY="$CF_KEY"

docker compose \
  -f docker-compose.yml \
  -f clearfeature-external.yml \
  up -d
```

Check readiness:

```bash
curl -s http://127.0.0.1:8000/ready
```

You should see:

```json
{"status":"ready"}
```

### Where does data live?

```text
Raw reports       → MinIO
Feature history   → Postgres
Latest online     → Valkey
Events / workers  → Kafka / Redpanda
```

`features.py` stores **calculation logic**, not feature values.

Postgres keeps historical feature values for reproducible training. Valkey keeps the latest online values for low-latency access.

---

## 6. Batch: ingest and materialize

Go back to the Feature Project:

```bash
cd ../snapshot-ratio
```

Ingest both sources:

```bash
fsctl ingest \
  --entity-type application_snapshot \
  --source-name credit_bureau_report \
  --report-type credit_bureau_report \
  --input data/credit.jsonl \
  --output output/credit_manifest.json

fsctl ingest \
  --entity-type application_snapshot \
  --source-name socdem_report \
  --report-type socdem_report \
  --input data/socdem.jsonl \
  --output output/socdem_manifest.json
```

Both should report:

```text
item_count_written = 1
item_count_rejected = 0
```

Each ingest creates one durable `SourceDatasetManifest`. Re-ingesting identical
content is safe: it is counted as `duplicate`, not written again.

Get the manifest IDs:

```bash
CREDIT_MANIFEST=$(python3 -c \
'import json; print(json.load(open("output/credit_manifest.json"))["manifest_id"])')

SOCDEM_MANIFEST=$(python3 -c \
'import json; print(json.load(open("output/socdem_manifest.json"))["manifest_id"])')
```

Now materialize the **entire F1 + F2 DAG with one command**:

```bash
fsctl materialize \
  --view credit_model_inputs \
  --view-version 1 \
  --manifest-id "$CREDIT_MANIFEST" \
  --manifest-id "$SOCDEM_MANIFEST" \
  --feature-group model_inputs \
  --wait
```

You want:

```text
status = completed
failed_items = 0
```

Under the hood ClearFeature joins both source manifests by the complete entity key,
loads raw reports by reference, computes F1 features, resolves the F2 dependency,
and writes feature history. Raw payloads do not need to travel through Kafka.

No custom F2 orchestration script is needed.

Three useful guarantees:

- every source required by the requested DAG is mandatory per entity; missing data
  becomes an explicit item error instead of a silent null;
- identical materialization requests are idempotent and return the same durable job;
- CLI exit codes are `0` = completed, `2` = completed with item errors, `1` =
  request/auth/infrastructure failure.

---

## 7. Build a training dataset

Create `data/observations.jsonl`:

```json
{"entity":{"user_id":"u1","application_id":"a1","report_id":"r1"},"observation_ts":"2026-08-13T10:02:00Z"}
```

Build a point-in-time-safe dataset:

```bash
fsctl build-training-dataset \
  --view credit_model_inputs \
  --view-version 1 \
  --feature active_monthly_payment \
  --feature monthly_income \
  --feature payment_to_income_ratio \
  --observations data/observations.jsonl \
  --output output/training.json
```

ClearFeature selects the latest feature values that were **actually available at each observation time**.

The four timestamps have different jobs:

```text
data_ts / report_ts  = what time the data describes
available_at         = when your system could know it
calc_ts              = when ClearFeature computed the value
observation_ts       = the historical decision/label time
```

A value must both belong to the past and have been available by `observation_ts`.
If needed, `--safety-gap-seconds` adds a conservative pipeline delay.

You now have the same feature definitions ready for model training.

---

## 8. Online: compute the same features

Online inference is **request-triggered**, not continuous source streaming: send one
snapshot's raw reports inline and ask for the output vector you need.

The request is executed with the **same registry and Python feature code** used by batch.
For online requests, `available_at` is server-stamped at accept time; callers cannot
backdate availability.

```bash
curl -sS \
  -X POST \
  "$CLEARFEATURE_API_URL/v1/feature-requests/compute" \
  -H "Authorization: Bearer $CLEARFEATURE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_type": "application_snapshot",
    "entity_key": {
      "user_id": "u2",
      "application_id": "a2",
      "report_id": "r2"
    },
    "view": "credit_model_inputs",
    "view_version": 1,
    "requested_feature_groups": ["model_inputs"],
    "reports": [
      {
        "source_name": "credit_bureau_report",
        "report_type": "credit_bureau_report",
        "report_ts": "2026-08-13T12:00:00Z",
        "payload": {
          "loans": [
            {"status":"active","monthly_payment":500},
            {"status":"active","monthly_payment":250}
          ],
          "report_ts":"2026-08-13T12:00:00Z"
        }
      },
      {
        "source_name": "socdem_report",
        "report_type": "socdem_report",
        "report_ts": "2026-08-13T12:00:00Z",
        "payload": {
          "monthly_income":3000,
          "report_ts":"2026-08-13T12:00:00Z"
        }
      }
    ]
  }' | python3 -m json.tool
```

Expected values:

```text
active_monthly_payment  = 750
monthly_income          = 3000
payment_to_income_ratio = 0.25
```

If you only need F2, request:

```json
"requested_features": ["payment_to_income_ratio"]
```

ClearFeature still computes the required F1 dependencies internally, but only returns and materializes the requested output.

If you request the whole feature group, all F1 + F2 outputs are returned and materialized. Dependencies and public outputs are intentionally separate.

---

## 9. Versions and local cleanup

Feature values are data; `features.py` is code.

When feature logic changes materially, create a **new feature version** instead of
silently replacing old history. Historical values remain in Postgres so old models
and training datasets stay reproducible, while Valkey serves the latest online values.

Removing a feature from a new registry stops new computation; it does **not**
automatically delete old historical rows.

For a completely clean local reset:

```bash
cd ../clearfeature

docker compose \
  -f docker-compose.yml \
  -f clearfeature-external.yml \
  down -v
```

This removes the local Postgres, MinIO, Valkey and Redpanda volumes.

For production, use immutable image tags/digests and the governed
`publish → promote → deploy/restart` release flow rather than local `:dev` images.

---

## 10. Troubleshooting

- **`401` / `403`** — check `CLEARFEATURE_API_KEY` and that the server key has
  `operator` role.
- **`invalid JSONL ... at line N`** — fix that local line; nothing is submitted.
- **`item_count_rejected > 0`** — inspect canonical timestamps/keys; common causes
  are invalid timezone timestamps or trusted `available_at < event_ts`.
- **`materialize` exits `2`** — read `first_errors`; usually one entity is missing
  a source required by the DAG.
- **training values are unexpectedly missing** — historical data without trusted
  `available_at` is considered available only from ingestion time.
- **cannot reach the API** — verify `CLEARFEATURE_API_URL` and
  `curl $CLEARFEATURE_API_URL/ready`.

### Important Community boundaries

Keep these four rules in mind:

1. ingestion expects canonical JSONL; schema inference is outside the platform;
2. intermediate dependencies are not persisted unless requested as outputs;
3. Community UDFs are trusted Python and are not sandboxed;
4. one compatible Feature Project/package environment is loaded per worker image.

---

# Done

You wrote only:

```text
Python feature logic
+
YAML feature DAG
```

ClearFeature handled:

```text
raw JSON
   ↓
ingestion + storage
   ↓
same feature DAG
   ↓
┌───────────────────────┐
│                       │
batch                 online
│                       │
training data        live features
└───────────────────────┘
```

**One feature definition. One DAG. Same logic for training and inference.**

That is the point of the feature platform.
