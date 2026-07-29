# Non-goals — verified absent

docs/13 §13 defines what beta must NOT contain. This pack verifies nothing snuck in;
each item is backed by a code-level check (acceptance runner and/or
`tests/acceptance/test_beta_acceptance_pack.py`).

| Non-goal | Verified by |
|---|---|
| **Online F3** (model inference in the online path) | planner rejects direct + transitive F3 (runner A8–A9, C8); `online_worker.py` contains no `ModelRunner` reference (test) |
| **Spark / Ray runtime** | no `pyspark`/`ray` import anywhere in `src` (test grep); T1 trigger checklist gates future adoption |
| **ClickHouse / Iceberg backend** | no `clickhouse` import in `src` (test grep); T3 trigger gates adoption |
| **AutoML / model training platform** | `fs_core/training.py` builds PIT *datasets*; no training/fit code exists; models arrive pinned by URI+digest |
| **UI / feature marketplace** | no frontend/UI code; catalog remains read-only API surface |
| **Arbitrary online SQL** | DWH is an ingestion boundary (invariant I5): `online_worker.py` has no DWH/SQL reference (test); workers never execute SQL from requests |
| **Raw payloads in Kafka** (beta-scale path) | dataset-scoped chunk events carry `source_refs` only (runner D1); compute events carry report *descriptors*; the capped inline mode is a documented alpha convenience, not the scale path |
| **Feature values / payloads in status, metrics, lineage, or propagation events** | `FeatureUpdated` dataclass has no value/payload/storage field (test); metric labels reject payload-like content; lineage output scanned values-free (runner A14, F2); acceptance output itself scanned for leaks (test) |
| **Hosted registry service / approval-API integration** | bundles + pointers are local filesystem; no HTTP registry service exists |

Grep spot-checks a reviewer can run:

```bash
rg -n "import (pyspark|ray|clickhouse)" src            # expect: no hits
rg -n "ModelRunner|DwhReader" src/fintech_feature_platform/api/online_worker.py  # no hits
rg -n '"payload_json"|"object_key"|"storage_uri"' src/fintech_feature_platform/fs_core/observability  # no hits
```
