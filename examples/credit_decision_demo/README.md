# Credit-decision demo

A minimal international credit-decision showcase on the real Feature Platform paths.
**Everything here is synthetic** — data, labels, and model are generated and unsuitable
for real lending decisions.

## Story

A lender receives an application plus four external reports (tax, credit bureau, telco,
socdem). The platform ingests 20–30k historical applications by reference, computes the
F1 report features and the F2 affordability layer through the real DAG, scores the
portfolio with a digest-pinned F3 PD model, refreshes the online store through the
guarded Mode-2 path, and answers lineage for any score.

## Layout

| path | what |
|---|---|
| `generator.py` / `generate_data.py` | deterministic synthetic population (7 correlated segments) |
| `schemas/report_schemas.md` | the five report contracts |
| `registry/credit_decision_v1.yaml` | one view: 22 F1 + 8 F2 + F3 `pd_score` (depth-4 DAG) |
| `features.py` | UDFs + the `FSP_UDF_PROVIDER` entrypoint |
| `model_lib.py` / `train_model.py` | pure-Python deterministic logistic regression |
| `model/artifact.json` | trained weights; sha256 digest pinned in the registry |
| `model_runner.py` | digest-verifying vector-first `ModelRunner` |
| `flow.py` / `run_batch.py` | batch flow over the real platform seams |
| `fixtures/` | committed 150-client fixture (seed 7) + golden expectations |

## Run it (Docker)

```bash
bash scripts/run_credit_batch_demo.sh --clients 30000
```

This generates data into `.demo-data/` (gitignored), restarts the compose stack with the
credit registry (`FSP_REGISTRY_PATH`/`FSP_UDF_PROVIDER` seam), and drives:
bulk JSONL ingestion → five manifest-scoped Kafka batch jobs (F1) → F2 DAG recompute →
F3 portfolio scoring → guarded Mode-2 refresh (run twice: second pass is all `noop`) →
`/v1/features/latest` + `/v1/lineage/feature-value` through the API.

## Model

`uv run python examples/credit_decision_demo/train_model.py` retrains deterministically
(2000 clients, seed 4242, features computed via the real ComputeCore) and must reproduce
the committed artifact digest bit-for-bit. Holdout metrics (see `model/metadata.json`):
ROC AUC ≈ 0.84 vs 0.78 for the bureau-score-only baseline — the multi-source features
carry real lift, but model quality is deliberately not the point of this demo.
