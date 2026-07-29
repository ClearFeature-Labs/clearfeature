# Credit-decision demo ()

A minimal international credit-decision showcase built entirely on the real platform
paths. **All data, labels, and the model are synthetic** — generated, deterministic, and
unsuitable for real lending decisions.

## Scenario

A lender receives a credit application plus four external reports (tax, credit bureau,
telco, socdem). The platform:

1. ingests 20–30k historical applications **by reference** (bulk JSONL → MinIO payloads +
   `raw_reports_meta` + one `SourceDatasetManifest` per source);
2. computes the F1 report features via **five manifest-scoped Kafka batch jobs** (one per
   source — manifest items carry one `source_ref` each, so every F1 feature reads exactly
   one source);
3. recomputes the F2 affordability layer through the **real DAG**
   (`compute_dependent_from_offline`: PIT-safe inputs, D3/D9 metadata, depth-4 graph);
4. scores the portfolio with the **F3 PD model-as-feature** (pure-Python logistic
   regression, sha256 digest pinned in the registry, vector-first digest-verifying runner);
5. refreshes the online store through **guarded Mode-2** (D9 write guard + token bucket;
   the demo runs it twice — the second pass is all `noop`, proving nothing fresher is ever
   overwritten);
6. serves `/v1/features/latest` and answers `/v1/lineage/feature-value` (model digest +
   input fingerprint + manifest report refs, values-free).

## Run it

```bash
bash scripts/run_credit_batch_demo.sh --clients 30000        # full portfolio
bash scripts/run_credit_batch_demo.sh --clients 2000         # quick run
```

The script generates data into `.demo-data/` (gitignored), rebuilds + restarts the
compose stack with the credit registry, and drives the flow host-side + through the API.
Details, contracts, and layout: `examples/credit_decision_demo/README.md` and
`examples/credit_decision_demo/schemas/report_schemas.md`.

## The registry seam (how one platform serves two contracts)
 added two backward-compatible settings:

```bash
FSP_REGISTRY_PATH=examples/credit_decision_demo/registry/credit_decision_v1.yaml
FSP_UDF_PROVIDER=examples.credit_decision_demo.features:build_registry_and_udfs
```

Unset, the stack serves the built-in demo registry exactly as before. Set (the demo
script exports them), the same API + workers serve the credit contract — the image ships
`examples/`, so **rebuilding the image is required** when the demo package changes.

## Synthetic population (7 correlated segments)

LOW_RISK · MEDIUM_RISK · HIGH_RISK · THIN_FILE · RECENT_DELINQUENCY ·
HIGH_INCOME_HIGH_DEBT · UNSTABLE_INCOME — internally coherent (income drives debt
capacity and telco top-ups; delinquency segments carry fresh DPD events; thin files have
short histories, not doom). The default label is drawn from hidden latent risk + noise.
Golden examples per segment live in `fixtures/golden_features.json`; risk ordering is
test-asserted (LOW_RISK pd ≈ 0.014 < UNSTABLE_INCOME ≈ 0.14 < HIGH_RISK ≈ 0.29 <
RECENT_DELINQUENCY ≈ 0.39 on the fixture).

## Model

Deterministic pure-Python logistic regression (no sklearn/numpy): trained on 2,000
generated clients (seed 4242) whose features are computed through the **real
ComputeCore**; holdout ROC AUC ≈ 0.84 vs 0.78 for a bureau-score-only baseline
(`model/metadata.json`). Retraining reproduces the committed artifact bit-for-bit; the
registry pins its digest, and the runner refuses a mismatched artifact.

## The online half

An external scorer that shows what a client's decision service looks like on top of the
platform:

```text
POST demo-model-service /v1/credit/decision {user_id, application_id}
  -> Feature API /v1/features/latest  (Valkey latest, produced by the Kafka-first
     batch flow + guarded Mode-2 projection — never a direct store read)
  -> the SAME committed artifact as batch F3 (DemoPdModelRunner; registry digest pin
     verified at service startup and on every predict)
  -> {pd_score, decision, model digest/uri, feature vector used}
```

- **One scorer, two paths**: batch F3 and the online service share the artifact loader
  and `predict_proba` — there is no second model implementation; online pd equals the
  batch pd exactly for the same feature state.
- **Decision policy** (deterministic demo contract): pd < 0.10 → `approve`,
  pd < 0.30 → `review`, else `decline`.
- **Explicit failures**: missing or stale required features → controlled 409 with a
  machine-readable status (`missing_features` / `stale_features`); Feature API down →
  502 `feature_api_unavailable`. Never silent defaults.
- **Boundary**: the service gets no store/broker credentials in compose and imports no
  store clients (test-pinned) — it depends on the Feature API HTTP contract only, and
  the decision path publishes nothing to Kafka (watermark-checked in the demo).
- **Runtime**: compose service `demo-model-service` (same `fsp-app` image), started
  only with `docker compose --profile demo up`; port `127.0.0.1:8090`.
- **Authentication **: both services run fail-closed API-key auth
  (`Authorization: Bearer`). The demo scripts generate ephemeral keys per run: an
  operator key for the driver, a service key the model service presents to the
  Feature API, and a separate client key for `/v1/credit/decision` (the model service
  has its own registry). The decision response includes the synthetic feature vector
  only because the demo sets `FSP_DECISION_INCLUDE_FEATURES=1` — hidden by default,
  not a production behavior. Keys guide: `docs/security/minimum_security.md`.

One command (batch flow + online validation + evidence under
`artifacts/credit_online_demo/`):

```bash
bash scripts/run_credit_online_demo.sh            # fresh volumes, 4 batch workers
bash scripts/run_credit_online_demo.sh --skip-batch --keep-running   # reuse state
```
