# Minimum security

The smallest credible API-key baseline for the platform's two HTTP services. Not
enterprise IAM: no OAuth/OIDC, SSO, JWT, mTLS, or secrets-manager integration — those
are deliberate non-goals for this stage (see §10).

## 1. Trust boundaries

```text
client / operator tooling ──HTTP──> Feature API          (api container, :8000)
client (decision consumer) ─HTTP──> demo-model-service   (profile demo, :8090)
demo-model-service ────────HTTP──> Feature API           (its OWN service key)
workers <──Kafka──> broker                               (no HTTP credentials — Kafka
                                                          is a separate boundary)
Postgres/Valkey/MinIO/Redpanda                           (never exposed to clients;
                                                          host ports bind 127.0.0.1)
```

Every HTTP hop crosses a trust boundary and authenticates — **the Docker network is
not a credential**: the model service presents its own service key to the Feature API,
and each service validates against its OWN accepted-key registry (a key accepted by
one service grants nothing on the other).

## 2. Credential convention

Exactly one, on every protected endpoint of both services:

```text
Authorization: Bearer <api-key>
```

Missing/malformed/unknown key → **401** (with `WWW-Authenticate: Bearer`); valid key
with insufficient role → **403** (detail carries `key_id` + `role`, never the secret);
public endpoint → no credential.

## 3. Roles

```text
service   data-plane: feature latest/history/consistency, feature-request
          submit/compute/status, model-score writeback, credit decision
operator  SUPERSET of service, plus: metrics, values-free lineage, batch
          submission/status, source ingestion/import, manifest metadata,
          training-dataset build
```

No `admin` role: no admin HTTP endpoint exists (promotion/rollback is the `fsctl`
CLI). Adding one later means adding a role + endpoints together.

## 4. Endpoint policy matrix

Feature API:

| Path | Group |
|---|---|
| `GET /health` | public (returns only `{"status":"ok"}`) |
| `GET /ready` | public (readiness: bounded dependency categories `ok|failed|timeout` only — never secrets or exception text;) |
| `POST /v1/features/latest` · `history` · `consistency-check` | service_or_operator |
| `POST /v1/feature-requests` · `/compute` · `GET /{request_id}` | service_or_operator |
| `POST /v1/model-scores` | service_or_operator |
| `GET /v1/observability/metrics` · `POST /v1/lineage/feature-value` | operator |
| `POST /v1/batch/jobs` · `GET /v1/batch/jobs/{job_id}` | operator |
| `POST /v1/source-datasets/ingest-jsonl` · `ingest-dwh-json` · `import-dwh-features` · `GET /{manifest_id}` | operator |
| `POST /v1/training-datasets/build` | operator |

demo-model-service:

| Path | Group |
|---|---|
| `GET /health` | public (minimal) |
| `POST /v1/credit/decision` | service_or_operator |

The classification is executable, not prose: `ENDPOINT_POLICY` in
`api/app.py` / `model_service.py` is enforced at startup
(`assert_policy_complete`) — a new route without a classification refuses to boot,
and tests pin the same invariant.

## 5. Configuration

Per process (each service reads its own environment):

```text
FSP_SECURITY_MODE   api_key (default, fail-closed) | disabled (dev only)
FSP_ENVIRONMENT     development (default) | pilot | production ...
FSP_API_KEYS        JSON list: [{"key_id": "...", "role": "service|operator",
                                 "secret": "<openssl rand -hex 32>"}, ...]
```

demo-model-service additionally:

```text
FSP_API_KEYS                    its OWN accepted client keys
                                (compose maps host var FSP_MODEL_SERVICE_API_KEYS)
FSP_FEATURE_API_KEY             the service-role secret it presents upstream
FSP_DECISION_INCLUDE_FEATURES   "1" -> include the synthetic input vector in
                                responses (demo/development ONLY; default hidden —
                                not a production behavior)
```

Startup validation (any violation prevents startup): known mode; `disabled` only with
`FSP_ENVIRONMENT=development` (and prints a loud warning); `api_key` requires ≥ 1 key;
unique `key_id`s; unique secrets; secret length ≥ 32; placeholder-looking secrets
rejected. Lookup compares the presented secret against **every** configured key with
`secrets.compare_digest` (no early exit). Raw secrets are never logged, never echoed,
and never attached to requests — handlers see `key_id` + `role` only.

## 6. Key generation, rotation, revocation

```bash
openssl rand -hex 32                  # generate every secret; never commit one
```

- **Add/rotate**: append the new key object to the JSON list and restart the service —
  multiple keys are active simultaneously, so old and new overlap while clients move.
- **Revoke**: remove the object from the list and restart.
- `.env` (gitignored) carries the lists locally; `.env.example` holds only
  placeholders, which the startup validation deliberately rejects.

## 7. Compose configuration

`docker compose up` runs the `api` container in `api_key` mode by default and **fails
closed** (refuses to start) without `FSP_API_KEYS`. All repo scripts
(`run_local_backend_smoke.sh`, `run_compose_smoke.sh`, `run_credit_batch_demo.sh`,
`run_credit_online_demo.sh`, `run_single_job_scaling_bench.sh`,
`run_security_minimum_smoke.sh`) generate **ephemeral per-run keys** when none are
supplied — the demo and pilot flows exercise authentication, never bypass it.

Development bypass (the only one, rejected outside development):

```bash
FSP_SECURITY_MODE=disabled FSP_ENVIRONMENT=development docker compose up -d
```

## 8. Service-to-service authentication

`demo-model-service` calls `POST /v1/features/latest` with
`Authorization: Bearer $FSP_FEATURE_API_KEY` (a service-role key in the Feature API's
registry, e.g. `key_id=svc-model-upstream`). In `api_key` mode the service refuses to
start without it. Upstream 401/403 surfaces as a bounded 502
`{"status": "feature_api_error", "upstream_status": ...}` — never headers or bodies.

## 9. Logging and error safety

- Neither service logs request headers; uvicorn access logs carry method/path/status.
- 401/403 bodies never echo the presented or configured credentials (test-pinned).
- Unexpected errors return FastAPI's opaque 500 — no stack traces, DSNs, or SQL.
- Metrics, lineage, request/job status, and DLQ summaries remain values-free
.
- OpenAPI (`/docs`, `/redoc`, `/openapi.json`) is disabled in `api_key` mode.
- CORS stays absent — no cross-origin browser access is granted.

## 10. What a pilot still needs from the deployment (out of scope here)

- **TLS termination** in front of both services (reverse proxy — nginx/traefik/ELB);
  the services speak plain HTTP bound to 127.0.0.1/compose network.
- **Rate limiting / request size limits** at the same proxy.
- Real credentials for Postgres/MinIO in `.env` (dev defaults are placeholders).
- Enterprise IAM (OAuth/OIDC/SSO), secrets manager, mTLS, signed audit logs — future
  stages, not this baseline.

## 11. Known limitations

Keys are env-delivered (no secrets-manager or file-mount indirection); no per-key
rate limits or usage audit; single flat registry per service; restart required for
key changes; `disabled` mode exists (development-gated by startup validation).
