# Community Observability Contract

The final Community-edition observability surface. Community provides
**infrastructure-level observability primitives** — the stable technical interfaces a
user needs to plug the platform into their own monitoring stack. Community does NOT
bundle a monitoring product.

## 1. Endpoint contract

| Process | Endpoint | Purpose | Enablement | Status semantics | Auth |
|---|---|---|---|---|---|
| API | `GET /health` | **Liveness**: process is up and serving HTTP | always | 200 `{status: ok}` | PUBLIC |
| API | `GET /ready` | **Readiness**: can this process perform its role (dependency capability) | always | 200 ready / 503 not_ready; body `{status, role, checks}` | PUBLIC (bounded categories only, no secrets) |
| API | `GET /metrics` | Prometheus text exposition (process-local registry) | observability port > 0 | 200 | internal port, not published |
| API | `GET /v1/observability/metrics` | legacy bounded JSON snapshot | always | 200 | operator key |
| workers (×6) | `GET /ready` | role readiness | observability port > 0 | 200 / 503, same schema as API | internal port |
| workers (×6) | `GET /metrics` | Prometheus text exposition | observability port > 0 | 200 | internal port |
| workers | liveness | process supervision (compose/systemd/orchestrator) | n/a | process state | n/a |

The three concepts are never conflated: `/health` = alive, `/ready` = able to perform
the role **right now** (never queue depth / last-success / throughput — an idle worker
with an empty queue is READY), `/metrics` = aggregate export.

## 2. Observability port

One lightweight process-local HTTP server (stdlib wsgiref + prometheus_client WSGI, no
second web framework) serves `/metrics` **and** `/ready` per process.

- **`FSP_OBSERVABILITY_PORT`** — primary setting. `0` (default) = server disabled:
  the process exposes neither endpoint.
- **`FSP_METRICS_PORT`** — retained backward-compatibility fallback with identical
  semantics. Precedence: `FSP_OBSERVABILITY_PORT` > `FSP_METRICS_PORT` > `0`.
  Empty values count as unset.

Readiness and metrics are logically separate **capabilities** that deliberately share
this one port: `/metrics` on an unscraped internal port costs nothing, so a deployment
that wants orchestrated readiness but no Prometheus simply enables the port and never
scrapes. This is the documented deployment contract (a deliberate release decision; it
resolves the earlier "metrics port gates readiness" naming coupling without a second
server or extra toggles). `docker-compose.yml` passes `FSP_OBSERVABILITY_PORT`
through to every app service (default `0`; no host port is ever published).

## 3. Readiness semantics

- Role × dependency matrix derived from actual handler store usage
  (`api/readiness.py::ROLE_DEPENDENCIES`); best-effort stores are not readiness
  dependencies.
- Probes are side-effect free, use probe-only clients built once at backend
  construction, and obey the **readiness deadline contract**: every probe terminates
  within a few seconds (valkey/minio 2 s + no retries; postgres 2 s
  checkout/connect/statement bounds + short keepalives; kafka 2 s librdkafka).
  Data-plane clients keep their own timeout/retry policy — the two contracts are
  never conflated.
- Per-dependency single-flight: ≤1 concurrent probe call and ≤1 live probe thread per
  dependency; the HTTP response is bounded (~2 s join per dependency) even while
  everything hangs.
- Failure → 503 without a crash; recovery → 200 (measured restore→READY ≤ 0.84 s).
- Responses carry stable dependency categories and `ok|failed|timeout` only — never
  exception text, endpoints, or credentials.

## 4. Metrics contract

Central catalog (`fs_core/observability/catalog.py`, 32 families) covering: process
identity, worker items/outcomes/last-success, online operation latency, batch chunk
latency, pipeline stages (compute includes nested lazy input_fetch — quantiles must
never be subtracted), per-feature compute timing, API request metrics
(framework-resolved route template only), D9 outcomes, artifact verification, storage
operations. All label domains are closed; the registry-bound `feature_id` is the only
approved dynamic dimension; user/entity/request/job identifiers never enter labels.

## 5. Logging contract

Structured JSON service logs (one object per line: `timestamp, level, event, service`
+ bounded fields) on the `fsp` namespace logger; uvicorn lifecycle/errors adopted into
the same envelope (`runtime_log`, tracebacks preserved), access log dropped in json
mode. `FSP_LOG_FORMAT=text` for developers; `FSP_LOG_LEVEL` (default INFO). Levels:
routine per-item/per-request success = DEBUG (Prometheus owns aggregates); summaries/
lifecycle/recovery = INFO; retry/degraded/readiness-failed/security-disabled =
WARNING; failed items/DLQ/5xx/verification failures/runtime exceptions = ERROR.
Technical correlation ids (`request_id`/`job_id`/`chunk_id`/`correlation_id`) appear
where those concepts exist; entity keys, payloads, values, and secrets never do.
`trace_id`/`span_id` are reserved field names — future OpenTelemetry adds them in one
formatter location without changing the event schema; trace context would propagate
via Kafka **headers** (the `x-fsp-attempt` channel already round-trips), never via
event payload JSON.

## 6. Community vs Enterprise / post-MVP boundary

**Community (this contract, complete):** `/health`, `/ready`, `/metrics`, the
documented metric catalog, structured JSON logs, feature-latency metrics,
worker/platform/storage metrics. Raw interfaces are full-fidelity — nothing is
artificially crippled.

**Enterprise / post-MVP (explicitly deferred from):** Grafana dashboard
packs, alert packs, SLO/SLA tooling, fleet/multi-environment monitoring, managed
telemetry, advanced operational UI, OpenTelemetry tracing, Redpanda consumer-lag
exposure, compose `monitoring` profile.

## 7. Resource ownership & known limitations

Probe-only clients (valkey probe client, minio probe client, `max_size=1/min_size=0`
readiness pool) and the observability server are created once per process at backend
construction/startup and live for the process lifetime; shutdown is process exit
(server thread is a daemon; `close()` on the server and readiness pool is idempotent —
tested). `AppBackend` has no aggregate close API by design (one backend per process;
documented MVP limitation). Other known limitations: output emitted before Python
logging exists stays plaintext; unhandled API 500s surface as `runtime_log` ERROR
(not `api_request_completed`); per-item success logs require `FSP_LOG_LEVEL=DEBUG`;
compose healthchecks still probe `/health` (liveness) — wiring orchestration to
`/ready` is deployment work for the monitoring profile.
