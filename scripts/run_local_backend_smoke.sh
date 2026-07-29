#!/usr/bin/env bash
#
# Real local backend smoke  — the one-command proof that the production-light
# Compose runtime works end to end through REAL containers:
#
#   Postgres + Valkey + MinIO + Redpanda + API + six --forever workers
#
# It starts the stack, verifies health + schema, pushes a real request through the
# Kafka-first chain (API -> Redpanda -> online-worker -> Valkey -> offline event ->
# offline-writer -> Postgres), reads the value back online, checks offline history in
# Postgres, probes metrics + lineage (values-free), and confirms workers are not
# restart-looping. Prints a PASS/FAIL summary; exits non-zero on failure.
#
# NOT part of `make verify` (which stays Docker-free).
#
# Usage:
#   bash scripts/run_local_backend_smoke.sh [flags]
#
# Flags:
#   --no-build            skip --build on compose up (reuse existing image)
#   --keep-running        leave the stack running after a PASS (default: compose stop)
#   --down                remove containers after a PASS (volumes always kept)
#   --clean-volumes       DESTRUCTIVE: `docker compose down -v` FIRST, so Postgres
#                         initdb re-applies infra/postgres/*.sql on a fresh volume
#                         (the "clean migration smoke"). Never done by default.
#   --timeout-seconds N   per-wait timeout budget (default 180)
#
# Safe defaults: volumes are never deleted; on FAILURE the stack is left running for
# debugging (`docker compose logs <service>`).

set -uo pipefail

cd "$(dirname "$0")/.."

BUILD=1
KEEP_RUNNING=0
DOWN_AFTER=0
CLEAN_VOLUMES=0
TIMEOUT_SECONDS=180

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-build) BUILD=0 ;;
        --keep-running) KEEP_RUNNING=1 ;;
        --down) DOWN_AFTER=1 ;;
        --clean-volumes) CLEAN_VOLUMES=1 ;;
        --timeout-seconds) shift; TIMEOUT_SECONDS="${1:?--timeout-seconds needs a value}" ;;
        *) echo "unknown flag: $1"; exit 2 ;;
    esac
    shift
done

API_PORT="${API_PORT:-8000}"
API="http://127.0.0.1:${API_PORT}"

#: the stack is fail-closed (api_key mode). Unless the caller supplies
# FSP_API_KEYS + FSP_CLIENT_API_KEY, generate an EPHEMERAL operator key for this run
# (operator is a superset of service, so one key drives the whole smoke).
if [[ -z "${FSP_API_KEYS:-}" ]]; then
    EPHEMERAL_KEY="$(openssl rand -hex 32)"
    export FSP_API_KEYS="[{\"key_id\":\"ops-smoke\",\"role\":\"operator\",\"secret\":\"${EPHEMERAL_KEY}\"}]"
    export FSP_CLIENT_API_KEY="${EPHEMERAL_KEY}"
fi
AUTH=(-H "Authorization: Bearer ${FSP_CLIENT_API_KEY:?FSP_CLIENT_API_KEY must be set when FSP_API_KEYS is supplied}")

SMOKE_USER="smoke_0072_user"
SMOKE_APP="smoke_0072_app"
ENTITY_ENCODED="user_id=${SMOKE_USER}|application_id=${SMOKE_APP}"

# Tables that infra/postgres/*.sql must have created (001..008).
REQUIRED_TABLES="raw_reports_meta features_offline feature_requests request_events \
batch_jobs batch_chunks source_dataset_manifests source_dataset_items"

WORKER_SERVICES="online-worker offline-writer metadata-writer model-score-writer \
batch-worker propagation-worker"

CHECKS=()
FAILED=0

pass() { CHECKS+=("  [PASS] $1"); echo "==> PASS: $1"; }
fail() { CHECKS+=("  [FAIL] $1"); echo "==> FAIL: $1" >&2; FAILED=1; }
note() { CHECKS+=("  $1"); echo "==> $1"; }

# Poll `eval $2` until success or the timeout budget runs out.
wait_for() {
    local label="$1" cmd="$2" deadline=$((SECONDS + TIMEOUT_SECONDS))
    while (( SECONDS < deadline )); do
        if eval "$cmd" >/dev/null 2>&1; then return 0; fi
        sleep 2
    done
    echo "timed out after ${TIMEOUT_SECONDS}s waiting for: $label" >&2
    return 1
}

psql_scalar() {  # run one SQL statement inside the postgres container, print scalar
    echo "$1" | docker compose exec -T postgres \
        sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA' 2>/dev/null
}

json_get() {  # json_get '<python-expr over d>' <<< "$json"
    python3 -c "import json,sys; d=json.load(sys.stdin); print($1)" 2>/dev/null
}

summary_and_exit() {
    echo
    if (( FAILED )); then
        echo "Real local backend smoke: FAIL"
    else
        echo "Real local backend smoke: PASS"
    fi
    echo
    echo "Checked:"
    printf '%s\n' "${CHECKS[@]}"
    if (( CLEAN_VOLUMES == 0 )); then
        echo
        echo "GATED: destructive clean-volume migration smoke not run by default" \
             "(use --clean-volumes)"
    fi
    if (( FAILED )); then
        echo
        echo "Stack left running for debugging: docker compose ps / logs <service>"
        echo "Docs: docs/deployment/local_backend_smoke.md (troubleshooting)"
        exit 1
    fi
    if (( KEEP_RUNNING )); then
        echo "Stack left running (--keep-running). Stop with: docker compose stop"
    elif (( DOWN_AFTER )); then
        docker compose down >/dev/null 2>&1
        echo "Containers removed (--down); volumes kept."
    else
        docker compose stop >/dev/null 2>&1
        echo "Stack stopped; volumes kept. Restart with: docker compose up -d"
    fi
    exit 0
}

# --- 0. docker availability ----------------------------------------------------
if ! docker info >/dev/null 2>&1; then
    echo "SKIP: local Docker daemon not available — live smoke not run."
    echo "Docker-free checks live in: uv run make verify"
    exit 1
fi

# --- 1. compose config ----------------------------------------------------------
if docker compose config --quiet; then
    pass "compose config valid"
else
    fail "compose config invalid"; summary_and_exit
fi

# --- 2. optional destructive clean (explicit flag only) --------------------------
if (( CLEAN_VOLUMES )); then
    note "[GATED->RUN] --clean-volumes: docker compose down -v (fresh initdb migration path)"
    docker compose down -v
fi

# --- 3. start stack, gated on healthchecks --------------------------------------
UP_ARGS=(up -d --wait)
(( BUILD )) && UP_ARGS=(up -d --build --wait)
if docker compose "${UP_ARGS[@]}"; then
    pass "containers up + healthchecks green (docker compose up --wait)"
else
    fail "docker compose up --wait failed"; summary_and_exit
fi
docker compose ps

# --- 4. Postgres schema (initdb runs ONLY on first empty-volume init) ------------
MISSING_TABLES=""
for table in $REQUIRED_TABLES; do
    exists=$(psql_scalar "SELECT to_regclass('public.${table}') IS NOT NULL;")
    [[ "$exists" == "t" ]] || MISSING_TABLES="$MISSING_TABLES $table"
done
if [[ -z "$MISSING_TABLES" ]]; then
    pass "Postgres schema present (8 required tables)"
else
    fail "Postgres tables missing:${MISSING_TABLES} — this volume predates the schema."
    echo "    initdb applies infra/postgres/*.sql ONLY on first init of an EMPTY volume." >&2
    echo "    Fix: bash scripts/apply_postgres_migrations.sh   (idempotent)" >&2
    echo "    Or clean smoke: bash scripts/run_local_backend_smoke.sh --clean-volumes" >&2
    summary_and_exit
fi

# --- 5. API health + metrics (bounded, values-free) ------------------------------
if wait_for "API /health" "curl -fsS ${API}/health | grep -q '\"ok\"'"; then
    pass "API /health"
else
    fail "API /health"; summary_and_exit
fi

METRICS=$(curl -fsS "${AUTH[@]}" "${API}/v1/observability/metrics" || true)
if echo "$METRICS" | grep -q '"counters"'; then
    if echo "$METRICS" | grep -qE 'payload_json|object_key|storage_uri'; then
        fail "metrics endpoint leaks forbidden fields"
    else
        pass "metrics endpoint bounded + values-free"
    fi
else
    fail "metrics endpoint did not return a bounded snapshot"
fi

# --- 6. real request through the Kafka-first chain --------------------------------
submit_request() {  # fresh report_ts each attempt: D9 writes, not noops
    local report_ts response
    report_ts="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
    response=$(curl -fsS "${AUTH[@]}" -X POST "${API}/v1/feature-requests/compute" \
        -H 'Content-Type: application/json' -d @- <<EOF
{
  "entity_type": "application",
  "entity_key": {"user_id": "${SMOKE_USER}", "application_id": "${SMOKE_APP}"},
  "view": "user_credit_risk",
  "view_version": 1,
  "deadline_ms": 60000,
  "requested_features": ["declared_income", "debt_to_income_ratio"],
  "reports": [
    {"source_name": "credit_report", "report_type": "credit_report",
     "report_ts": "${report_ts}",
     "payload": {"declared_income": 4200, "monthly_obligations": 800}},
    {"source_name": "tax_report", "report_type": "tax_report",
     "report_ts": "${report_ts}",
     "payload": {"income": 4000}}
  ]
}
EOF
    ) || return 1
    REQUEST_ID=$(json_get "d['request_id']" <<< "$response")
    [[ -n "$REQUEST_ID" ]]
}

chain_attempt() {  # submit + poll to terminal; sets ONLINE; ok iff a real online write
    submit_request || return 2
    wait_for "request completed" \
        "curl -fsS -H 'Authorization: Bearer ${FSP_CLIENT_API_KEY}' ${API}/v1/feature-requests/${REQUEST_ID} | grep -q '\"status\":\"completed\"'" \
        || return 2
    local status
    status=$(curl -fsS "${AUTH[@]}" "${API}/v1/feature-requests/${REQUEST_ID}")
    ONLINE=$(json_get "d.get('online_write_status')" <<< "$status")
    [[ "$ONLINE" == "written" || "$ONLINE" == "written_recompute" || "$ONLINE" == "noop" ]]
}

# A just-started online worker can spend the first seconds joining its Kafka consumer
# group; a request submitted in that window can legitimately end deadline_expired (the
# deadline semantics doing their job). One retry against the now-warm consumer.
if chain_attempt; then
    pass "Kafka-first chain completed (API -> Redpanda -> online-worker; online=${ONLINE})"
elif [[ "${ONLINE:-}" == "deadline_expired" ]]; then
    note "[INFO] first request expired during consumer warm-up (cold start); retrying once"
    if chain_attempt; then
        pass "Kafka-first chain completed on warm retry (online=${ONLINE})"
    else
        fail "chain failed after warm retry (online=${ONLINE:-none}; see: docker compose logs online-worker)"
        summary_and_exit
    fi
else
    fail "request did not complete (online=${ONLINE:-none}; see: docker compose logs online-worker)"
    summary_and_exit
fi

if wait_for "offline write" \
    "curl -fsS -H 'Authorization: Bearer ${FSP_CLIENT_API_KEY}' ${API}/v1/feature-requests/${REQUEST_ID} | grep -q '\"offline_write_status\":\"written\"'"; then
    pass "offline event consumed (offline-writer wrote history)"
else
    fail "offline_write_status never became written (check: docker compose logs offline-writer)"
fi

# --- 7. read the value back online -----------------------------------------------
LATEST=$(curl -fsS "${AUTH[@]}" -X POST "${API}/v1/features/latest" -H 'Content-Type: application/json' \
    -d "{\"entity\":{\"user_id\":\"${SMOKE_USER}\",\"application_id\":\"${SMOKE_APP}\"},
         \"view\":\"user_credit_risk\",\"view_version\":1,
         \"requested_features\":[\"declared_income\",\"debt_to_income_ratio\"]}" || true)
DTI=$(json_get "d['features']['debt_to_income_ratio']['value']" <<< "$LATEST")
if [[ "$DTI" == "0.2" ]]; then
    pass "online latest read from Valkey (debt_to_income_ratio=0.2, F2-derived)"
else
    fail "online latest missing/wrong (got debt_to_income_ratio=${DTI:-none})"
fi

# --- 8. offline history really is in Postgres --------------------------------------
OFFLINE_ROWS=$(psql_scalar \
    "SELECT count(*) FROM features_offline WHERE entity_key_encoded = '${ENTITY_ENCODED}';")
if [[ "${OFFLINE_ROWS:-0}" -ge 2 ]]; then
    pass "offline history rows in Postgres (${OFFLINE_ROWS} rows for smoke entity)"
else
    fail "expected >=2 offline rows in Postgres for smoke entity, got ${OFFLINE_ROWS:-0}"
fi

# --- 9. lineage (values-free) -------------------------------------------------------
LINEAGE=$(curl -fsS "${AUTH[@]}" -X POST "${API}/v1/lineage/feature-value" -H 'Content-Type: application/json' \
    -d "{\"view\":\"user_credit_risk\",\"view_version\":1,
         \"feature_name\":\"declared_income\",\"feature_version\":1,
         \"entity\":{\"user_id\":\"${SMOKE_USER}\",\"application_id\":\"${SMOKE_APP}\"}}" || true)
FOUND=$(json_get "d['found']" <<< "$LINEAGE")
if [[ "$FOUND" == "True" ]]; then
    if echo "$LINEAGE" | grep -qE '"(object_key|storage_uri|payload)'; then
        fail "lineage response leaks forbidden fields"
    elif echo "$LINEAGE" | grep -q '4200'; then
        fail "lineage response leaks a feature value"
    else
        pass "lineage answers provenance, values-free (hashes/timestamps only)"
    fi
else
    fail "lineage did not find the smoke feature value"
fi

# --- 10. workers steady (no restart loops) ------------------------------------------
RESTART_ISSUES=""
for svc in $WORKER_SERVICES; do
    cid=$(docker compose ps -q "$svc")
    [[ -n "$cid" ]] || { RESTART_ISSUES="$RESTART_ISSUES ${svc}:not-running"; continue; }
    rc=$(docker inspect --format '{{.RestartCount}}' "$cid")
    running=$(docker inspect --format '{{.State.Running}}' "$cid")
    [[ "$running" == "true" && "$rc" == "0" ]] || \
        RESTART_ISSUES="$RESTART_ISSUES ${svc}:running=${running},restarts=${rc}"
done
if [[ -z "$RESTART_ISSUES" ]]; then
    pass "all 6 workers running with restart count 0 (--forever daemon mode holds)"
else
    fail "worker instability:${RESTART_ISSUES}"
fi

summary_and_exit
