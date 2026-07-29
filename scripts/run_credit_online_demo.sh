#!/usr/bin/env bash
#
# Credit-decision ONLINE demo  — one command, real containers.
#
# Extends the earlier batch demo with the online half: starts the stack WITH the
# demo-model-service (compose profile `demo`), runs the complete batch flow (which
# doubles as the earlier regression proof), then validates online decisions:
# seven golden segments batch-vs-online, >=50 decisions with a concurrent burst,
# latency targets, a service restart, and a "nothing entered Kafka" check.
# Evidence lands in artifacts/credit_online_demo/. SYNTHETIC data only.
#
# Usage:
#   bash scripts/run_credit_online_demo.sh [--workers 4] [--clients 2000] [--seed 42]
#                                          [--skip-batch] [--keep-running]
#
# --skip-batch reuses existing stack state (offline history must already exist).

set -euo pipefail

cd "$(dirname "$0")/.."

WORKERS=4
CLIENTS=2000
SEED=42
SKIP_BATCH=0
KEEP_RUNNING=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers) shift; WORKERS="${1:?--workers needs a value}" ;;
        --clients) shift; CLIENTS="${1:?--clients needs a value}" ;;
        --seed) shift; SEED="${1:?--seed needs a value}" ;;
        --skip-batch) SKIP_BATCH=1 ;;
        --keep-running) KEEP_RUNNING=1 ;;
        *) echo "unknown flag: $1"; exit 2 ;;
    esac
    shift
done

DATA_DIR=".demo-data/credit_decision"
export FSP_REGISTRY_PATH="examples/credit_decision_demo/registry/credit_decision_v1.yaml"
export FSP_UDF_PROVIDER="examples.credit_decision_demo.features:build_registry_and_udfs"

#: fail-closed stack. Generate EPHEMERAL keys unless supplied:
#   - Feature API registry: an operator key (demo driver) + a service key that the
#     model service presents upstream;
#   - model-service registry: its own service key for decision clients.
if [[ -z "${FSP_API_KEYS:-}" ]]; then
    OPS_KEY="$(openssl rand -hex 32)"
    MODEL_UPSTREAM_KEY="$(openssl rand -hex 32)"
    MODEL_CLIENT_KEY="$(openssl rand -hex 32)"
    export FSP_API_KEYS="[{\"key_id\":\"ops-demo\",\"role\":\"operator\",\"secret\":\"${OPS_KEY}\"},{\"key_id\":\"svc-model-upstream\",\"role\":\"service\",\"secret\":\"${MODEL_UPSTREAM_KEY}\"}]"
    export FSP_MODEL_SERVICE_API_KEYS="[{\"key_id\":\"svc-decision-client\",\"role\":\"service\",\"secret\":\"${MODEL_CLIENT_KEY}\"}]"
    export FSP_FEATURE_API_KEY="${MODEL_UPSTREAM_KEY}"
    export FSP_CLIENT_API_KEY="${OPS_KEY}"
    export FSP_MODEL_CLIENT_API_KEY="${MODEL_CLIENT_KEY}"
fi
# Demo-only: the online validation compares the synthetic input vector.
export FSP_DECISION_INCLUDE_FEATURES="${FSP_DECISION_INCLUDE_FEATURES:-1}"

if ! docker info >/dev/null 2>&1; then
    echo "SKIP: local Docker daemon not available — the online demo needs the compose stack."
    exit 1
fi

echo "==> syncing host extras (storage/postgres/online for the local backend steps)"
uv sync --extra dev --extra storage --extra postgres --extra online >/dev/null

EXPECTED_LINES=$((CLIENTS + 1))
if [[ ! -f "$DATA_DIR/labels.csv" ]] \
   || [[ "$(wc -l < "$DATA_DIR/labels.csv" | tr -d ' ')" != "$EXPECTED_LINES" ]]; then
    echo "==> generating ${CLIENTS} synthetic clients (seed ${SEED}) into ${DATA_DIR}"
    uv run python examples/credit_decision_demo/generate_data.py \
        --clients "$CLIENTS" --seed "$SEED" --output "$DATA_DIR"
fi

if (( ! SKIP_BATCH )); then
    echo "==> clean state: docker compose --profile demo down -v (fresh volumes)"
    docker compose --profile demo down -v --remove-orphans
fi

echo "==> starting the stack + demo-model-service (profile demo, batch-worker=${WORKERS})"
docker compose --profile demo up -d --build --wait --scale batch-worker="$WORKERS"

if (( ! SKIP_BATCH )); then
    echo "==> running the earlier batch flow (also the batch-demo regression proof)"
    FSP_BACKEND=local FSP_EVENTS=memory \
        uv run python examples/credit_decision_demo/run_batch.py --data-dir "$DATA_DIR"
fi

echo "==> running the online decision validation"
RESULT=0
FSP_BACKEND=local FSP_EVENTS=memory \
    uv run python examples/credit_decision_demo/run_online.py \
        --data-dir "$DATA_DIR" || RESULT=$?

if (( ! KEEP_RUNNING )); then
    docker compose --profile demo stop
else
    echo "Stack left running WITH the demo registry + demo-model-service."
fi

exit "$RESULT"
