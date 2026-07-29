#!/usr/bin/env bash
#
# Credit-decision batch demo  — one command, real containers.
#
# Generates the synthetic portfolio, restarts the compose app services with the credit
# demo registry (FSP_REGISTRY_PATH / FSP_UDF_PROVIDER seam), then drives the full flow:
# bulk ingestion -> five Kafka-first F1 batch jobs -> F2 DAG -> F3 PD scoring -> guarded
# Mode-2 refresh -> Feature-API reads + lineage. SYNTHETIC data only.
#
# Usage:
#   bash scripts/run_credit_batch_demo.sh [--clients 30000] [--seed 42] [--skip-generate]
#
# The stack keeps running with the demo registry afterwards; restart without the env
# (`docker compose up -d`) to return to the built-in registry.

set -euo pipefail

cd "$(dirname "$0")/.."

CLIENTS=30000
SEED=42
SKIP_GENERATE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --clients) shift; CLIENTS="${1:?--clients needs a value}" ;;
        --seed) shift; SEED="${1:?--seed needs a value}" ;;
        --skip-generate) SKIP_GENERATE=1 ;;
        *) echo "unknown flag: $1"; exit 2 ;;
    esac
    shift
done

DATA_DIR=".demo-data/credit_decision"
export FSP_REGISTRY_PATH="examples/credit_decision_demo/registry/credit_decision_v1.yaml"
export FSP_UDF_PROVIDER="examples.credit_decision_demo.features:build_registry_and_udfs"

#: fail-closed stack — generate an ephemeral operator key unless supplied.
if [[ -z "${FSP_API_KEYS:-}" ]]; then
    EPHEMERAL_KEY="$(openssl rand -hex 32)"
    export FSP_API_KEYS="[{\"key_id\":\"ops-demo\",\"role\":\"operator\",\"secret\":\"${EPHEMERAL_KEY}\"}]"
    export FSP_CLIENT_API_KEY="${EPHEMERAL_KEY}"
fi

if ! docker info >/dev/null 2>&1; then
    echo "SKIP: local Docker daemon not available — the batch demo needs the compose stack."
    exit 1
fi

echo "==> syncing host extras (storage/postgres/online for the local backend steps)"
uv sync --extra dev --extra storage --extra postgres --extra online >/dev/null

if (( ! SKIP_GENERATE )); then
    echo "==> generating ${CLIENTS} synthetic clients (seed ${SEED}) into ${DATA_DIR}"
    uv run python examples/credit_decision_demo/generate_data.py \
        --clients "$CLIENTS" --seed "$SEED" --output "$DATA_DIR"
fi

echo "==> (re)building + starting the stack with the credit demo registry"
# --build is required: the fsp-app image must contain the demo package + registry seam.
docker compose up -d --build --wait

echo "==> running the batch flow"
FSP_BACKEND=local FSP_EVENTS=memory \
    uv run python examples/credit_decision_demo/run_batch.py --data-dir "$DATA_DIR"

echo
echo "Stack left running WITH the demo registry (FSP_REGISTRY_PATH exported)."
echo "Return to the built-in registry with: docker compose up -d   (in a fresh shell)"
