#!/usr/bin/env bash
#
# Live time-contract smoke wrapper.
#
# Starts the secure stack with the credit-demo registry (REUSING existing volumes to
# prove the additive 010 migration path), applies migrations, and runs
# scripts/run_time_contract_smoke.py. Generates an ephemeral operator key unless
# FSP_API_KEYS/FSP_CLIENT_API_KEY are supplied.
#
# Usage:
#   bash scripts/run_time_contract_smoke.sh [--no-build] [--keep-running]

set -euo pipefail

cd "$(dirname "$0")/.."

BUILD=1
KEEP_RUNNING=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-build) BUILD=0 ;;
        --keep-running) KEEP_RUNNING=1 ;;
        *) echo "unknown flag: $1"; exit 2 ;;
    esac
    shift
done

export FSP_REGISTRY_PATH="examples/credit_decision_demo/registry/credit_decision_v1.yaml"
export FSP_UDF_PROVIDER="examples.credit_decision_demo.features:build_registry_and_udfs"
if [[ -z "${FSP_API_KEYS:-}" ]]; then
    EPHEMERAL_KEY="$(openssl rand -hex 32)"
    export FSP_API_KEYS="[{\"key_id\":\"ops-tc\",\"role\":\"operator\",\"secret\":\"${EPHEMERAL_KEY}\"}]"
    export FSP_CLIENT_API_KEY="${EPHEMERAL_KEY}"
fi

UP_ARGS=(up -d --wait)
(( BUILD )) && UP_ARGS=(up -d --build --wait)
docker compose "${UP_ARGS[@]}"
bash scripts/apply_postgres_migrations.sh

RESULT=0
uv run python scripts/run_time_contract_smoke.py || RESULT=$?

if (( ! KEEP_RUNNING )); then
    docker compose --profile demo stop
fi
exit "$RESULT"
