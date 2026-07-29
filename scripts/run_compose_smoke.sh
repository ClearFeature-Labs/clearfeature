#!/usr/bin/env bash
#
# Optional Compose smoke. NOT part of `make verify` (which stays Docker-free).
#
# Builds + starts the full stack (infra + api + workers), waits for healthchecks, probes
# the API health/metrics endpoints from the host, and prints service status. It does NOT
# stop services or touch volumes on exit — `docker compose stop` / `down` is your call
# (`down -v` destroys data volumes; never run it implicitly).
#
# Usage:
#   bash scripts/run_compose_smoke.sh

set -euo pipefail

API_PORT="${API_PORT:-8000}"

#: fail-closed stack — generate an ephemeral operator key unless supplied.
if [[ -z "${FSP_API_KEYS:-}" ]]; then
    EPHEMERAL_KEY="$(openssl rand -hex 32)"
    export FSP_API_KEYS="[{\"key_id\":\"ops-smoke\",\"role\":\"operator\",\"secret\":\"${EPHEMERAL_KEY}\"}]"
    export FSP_CLIENT_API_KEY="${EPHEMERAL_KEY}"
fi

echo "==> docker compose up -d --build --wait (healthchecks gate readiness)"
docker compose up -d --build --wait

echo "==> API /health"
curl -fsS "http://localhost:${API_PORT}/health"
echo

echo "==> API /v1/observability/metrics (bounded JSON snapshot, operator key)"
curl -fsS -H "Authorization: Bearer ${FSP_CLIENT_API_KEY}" \
    "http://localhost:${API_PORT}/v1/observability/metrics" | head -c 400
echo

echo "==> service status"
docker compose ps

echo "Compose smoke passed. Stack left running (use 'docker compose stop' to stop)."
