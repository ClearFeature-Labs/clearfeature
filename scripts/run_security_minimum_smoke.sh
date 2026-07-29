#!/usr/bin/env bash
#
# Security-minimum live smoke  — the real Compose stack in api_key mode.
#
# Generates EPHEMERAL keys (never stored in committed files), starts the stack WITH the
# demo-model-service (profile demo), and verifies the whole access matrix live:
#
#   public health | 401 on missing/invalid keys | 403 on wrong role | operator superset
#   docs disabled | separate key registries | model-service -> Feature API s2s auth
#   no key material in service logs | all containers healthy
#
# Evidence (safe, regenerable, no secrets): artifacts/security_minimum/
#
# Usage:
#   bash scripts/run_security_minimum_smoke.sh [--no-build] [--keep-running]

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

API_PORT="${API_PORT:-8000}"
MODEL_PORT="${MODEL_SERVICE_PORT:-8090}"
API="http://127.0.0.1:${API_PORT}"
MODEL="http://127.0.0.1:${MODEL_PORT}"
OUT_DIR="artifacts/security_minimum"
mkdir -p "$OUT_DIR"

PASS_COUNT=0
FAIL_COUNT=0
RESULTS_FILE="$(mktemp)"
trap 'rm -f "$RESULTS_FILE"' EXIT

check() {  # check <name> <expected_http_code> <actual_http_code>
    local name="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        echo "==> PASS: ${name} (${actual})"; PASS_COUNT=$((PASS_COUNT + 1))
        echo "{\"name\": \"${name}\", \"expected\": \"${expected}\", \"actual\": \"${actual}\", \"ok\": true}" >> "$RESULTS_FILE"
    else
        echo "==> FAIL: ${name} (expected ${expected}, got ${actual})"; FAIL_COUNT=$((FAIL_COUNT + 1))
        echo "{\"name\": \"${name}\", \"expected\": \"${expected}\", \"actual\": \"${actual}\", \"ok\": false}" >> "$RESULTS_FILE"
    fi
}

code() {  # code <url> [curl args...] -> HTTP status
    curl -s -o /dev/null -w "%{http_code}" "$@" || echo "000"
}

# --- ephemeral keys (this run only; never written to any artifact) -----------------
OPS_KEY="$(openssl rand -hex 32)"
SVC_KEY="$(openssl rand -hex 32)"
MODEL_UPSTREAM_KEY="$(openssl rand -hex 32)"
MODEL_CLIENT_KEY="$(openssl rand -hex 32)"
BAD_KEY="$(openssl rand -hex 32)"

export FSP_SECURITY_MODE=api_key
export FSP_API_KEYS="[{\"key_id\":\"ops-sec-smoke\",\"role\":\"operator\",\"secret\":\"${OPS_KEY}\"},{\"key_id\":\"svc-sec-smoke\",\"role\":\"service\",\"secret\":\"${SVC_KEY}\"},{\"key_id\":\"svc-model-upstream\",\"role\":\"service\",\"secret\":\"${MODEL_UPSTREAM_KEY}\"}]"
export FSP_MODEL_SERVICE_API_KEYS="[{\"key_id\":\"svc-decision-client\",\"role\":\"service\",\"secret\":\"${MODEL_CLIENT_KEY}\"}]"
export FSP_FEATURE_API_KEY="${MODEL_UPSTREAM_KEY}"
# The model service pins the demo registry; the API must serve the same contract so
# the s2s call reaches a real view.
export FSP_REGISTRY_PATH="examples/credit_decision_demo/registry/credit_decision_v1.yaml"
export FSP_UDF_PROVIDER="examples.credit_decision_demo.features:build_registry_and_udfs"

echo "==> starting the stack in api_key mode (profile demo)"
UP_ARGS=(--profile demo up -d --wait)
(( BUILD )) && UP_ARGS=(--profile demo up -d --build --wait)
docker compose "${UP_ARGS[@]}"

# --- access matrix -----------------------------------------------------------------
check "feature-api health is public"          200 "$(code "$API/health")"
check "model-service health is public"        200 "$(code "$MODEL/health")"
check "metrics without key -> 401"            401 "$(code "$API/v1/observability/metrics")"
check "metrics with invalid key -> 401"       401 "$(code "$API/v1/observability/metrics" -H "Authorization: Bearer $BAD_KEY")"
check "metrics with malformed header -> 401"  401 "$(code "$API/v1/observability/metrics" -H "Authorization: $OPS_KEY")"
check "metrics with service key -> 403"       403 "$(code "$API/v1/observability/metrics" -H "Authorization: Bearer $SVC_KEY")"
check "metrics with operator key -> 200"      200 "$(code "$API/v1/observability/metrics" -H "Authorization: Bearer $OPS_KEY")"

LATEST_BODY='{"view":"credit_decision","view_version":1,"entity":{"user_id":"sec_u","application_id":"sec_a"},"requested_features":["bureau_score"]}'
check "latest without key -> 401"             401 "$(code "$API/v1/features/latest" -X POST -H 'Content-Type: application/json' -d "$LATEST_BODY")"
check "latest with service key -> 200"        200 "$(code "$API/v1/features/latest" -X POST -H 'Content-Type: application/json' -H "Authorization: Bearer $SVC_KEY" -d "$LATEST_BODY")"
check "latest with operator key -> 200 (superset)" 200 "$(code "$API/v1/features/latest" -X POST -H 'Content-Type: application/json' -H "Authorization: Bearer $OPS_KEY" -d "$LATEST_BODY")"
check "batch status with service key -> 403"  403 "$(code "$API/v1/batch/jobs/none" -H "Authorization: Bearer $SVC_KEY")"
check "batch status with operator key -> 404 (auth passed)" 404 "$(code "$API/v1/batch/jobs/none" -H "Authorization: Bearer $OPS_KEY")"

check "feature-api /docs disabled"            404 "$(code "$API/docs")"
check "feature-api /openapi.json disabled"    404 "$(code "$API/openapi.json")"
check "model-service /docs disabled"          404 "$(code "$MODEL/docs")"

DECISION_BODY='{"user_id":"sec_u","application_id":"sec_a"}'
check "decision without key -> 401"           401 "$(code "$MODEL/v1/credit/decision" -X POST -H 'Content-Type: application/json' -d "$DECISION_BODY")"
check "decision with FEATURE-API key -> 401 (separate registries)" 401 "$(code "$MODEL/v1/credit/decision" -X POST -H 'Content-Type: application/json' -H "Authorization: Bearer $SVC_KEY" -d "$DECISION_BODY")"
# 409 missing_features proves the FULL chain: model-service auth passed AND its own
# upstream service key was accepted by the Feature API (a rejected upstream key would
# surface as 502 feature_api_error).
check "decision with model-client key -> 409 (s2s auth to Feature API worked)" 409 "$(code "$MODEL/v1/credit/decision" -X POST -H 'Content-Type: application/json' -H "Authorization: Bearer $MODEL_CLIENT_KEY" -d "$DECISION_BODY")"

# --- log safety --------------------------------------------------------------------
LOGS="$(docker compose --profile demo logs api demo-model-service 2>/dev/null || true)"
LEAKED=0
for secret in "$OPS_KEY" "$SVC_KEY" "$MODEL_UPSTREAM_KEY" "$MODEL_CLIENT_KEY"; do
    if grep -qF "$secret" <<< "$LOGS"; then LEAKED=1; fi
done
check "no key material in api/model-service logs" 0 "$LEAKED"
if grep -q "authentication is DISABLED" <<< "$LOGS"; then
    check "no disabled-mode warning in api_key mode" absent present
else
    check "no disabled-mode warning in api_key mode" absent absent
fi

# --- containers healthy ------------------------------------------------------------
UNHEALTHY="$(docker compose --profile demo ps --format '{{.Names}} {{.Status}}' | grep -civ 'Up' || true)"
check "all containers up" 0 "$UNHEALTHY"

# --- evidence (safe: no secrets; key material was generated above and dies with the shell)
# This step only reads ENDPOINT_POLICY, a plain module-level dict with no security
# dependency — AUTOSTART=0 stops each module from building a live app on import, so
# this host-side helper needs NO security env at all and cannot print the dev-bypass
# warning (: that warning used to appear here, confusingly, right after the
# "no disabled-mode warning" check above — which correctly inspects the SECURE
# containers' own logs and is unrelated to this one-shot policy-dict reader).
GIT_COMMIT="$(git rev-parse HEAD)"
FSP_API_APP_AUTOSTART=0 FSP_MODEL_SERVICE_AUTOSTART=0 \
    uv run python - "$OUT_DIR" "$GIT_COMMIT" "$PASS_COUNT" "$FAIL_COUNT" "$RESULTS_FILE" <<'EOF'
import json
import sys
from datetime import UTC, datetime

out_dir, commit, passed, failed, results_file = sys.argv[1:6]
from examples.credit_decision_demo import model_service  # noqa: E402
from fintech_feature_platform.api import app as api_app  # noqa: E402

results = [json.loads(line) for line in open(results_file)]
policy = {
    "feature-api": api_app.ENDPOINT_POLICY,
    "demo-model-service": model_service.ENDPOINT_POLICY,
}
with open(f"{out_dir}/endpoint_policy.json", "w") as fh:
    json.dump(policy, fh, indent=2, sort_keys=True)
with open(f"{out_dir}/test_matrix.json", "w") as fh:
    json.dump(results, fh, indent=2)
report = {
    "suite": "security_minimum",
    "timestamp": datetime.now(tz=UTC).isoformat(),
    "git_commit": commit,
    "security_mode": "api_key",
    "endpoint_policy_summary": {
        service: {group: sum(1 for g in mapping.values() if g == group)
                  for group in sorted(set(mapping.values()))}
        for service, mapping in policy.items()
    },
    "role_test_results": {"passed": int(passed), "failed": int(failed)},
    "secret_leak_scan": "no generated key material found in api/model-service logs",
    "known_limitations": [
        "TLS termination and rate limiting are reverse-proxy responsibilities",
        "single-node compose; keys are env-delivered; no secrets manager",
    ],
    "verdict": "PASS" if int(failed) == 0 else "FAIL",
}
with open(f"{out_dir}/report.json", "w") as fh:
    json.dump(report, fh, indent=2)
with open(f"{out_dir}/README.md", "w") as fh:
    fh.write(
        "# Security-minimum evidence\n\n"
        "Regenerable, secret-free evidence from `scripts/run_security_minimum_smoke.sh`\n"
        "(ephemeral keys are generated per run and never written anywhere).\n"
        "`report.json` (run summary), `endpoint_policy.json` (both services' full\n"
        "route classification), `test_matrix.json` (live access-matrix results).\n"
    )
print(f"evidence written to {out_dir}")
EOF

# Belt-and-braces: no generated key may appear in the evidence.
for secret in "$OPS_KEY" "$SVC_KEY" "$MODEL_UPSTREAM_KEY" "$MODEL_CLIENT_KEY"; do
    if grep -rqF "$secret" "$OUT_DIR"; then
        echo "==> FAIL: key material leaked into $OUT_DIR"; FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
done

if (( ! KEEP_RUNNING )); then
    docker compose --profile demo stop
fi

echo
if (( FAIL_COUNT == 0 )); then
    echo "Security minimum smoke: PASS (${PASS_COUNT} checks)"
    exit 0
fi
echo "Security minimum smoke: FAIL (${FAIL_COUNT} of $((PASS_COUNT + FAIL_COUNT)) checks failed)"
exit 1
