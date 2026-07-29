"""Online demo-model-service for the credit-decision demo.

One request path:

    POST /v1/credit/decision
      -> Feature API /v1/features/latest  (the platform's Kafka-first-computed,
         Mode-2-projected Valkey latest values — never a direct store read)
      -> the SAME committed model artifact used by batch F3
         (DemoPdModelRunner: shared loader + shared predict_proba, registry digest pin
         enforced at startup and on every predict)
      -> pd_score + deterministic decision.

Boundaries (the demo's whole point):

- The service depends ONLY on the Feature API HTTP contract and the committed model
  artifact. It never imports or connects to Postgres, Valkey, MinIO, Kafka, or raw
  reports (pinned by a test).
- No second model implementation: scoring goes through the exact batch F3 code path
  (`DemoPdModelRunner.predict` -> `model_lib.predict_proba`).
- Missing or stale required features produce explicit controlled responses
  (409 with a machine-readable status), never silent defaults.

Decision policy (demo contract, deterministic):

    pd_score <  0.10  -> "approve"
    pd_score <  0.30  -> "review"
    pd_score >= 0.30  -> "decline"

Configuration (env):

    FSP_FEATURE_API_URL   Feature API base URL (compose: http://api:8000)
    FSP_REGISTRY_PATH     registry YAML holding the pd_score digest pin
                          (defaults to the demo registry)
    FSP_SECURITY_MODE / FSP_ENVIRONMENT / FSP_API_KEYS
                          this service's OWN accepted-key registry (;
                          shared security module, `Authorization: Bearer`)
    FSP_FEATURE_API_KEY   the service-role key it presents to the Feature API
    FSP_DECISION_INCLUDE_FEATURES
                          "1" -> include the synthetic input vector in responses
                          (demo/development only; hidden by default)
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, field_validator

from examples.credit_decision_demo.features import REGISTRY_PATH
from examples.credit_decision_demo.model_lib import ARTIFACT_PATH
from examples.credit_decision_demo.model_runner import DemoPdModelRunner
from fintech_feature_platform.api.security import (
    MODE_API_KEY,
    PUBLIC,
    SERVICE_OR_OPERATOR,
    SecurityConfig,
    assert_policy_complete,
    require_role,
    warn_if_disabled,
)
from fintech_feature_platform.fs_core.model_runner import ModelRef
from fintech_feature_platform.fs_core.registry.loader import load_registry_file

VIEW = "credit_decision"
VIEW_VERSION = 1
MODEL_FEATURE_NAME = "pd_score"

# Deterministic demo decision policy; boundaries are inclusive on the lower edge.
APPROVE_BELOW = 0.10
REVIEW_BELOW = 0.30

SERVICE_NAME = "demo-model-service"


def decide(pd_score: float) -> str:
    if pd_score < APPROVE_BELOW:
        return "approve"
    if pd_score < REVIEW_BELOW:
        return "review"
    return "decline"


class DecisionRequest(BaseModel):
    user_id: str
    application_id: str
    # Defaults to "now"; used for the response echo and the staleness check.
    observation_ts: datetime | None = None
    # When set, any required feature whose data_ts is older than
    # observation_ts - max_feature_age_seconds makes the request fail explicitly.
    max_feature_age_seconds: float | None = None

    @field_validator("observation_ts")
    @classmethod
    def _require_timezone_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("observation_ts must be timezone-aware")
        return value


ENDPOINT_POLICY: dict[str, str] = {
    "/health": PUBLIC,
    "/v1/credit/decision": SERVICE_OR_OPERATOR,
}


def _http_fetch_latest(api_url: str, api_key: str | None) -> Callable[[dict], dict]:
    def fetch(payload: dict) -> dict:
        headers = {"Content-Type": "application/json"}
        if api_key:
            # The model service authenticates to the Feature API with its OWN
            # service-role key  — the Docker network is not a credential.
            headers["Authorization"] = f"Bearer {api_key}"
        request = urllib.request.Request(
            api_url + "/v1/features/latest",
            data=json.dumps(payload).encode(),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read())

    return fetch


def create_app(
    *,
    artifact_path: Path = ARTIFACT_PATH,
    registry_path: Path | None = None,
    feature_api_url: str | None = None,
    fetch_latest: Callable[[dict], dict] | None = None,
    security: SecurityConfig | None = None,
    feature_api_key: str | None = None,
    include_features: bool | None = None,
) -> FastAPI:
    """Build the service; fails LOUDLY at startup on a registry/artifact digest mismatch."""
    registry_file = Path(
        registry_path
        or os.environ.get("FSP_REGISTRY_PATH")
        or REGISTRY_PATH
    )
    api_url = (
        feature_api_url
        or os.environ.get("FSP_FEATURE_API_URL")
        or "http://127.0.0.1:8000"
    ).rstrip("/")

    security = security if security is not None else SecurityConfig.from_env()
    security.validate()
    warn_if_disabled(security, SERVICE_NAME)
    secure = security.mode == MODE_API_KEY
    outbound_key = feature_api_key or os.environ.get("FSP_FEATURE_API_KEY") or None
    if secure and fetch_latest is None and not outbound_key:
        raise RuntimeError(
            "api_key mode requires FSP_FEATURE_API_KEY (the model service's own "
            "service-role key for the Feature API)"
        )
    if include_features is None:
        # Demo/development-only: expose the synthetic input vector in responses.
        include_features = os.environ.get("FSP_DECISION_INCLUDE_FEATURES") == "1"

    registry = load_registry_file(registry_file)
    view = next(v for v in registry.feature_views if v.name == VIEW)
    feature = next(f for f in view.features if f.name == MODEL_FEATURE_NAME)
    if feature.model is None:
        raise RuntimeError(f"{MODEL_FEATURE_NAME} is not a model feature in {registry_file}")
    model_spec = feature.model

    runner = DemoPdModelRunner(artifact_path)
    if runner.digest != model_spec.digest:
        raise RuntimeError(
            f"model digest mismatch at startup: registry pins {model_spec.digest!r}, "
            f"loaded artifact is {runner.digest!r} — refusing to serve"
        )
    model_ref = ModelRef(
        uri=model_spec.uri, digest=model_spec.digest, output_name=model_spec.output_name
    )
    # The artifact owns the input order; the registry pins the dependency SET.
    feature_order: list[str] = runner.feature_order
    dep_names = {dep.feature for dep in feature.deps}
    if set(feature_order) != dep_names:
        raise RuntimeError(
            f"artifact feature_order {sorted(feature_order)} != registry deps "
            f"{sorted(dep_names)} — artifact and contract disagree"
        )
    # "mlflow://credit_pd_demo/1" -> name=credit_pd_demo, version=1 (demo convention).
    uri_tail = model_spec.uri.split("://", 1)[-1]
    model_name, _, model_version = uri_tail.rpartition("/")

    fetch = fetch_latest or _http_fetch_latest(api_url, outbound_key)
    app = FastAPI(
        title=SERVICE_NAME,
        # OpenAPI/Swagger/ReDoc are disabled in api_key mode.
        docs_url=None if secure else "/docs",
        redoc_url=None if secure else "/redoc",
        openapi_url=None if secure else "/openapi.json",
    )
    decision_auth = Depends(require_role(security, SERVICE_OR_OPERATOR))

    @app.get("/health")
    def health() -> dict:
        # Minimal by policy : no digest, no versions, no dependencies.
        return {"status": "ok"}

    @app.post("/v1/credit/decision", dependencies=[decision_auth])
    def credit_decision(request: DecisionRequest) -> dict:
        request_id = f"dec_{uuid.uuid4().hex}"
        observation_ts = request.observation_ts or datetime.now(tz=UTC)

        try:
            latest = fetch({
                "view": VIEW, "view_version": VIEW_VERSION,
                "entity": {"user_id": request.user_id,
                           "application_id": request.application_id},
                "requested_features": feature_order,
            })
        except urllib.error.HTTPError as exc:
            # Bounded: the upstream status code only — never headers or bodies.
            raise HTTPException(
                status_code=502,
                detail={"status": "feature_api_error", "upstream_status": exc.code},
            ) from exc
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise HTTPException(
                status_code=502,
                detail={"status": "feature_api_unavailable"},
            ) from exc

        missing = sorted(set(latest.get("missing") or [])
                         | (set(feature_order) - set(latest.get("features") or {})))
        if missing:
            raise HTTPException(
                status_code=409,
                detail={"status": "missing_features", "missing": missing,
                        "request_id": request_id},
            )

        features = latest["features"]
        if request.max_feature_age_seconds is not None:
            stale = sorted(
                name for name, item in features.items()
                if (observation_ts - datetime.fromisoformat(item["data_ts"])
                    ).total_seconds() > request.max_feature_age_seconds
            )
            if stale:
                raise HTTPException(
                    status_code=409,
                    detail={"status": "stale_features", "stale": stale,
                            "max_feature_age_seconds": request.max_feature_age_seconds,
                            "request_id": request_id},
                )

        row: dict[str, Any] = {name: features[name]["value"] for name in feature_order}
        pd_score = runner.predict(model_ref, [row])[0]

        response = {
            "request_id": request_id,
            "status": "completed",
            "user_id": request.user_id,
            "application_id": request.application_id,
            "pd_score": pd_score,
            "decision": decide(pd_score),
            "decision_policy": {"approve_below": APPROVE_BELOW,
                                "review_below": REVIEW_BELOW},
            "model_name": model_name,
            "model_version": model_version,
            "model_uri": model_spec.uri,
            "model_digest": runner.digest,
            "feature_view": VIEW,
            "feature_view_version": VIEW_VERSION,
            "observation_ts": observation_ts.isoformat(),
        }
        if include_features:
            # Demo/development-only (FSP_DECISION_INCLUDE_FEATURES=1): synthetic
            # input vector for validation. NOT a production default.
            response["features"] = {
                name: {"value": features[name]["value"],
                       "feature_version": features[name]["feature_version"],
                       "data_ts": features[name]["data_ts"]}
                for name in feature_order
            }
        return response

    assert_policy_complete(app, ENDPOINT_POLICY, SERVICE_NAME)
    return app


# uvicorn entrypoint: `uvicorn examples.credit_decision_demo.model_service:app`
app = create_app() if os.environ.get("FSP_MODEL_SERVICE_AUTOSTART", "1") == "1" else None
