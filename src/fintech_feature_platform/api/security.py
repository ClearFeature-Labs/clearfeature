"""Minimum API-key security for the platform's HTTP services.

One shared module for BOTH FastAPI applications (Feature API and the demo
model-service). Design, approved in the earlier Phase A review:

- One credential convention: ``Authorization: Bearer <api-key>``.
- Two roles: ``service`` (data-plane feature/model/decision calls) and ``operator``
  (a strict superset: everything service can do, plus observability, lineage,
  ingestion, batch and training endpoints).
- Two modes: ``api_key`` (fail-closed: invalid or empty key configuration prevents
  startup) and ``disabled`` (allowed ONLY when ``FSP_ENVIRONMENT=development``;
  prints a loud startup warning).
- Each application loads its OWN accepted-key list from ``FSP_API_KEYS`` in its own
  process environment — a key accepted by the model service grants nothing on the
  Feature API.
- Keys are opaque; lookup uses ``secrets.compare_digest`` against every configured
  secret; raw secrets are never logged, never echoed in errors, and never stored on
  the request — handlers only ever see ``key_id`` + ``role``.

Semantics: missing/malformed/unknown credential -> 401 (with ``WWW-Authenticate:
Bearer``); valid credential with an insufficient role -> 403; public endpoint -> no
credential required.
"""

from __future__ import annotations

import json
import os
import secrets
from collections.abc import Mapping
from dataclasses import dataclass

from fastapi import HTTPException, Request

ROLE_SERVICE = "service"
ROLE_OPERATOR = "operator"
ROLES = (ROLE_SERVICE, ROLE_OPERATOR)

MODE_API_KEY = "api_key"
MODE_DISABLED = "disabled"
MODES = (MODE_API_KEY, MODE_DISABLED)

ENV_DEVELOPMENT = "development"

MIN_SECRET_LENGTH = 32
# Substrings that mark a secret as a placeholder, not a real credential. Generated
# keys are hex (openssl rand -hex 32), which can never contain these.
PLACEHOLDER_MARKERS = ("replace", "changeme", "change-me", "placeholder", "example",
                       "secret", "password", "your-key", "xxx")

# Endpoint groups (the whole policy vocabulary — see docs/security/minimum_security.md).
PUBLIC = "public"
SERVICE_OR_OPERATOR = "service_or_operator"
OPERATOR_ONLY = "operator"

_GROUP_ROLES = {
    SERVICE_OR_OPERATOR: (ROLE_SERVICE, ROLE_OPERATOR),
    OPERATOR_ONLY: (ROLE_OPERATOR,),
}


@dataclass(frozen=True)
class ApiKey:
    key_id: str
    role: str
    secret: str


@dataclass(frozen=True)
class AuthContext:
    """What a handler may know about the caller — never the secret."""

    key_id: str
    role: str


@dataclass(frozen=True)
class SecurityConfig:
    mode: str
    environment: str
    keys: tuple[ApiKey, ...]

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> SecurityConfig:
        env = os.environ if env is None else env
        raw_keys = env.get("FSP_API_KEYS", "")
        keys: tuple[ApiKey, ...] = ()
        if raw_keys.strip():
            try:
                parsed = json.loads(raw_keys)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    "FSP_API_KEYS is not valid JSON (expected a list of "
                    '{"key_id", "role", "secret"} objects)'
                ) from exc
            if not isinstance(parsed, list):
                raise RuntimeError("FSP_API_KEYS must be a JSON list")
            entries = []
            for index, item in enumerate(parsed):
                if not isinstance(item, dict) or set(item) != {"key_id", "role", "secret"}:
                    raise RuntimeError(
                        f"FSP_API_KEYS[{index}] must have exactly key_id/role/secret"
                    )
                entries.append(ApiKey(
                    key_id=str(item["key_id"]), role=str(item["role"]),
                    secret=str(item["secret"]),
                ))
            keys = tuple(entries)
        return cls(
            mode=env.get("FSP_SECURITY_MODE", MODE_API_KEY),
            environment=env.get("FSP_ENVIRONMENT", ENV_DEVELOPMENT),
            keys=keys,
        )

    def validate(self) -> None:
        """Fail loudly at startup on any invalid combination (never at request time)."""
        if self.mode not in MODES:
            raise RuntimeError(
                f"FSP_SECURITY_MODE must be one of {MODES}, got {self.mode!r}"
            )
        if self.mode == MODE_DISABLED:
            if self.environment != ENV_DEVELOPMENT:
                raise RuntimeError(
                    "FSP_SECURITY_MODE=disabled is allowed only with "
                    f"FSP_ENVIRONMENT={ENV_DEVELOPMENT}; this environment is "
                    f"{self.environment!r} — refusing to start without authentication"
                )
            return
        if not self.keys:
            raise RuntimeError(
                "api_key mode requires at least one key in FSP_API_KEYS "
                "(or set the explicit development bypass: FSP_SECURITY_MODE=disabled "
                "with FSP_ENVIRONMENT=development)"
            )
        key_ids = [key.key_id for key in self.keys]
        if len(set(key_ids)) != len(key_ids):
            raise RuntimeError("duplicate key_id values in FSP_API_KEYS")
        secrets_list = [key.secret for key in self.keys]
        if len(set(secrets_list)) != len(secrets_list):
            raise RuntimeError("duplicate secret values in FSP_API_KEYS")
        for key in self.keys:
            if key.role not in ROLES:
                raise RuntimeError(
                    f"key {key.key_id!r}: role must be one of {ROLES}, got {key.role!r}"
                )
            if len(key.secret) < MIN_SECRET_LENGTH:
                raise RuntimeError(
                    f"key {key.key_id!r}: secret is shorter than "
                    f"{MIN_SECRET_LENGTH} characters"
                )
            lowered = key.secret.lower()
            if any(marker in lowered for marker in PLACEHOLDER_MARKERS):
                raise RuntimeError(
                    f"key {key.key_id!r}: secret looks like a placeholder — generate a "
                    "real one (openssl rand -hex 32)"
                )

    def resolve(self, presented: str) -> ApiKey | None:
        """Constant-time scan over every configured secret; None when unknown."""
        found: ApiKey | None = None
        for key in self.keys:
            # Always compare every key (no early exit) so timing does not reveal
            # which position, if any, matched.
            if secrets.compare_digest(key.secret.encode(), presented.encode()):
                found = key
        return found


def warn_if_disabled(config: SecurityConfig, service_name: str) -> None:
    # Structured governance/security event.
    if config.mode == MODE_DISABLED:
        import logging

        from fintech_feature_platform.fs_core.observability.logs import (
            get_logger,
            log_event,
        )

        log_event(
            get_logger("security"), logging.WARNING, "security_disabled_mode",
            service=service_name,
            detail="authentication disabled (development only); never run a pilot",
        )


def require_role(config: SecurityConfig, group: str):
    """FastAPI dependency enforcing one endpoint group from the policy vocabulary."""
    allowed = _GROUP_ROLES[group]

    def dependency(request: Request) -> AuthContext:
        if config.mode == MODE_DISABLED:
            return AuthContext(key_id="dev-bypass", role=ROLE_OPERATOR)
        header = request.headers.get("Authorization", "")
        scheme, _, credential = header.partition(" ")
        if scheme.lower() != "bearer" or not credential.strip():
            raise HTTPException(
                status_code=401,
                detail={"status": "unauthorized",
                        "error": "missing or malformed Authorization header"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        key = config.resolve(credential.strip())
        if key is None:
            raise HTTPException(
                status_code=401,
                detail={"status": "unauthorized", "error": "unknown API key"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        if key.role not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"status": "forbidden", "key_id": key.key_id,
                        "role": key.role, "required": group},
            )
        return AuthContext(key_id=key.key_id, role=key.role)

    return dependency


def assert_policy_complete(app, policy: Mapping[str, str], service_name: str) -> None:
    """Startup guard: every mounted route must carry an explicit classification.

    ``policy`` maps route path -> endpoint group. FastAPI's built-in docs routes are
    exempt (they are removed entirely in api_key mode).
    """
    exempt = {"/docs", "/redoc", "/openapi.json", "/docs/oauth2-redirect"}
    mounted = {
        route.path for route in app.routes
        if getattr(route, "methods", None) and route.path not in exempt
    }
    unclassified = mounted - set(policy)
    if unclassified:
        raise RuntimeError(
            f"{service_name}: routes without a security classification: "
            f"{sorted(unclassified)} — add them to the endpoint policy"
        )
    stale = set(policy) - mounted
    if stale:
        raise RuntimeError(
            f"{service_name}: endpoint policy lists unknown routes: {sorted(stale)}"
        )
