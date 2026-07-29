"""Structured operational logging seam.

One consistent way for API/worker processes to emit machine-readable operational
events, built on stdlib ``logging`` only (no logging framework). Business/core code
never imports a logging backend — it calls ``log_event`` with a STABLE event name and
bounded fields.

Contract (one JSON object per line in service mode):

    timestamp   UTC ISO-8601, always present
    level       debug|info|warning|error, always present
    event       stable machine-readable event name (e.g. worker_item_completed)
    service     process identity set once by configure_logging (api, online-worker, ...)
    + bounded event fields (worker_role, operation, result, duration_ms,
      request_id/job_id/chunk_id where those concepts genuinely exist)

Reserved for future OpenTelemetry (do NOT invent values now): ``trace_id`` and
``span_id`` are reserved field names — when tracing lands, the active span context is
added here (one place) without changing any call site or the event schema.

Volume policy: per-feature and per-store-call detail stays in Prometheus; logs
cover lifecycle, operation completion/failure, retries, DLQ, readiness
transitions, and governance/security events. No per-feature INFO events.

Env (read directly — logging is process bootstrap, before Settings):

    FSP_LOG_FORMAT  json (default; service mode) | text (developer mode)
    FSP_LOG_LEVEL   DEBUG|INFO|WARNING|ERROR (default INFO)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime
from typing import Any

# All platform loggers live under this namespace; configure_logging attaches exactly
# one handler here (propagate=False), so uvicorn/root logging is never clobbered.
ROOT_LOGGER_NAME = "fsp"

_FIELDS_ATTR = "fsp_fields"
_SERVICE_ATTR = "fsp_service"


class JsonLogFormatter(logging.Formatter):
    """One JSON object per line; only explicit bounded fields, never record.args."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(tz=UTC).isoformat(),
            "level": record.levelname.lower(),
            "event": record.getMessage(),
            "service": self._service,
        }
        fields = getattr(record, _FIELDS_ATTR, None)
        if fields:
            payload.update(fields)
        return json.dumps(payload, sort_keys=True, default=str)


class TextLogFormatter(logging.Formatter):
    """Developer-friendly single line: LEVEL event key=value ..."""

    def format(self, record: logging.LogRecord) -> str:
        fields = getattr(record, _FIELDS_ATTR, None) or {}
        suffix = "".join(f" {key}={value}" for key, value in sorted(fields.items()))
        return f"{record.levelname} {record.getMessage()}{suffix}"


def configure_logging(service: str, *, stream: Any = None) -> logging.Logger:
    """Configure the process's ``fsp`` logger once; idempotent per process.

    ``service`` is the stable process identity (api, online-worker, ...). Reconfiguring
    (tests) replaces the handler. Returns the namespace logger.
    """
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    fmt = os.environ.get("FSP_LOG_FORMAT", "json").strip().lower()
    level_name = os.environ.get("FSP_LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(
        TextLogFormatter() if fmt == "text" else JsonLogFormatter(service)
    )
    for old in list(logger.handlers):
        logger.removeHandler(old)
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger


def get_logger(name: str) -> logging.Logger:
    """A child of the ``fsp`` namespace logger (e.g. ``get_logger("worker")``)."""
    return logging.getLogger(f"{ROOT_LOGGER_NAME}.{name}")


class RuntimeJsonFormatter(logging.Formatter):
    """Wraps third-party runtime records (uvicorn lifecycle/errors) into the same
    one-JSON-object-per-line envelope, under the stable event ``runtime_log``.

    The original human message goes into ``detail`` (it is runtime prose, not a stable
    event name); exceptions are preserved in ``error`` — errors are never hidden.
    """

    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(tz=UTC).isoformat(),
            "level": record.levelname.lower(),
            "event": "runtime_log",
            "service": self._service,
            "logger": record.name,
            "detail": record.getMessage(),
        }
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, sort_keys=True, default=str)


def adopt_uvicorn_logging(service: str, *, stream: Any = None) -> None:
    """Make the API container's runtime output machine-parseable. Idempotent;
    call after ``configure_logging`` (uvicorn installs its
    handlers in ``Config.__init__``, before the app module is imported, so this
    override — running at app construction — wins and persists).

    - ``uvicorn`` / ``uvicorn.error`` (lifecycle, errors): re-routed through
      ``RuntimeJsonFormatter`` — kept at their native levels, never silenced.
    - ``uvicorn.access``: DISABLED in json mode — it duplicates, in plaintext, what
      ``api_request_completed`` (structured) and ``fsp_api_requests_total``
      (Prometheus) already provide.

    In ``FSP_LOG_FORMAT=text`` (developer) mode uvicorn's native output is left
    untouched — dev readability wins there.
    """
    if os.environ.get("FSP_LOG_FORMAT", "json").strip().lower() == "text":
        return
    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(RuntimeJsonFormatter(service))
    for name in ("uvicorn", "uvicorn.error"):
        runtime_logger = logging.getLogger(name)
        for old in list(runtime_logger.handlers):
            runtime_logger.removeHandler(old)
        runtime_logger.addHandler(handler)
        runtime_logger.setLevel(logging.INFO)  # uvicorn's own default: lifecycle at INFO
        runtime_logger.propagate = False
    access = logging.getLogger("uvicorn.access")
    for old in list(access.handlers):
        access.removeHandler(old)
    access.propagate = False
    access.addHandler(logging.NullHandler())


def log_event(
    logger: logging.Logger, level: int, event: str, **fields: Any
) -> None:
    """Emit one structured event; ``None``-valued fields are dropped (bounded output).

    ``event`` is the machine identity — a short stable snake_case name, never a human
    sentence. Everything else goes into fields.
    """
    if not logger.isEnabledFor(level):
        return
    bounded = {key: value for key, value in fields.items() if value is not None}
    logger.log(level, event, extra={_FIELDS_ATTR: bounded})
