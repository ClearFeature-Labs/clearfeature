"""Tiny per-process observability server.

One instance per process, serving on ONE internal port:

- ``GET /metrics``  — the process-local ``CollectorRegistry`` in Prometheus text format;
- ``GET /ready``    — bounded machine-readable readiness, when a checker is
  wired: HTTP 200 when ready, 503 when not. No checker -> 404 (metrics-only server).

Deliberately minimal: stdlib ``wsgiref`` + the ``prometheus_client`` WSGI app — no
FastAPI/uvicorn in worker processes, no credentials, no feature values, no second
application framework. Readiness and metrics are distinct CAPABILITIES served on this
ONE deliberate process port ( closure: ``FSP_OBSERVABILITY_PORT``, with
``FSP_METRICS_PORT`` retained as the compatibility fallback): with the port 0 (the
default) neither endpoint exists — documented, tested behavior. The serving thread is a daemon
so it can never prevent process shutdown; ``close()`` releases the socket
deterministically (tests).
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from wsgiref.simple_server import WSGIRequestHandler, make_server

from prometheus_client import CollectorRegistry, make_wsgi_app


class _SilentHandler(WSGIRequestHandler):
    """No per-scrape access-log lines (scrapes every few seconds would flood logs)."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return None


class MetricsServer:
    """Serves ``GET /metrics`` (+ optional ``GET /ready``) for one process. ``port=0``
    binds ephemeral (tests); production passes the configured observability port.

    ``ready_check`` is any callable returning an object with ``ready: bool`` and
    ``to_dict()`` (a ``ReadinessReport``); it must be bounded and non-raising — the
    checker guarantees both. The response body is the bounded report JSON only.
    """

    def __init__(
        self,
        registry: CollectorRegistry,
        port: int,
        *,
        ready_check: Callable[[], object] | None = None,
    ) -> None:
        metrics_app = make_wsgi_app(registry)

        def _app(environ, start_response):
            if environ.get("PATH_INFO") == "/ready":
                if ready_check is None:
                    start_response("404 Not Found", [("Content-Type", "text/plain")])
                    return [b"no readiness checker configured"]
                report = ready_check()
                body = json.dumps(report.to_dict(), sort_keys=True).encode("utf-8")
                status = "200 OK" if report.ready else "503 Service Unavailable"
                start_response(status, [("Content-Type", "application/json")])
                return [body]
            return metrics_app(environ, start_response)

        self._app = _app
        self._httpd = make_server("", port, self._app, handler_class=_SilentHandler)
        self.port = self._httpd.server_port
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, name="fsp-metrics", daemon=True
        )

    def start(self) -> MetricsServer:
        self._thread.start()
        return self

    def close(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
