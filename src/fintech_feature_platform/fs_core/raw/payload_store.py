"""Raw payload storage seam.

A narrow read contract for fetching a raw payload by its ``storage_uri`` (the
internal physical pointer to the payload object), plus a tiny in-memory
implementation for tests. The real implementation (MinIO) will satisfy the same
``get_payload`` contract, keyed by the object's storage URI.

``storage_uri`` is resolved from a report's metadata (see ``ReportResolver``); it
is distinct from the public ``report_ref`` and is never exposed to clients.
"""

from __future__ import annotations

from typing import Any, Protocol


class RawPayloadStore(Protocol):
    def get_payload(self, storage_uri: str) -> Any: ...


class InMemoryPayloadStore:
    def __init__(self) -> None:
        self._payloads: dict[str, Any] = {}

    def put(self, storage_uri: str, payload: Any) -> None:
        self._payloads[storage_uri] = payload

    def get_payload(self, storage_uri: str) -> Any:
        return self._payloads[storage_uri]
