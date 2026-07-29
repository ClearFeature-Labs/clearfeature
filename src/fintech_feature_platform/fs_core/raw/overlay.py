"""Request-scoped overlay adapters for the raw-report read seams.

These let the direct-payload compute path serve freshly submitted payloads/metadata
from an in-request overlay (so they are not read back from MinIO/Postgres), while
falling back to the persistent base store for anything not in the overlay.

Both are read-only: they never mutate the base store/repository. A miss in both the
overlay and the base behaves exactly like the base (its ``KeyError`` propagates).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fintech_feature_platform.fs_core.models import RawReportMeta
from fintech_feature_platform.fs_core.raw.meta_repository import RawReportMetaRepository
from fintech_feature_platform.fs_core.raw.payload_store import RawPayloadStore


class OverlayRawPayloadStore:
    def __init__(self, overlay: Mapping[str, Any], base: RawPayloadStore) -> None:
        self._overlay = overlay
        self._base = base

    def get_payload(self, storage_uri: str) -> Any:
        if storage_uri in self._overlay:
            return self._overlay[storage_uri]
        return self._base.get_payload(storage_uri)


class OverlayRawReportMetaRepository:
    def __init__(
        self, overlay: Mapping[str, RawReportMeta], base: RawReportMetaRepository
    ) -> None:
        self._overlay = overlay
        self._base = base

    def get_meta(self, report_ref: str) -> RawReportMeta:
        if report_ref in self._overlay:
            return self._overlay[report_ref]
        return self._base.get_meta(report_ref)
