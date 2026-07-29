"""Resolve a registry source name to its raw payload for one request.

This is the concrete ``report_ref`` resolution path:

    source name -> report_ref -> metadata (existence + report_type)
                -> storage_uri -> payload

The public ``report_ref`` only ever identifies a report; the payload is loaded by
the internal ``storage_uri`` carried on the metadata (so the physical location can
change without affecting callers). ``make_source_loader`` returns a callable
suitable for ``RequestContext(source_loader=...)``. RequestContext owns the "load
once per request" caching, so the resolver itself does not cache.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from fintech_feature_platform.fs_core.raw.meta_repository import RawReportMetaRepository
from fintech_feature_platform.fs_core.raw.payload_store import RawPayloadStore


class ReportResolver:
    def __init__(
        self,
        payload_store: RawPayloadStore,
        meta_repo: RawReportMetaRepository,
    ) -> None:
        self._payload_store = payload_store
        self._meta_repo = meta_repo

    def make_source_loader(
        self,
        source_refs: Mapping[str, str],
        expected_types: Mapping[str, str] | None = None,
    ) -> Callable[[str], Any]:
        """Return a ``load(source_name) -> payload`` callable for one request.

        ``source_refs`` binds each source name to a ``report_ref``.
        ``expected_types`` (optional) maps a source name to the report_type its
        report must have; mismatches raise ``ValueError``.
        """

        def load(source_name: str) -> Any:
            if source_name not in source_refs:
                raise ValueError(f"no report_ref bound for source {source_name!r}")
            report_ref = source_refs[source_name]

            try:
                meta = self._meta_repo.get_meta(report_ref)
            except KeyError:
                raise ValueError(f"unknown report_ref {report_ref!r}") from None

            if expected_types is not None:
                expected = expected_types.get(source_name)
                if expected is not None and meta.report_type != expected:
                    raise ValueError(
                        f"source {source_name!r} expected report_type {expected!r} "
                        f"but report_ref {report_ref!r} has {meta.report_type!r}"
                    )

            try:
                return self._payload_store.get_payload(meta.storage_uri)
            except KeyError:
                raise ValueError(
                    f"no payload stored at storage_uri {meta.storage_uri!r} "
                    f"for report_ref {report_ref!r}"
                ) from None

        return load
