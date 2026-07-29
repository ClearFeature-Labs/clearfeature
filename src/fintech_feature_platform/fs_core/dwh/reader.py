"""Data-warehouse reader seam.

A DWH is an **ingestion boundary, not a compute runtime** (invariant I5): the platform
reads DWH rows only to materialize them into landing form (a) or (b). The reader yields
already-extracted rows as plain dicts keyed by a named query/source — it does not
execute arbitrary SQL from the online or batch compute paths.

Only ``InMemoryDwhReader`` (test/adapter) is provided here. A real Postgres/Greenplum/
ClickHouse adapter satisfies the same ``read_rows`` contract without changing ingestion
logic; heavy DB drivers are deliberately not a dependency of this module.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Protocol


class DwhReader(Protocol):
    def read_rows(self, query_name: str) -> Iterable[Mapping[str, Any]]: ...


class InMemoryDwhReader:
    """Reader backed by preloaded rows per named query (tests / a fake DWH)."""

    def __init__(self, rows_by_query: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
        self._rows = {name: list(rows) for name, rows in rows_by_query.items()}

    def read_rows(self, query_name: str) -> Iterable[Mapping[str, Any]]:
        if query_name not in self._rows:
            raise KeyError(f"unknown DWH query {query_name!r}")
        return list(self._rows[query_name])
