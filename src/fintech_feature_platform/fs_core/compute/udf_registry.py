"""Explicit in-memory mapping from registry UDF ids to Python callables.

No dynamic import: a UDF id (e.g. ``"udf.credit.declared_income"``) is looked up
in a plain dict. Each UDF has the signature

    (sources: Mapping[str, Any], deps: Mapping[str, Any]) -> Any

where ``sources`` is keyed by source name and ``deps`` by dependency feature
name.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

Udf = Callable[[Mapping[str, Any], Mapping[str, Any]], Any]


class UdfRegistry:
    def __init__(self, udfs: Mapping[str, Udf]) -> None:
        self._udfs = dict(udfs)

    def get(self, udf_id: str) -> Udf:
        try:
            return self._udfs[udf_id]
        except KeyError:
            raise ValueError(f"unknown udf id {udf_id!r}") from None
