"""Consistency-check API models + report mapper.

Defined separately from ``app`` to avoid a circular import. The mapper converts the
core ``FeatureConsistencyReport`` into the response shape (no internal storage detail
is exposed).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from fintech_feature_platform.fs_core.consistency import FeatureConsistencyReport


class ConsistencyCheckRequest(BaseModel):
    view: str
    view_version: int
    entity: dict[str, str]
    features: list[str]


class ConsistencyItemOut(BaseModel):
    status: str
    offline_value: Any
    online_value: Any
    offline_data_ts: datetime | None
    online_data_ts: datetime | None
    offline_feature_version: int | None
    online_feature_version: int | None


class ConsistencyCheckResponse(BaseModel):
    status: str
    checks: dict[str, ConsistencyItemOut]


def to_response(report: FeatureConsistencyReport) -> ConsistencyCheckResponse:
    checks = {
        name: ConsistencyItemOut(
            status=item.status,
            offline_value=item.offline_value,
            online_value=item.online_value,
            offline_data_ts=item.offline_data_ts,
            online_data_ts=item.online_data_ts,
            offline_feature_version=item.offline_feature_version,
            online_feature_version=item.online_feature_version,
        )
        for name, item in report.items.items()
    }
    return ConsistencyCheckResponse(status=report.status, checks=checks)
