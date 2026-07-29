"""Training-dataset API models + mappers.

Defined separately from ``app`` to avoid a circular import. Converts the request's
observation inputs into core ``TrainingObservation`` objects and the core
``TrainingDataset`` into the response shape (no internal storage detail is exposed).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, field_validator

from fintech_feature_platform.fs_core.training import (
    TrainingDataset,
    TrainingObservation,
)


class ObservationIn(BaseModel):
    entity: dict[str, str]
    observation_ts: datetime
    context: dict[str, Any] | None = None

    @field_validator("observation_ts")
    @classmethod
    def _require_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError("must be timezone-aware")
        return value


class TrainingDatasetRequest(BaseModel):
    view: str
    view_version: int
    features: list[str]
    observations: list[ObservationIn]
    missing_policy: Literal["keep_null", "error"] = "keep_null"
    # Conservative delay before the observation point (default 0 = old behavior). A row
    # is eligible only if data_ts <= observation_ts - safety_gap and calc_ts <= obs.
    safety_gap_seconds: int = 0

    @field_validator("safety_gap_seconds")
    @classmethod
    def _non_negative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("safety_gap_seconds must be non-negative")
        return value


class FeatureMetaOut(BaseModel):
    status: str
    feature_version: int | None
    data_ts: datetime | None


class TrainingRowOut(BaseModel):
    entity: dict[str, str]
    observation_ts: datetime
    context: dict[str, Any] | None
    features: dict[str, Any]
    feature_metadata: dict[str, FeatureMetaOut]


class SummaryOut(BaseModel):
    rows: int
    features: int
    missing_values: int
    future_records_ignored: int
    safety_gap_seconds: int


class TrainingDatasetResponse(BaseModel):
    rows: list[TrainingRowOut]
    summary: SummaryOut


def to_core_observations(
    observations: list[ObservationIn],
) -> list[TrainingObservation]:
    return [
        TrainingObservation(
            entity=observation.entity,
            observation_ts=observation.observation_ts,
            context=observation.context,
        )
        for observation in observations
    ]


def dataset_to_response(dataset: TrainingDataset) -> TrainingDatasetResponse:
    rows = [
        TrainingRowOut(
            entity=row.entity,
            observation_ts=row.observation_ts,
            context=row.context,
            features=row.features,
            feature_metadata={
                name: FeatureMetaOut(
                    status=cell.status,
                    feature_version=cell.feature_version,
                    data_ts=cell.data_ts,
                )
                for name, cell in row.feature_metadata.items()
            },
        )
        for row in dataset.rows
    ]
    return TrainingDatasetResponse(rows=rows, summary=SummaryOut(**dataset.summary))
