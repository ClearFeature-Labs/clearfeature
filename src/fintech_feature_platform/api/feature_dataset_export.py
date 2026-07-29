"""Export a PIT ``TrainingDataset`` to JSONL / CSV / (optional) Parquet.

JSONL preserves the full row shape (entity, observation_ts, context, features,
feature_metadata). CSV/Parquet are flattened, DataFrame-friendly tables. The dataset is
feature-only (no label/target column) — DS/ML join targets externally via
entity/context keys.
"""

from __future__ import annotations

import csv
import json
from typing import Any

from fintech_feature_platform.fs_core.training import TrainingDataset


def dataset_to_jsonl_rows(dataset: TrainingDataset) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in dataset.rows:
        rows.append(
            {
                "entity": dict(row.entity),
                "observation_ts": row.observation_ts.isoformat(),
                "context": row.context,
                "features": dict(row.features),
                "feature_metadata": {
                    name: {
                        "status": cell.status,
                        "feature_version": cell.feature_version,
                        "data_ts": cell.data_ts.isoformat() if cell.data_ts else None,
                    }
                    for name, cell in row.feature_metadata.items()
                },
            }
        )
    return rows


def flat_fieldnames(dataset: TrainingDataset) -> list[str]:
    if not dataset.rows:
        return []
    first = dataset.rows[0]
    entity_fields = list(first.entity.keys())
    feature_names = list(first.features.keys())
    context_keys = sorted(
        {key for row in dataset.rows if row.context for key in row.context}
    )
    columns = [f"entity.{field}" for field in entity_fields]
    columns.append("observation_ts")
    columns += [f"context.{key}" for key in context_keys]
    columns += [f"feature.{name}" for name in feature_names]
    for name in feature_names:
        columns += [
            f"meta.{name}.status",
            f"meta.{name}.feature_version",
            f"meta.{name}.data_ts",
        ]
    return columns


def dataset_to_flat_rows(dataset: TrainingDataset) -> list[dict[str, Any]]:
    if not dataset.rows:
        return []
    entity_fields = list(dataset.rows[0].entity.keys())
    feature_names = list(dataset.rows[0].features.keys())
    context_keys = sorted(
        {key for row in dataset.rows if row.context for key in row.context}
    )

    flat_rows: list[dict[str, Any]] = []
    for row in dataset.rows:
        flat: dict[str, Any] = {}
        for field_name in entity_fields:
            flat[f"entity.{field_name}"] = row.entity.get(field_name)
        flat["observation_ts"] = row.observation_ts.isoformat()
        context = row.context or {}
        for key in context_keys:
            flat[f"context.{key}"] = context.get(key)
        for name in feature_names:
            flat[f"feature.{name}"] = row.features.get(name)
            cell = row.feature_metadata.get(name)
            flat[f"meta.{name}.status"] = cell.status if cell else None
            flat[f"meta.{name}.feature_version"] = (
                cell.feature_version if cell else None
            )
            flat[f"meta.{name}.data_ts"] = (
                cell.data_ts.isoformat() if cell and cell.data_ts else None
            )
        flat_rows.append(flat)
    return flat_rows


def write_dataset(dataset: TrainingDataset, path: str, fmt: str) -> None:
    if fmt == "jsonl":
        with open(path, "w", encoding="utf-8") as handle:
            for row in dataset_to_jsonl_rows(dataset):
                handle.write(json.dumps(row) + "\n")
        return
    if fmt == "csv":
        fieldnames = flat_fieldnames(dataset)
        with open(path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(dataset_to_flat_rows(dataset))
        return
    if fmt == "parquet":
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:  # pragma: no cover - exercised only without pyarrow
            raise RuntimeError(
                "Parquet support requires optional dependency pyarrow"
            ) from exc
        table = pa.Table.from_pylist(dataset_to_flat_rows(dataset))
        pq.write_table(table, path)
        return
    raise ValueError(f"unknown format {fmt!r}")
