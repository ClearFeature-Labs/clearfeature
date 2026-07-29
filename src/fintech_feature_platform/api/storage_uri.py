"""Builders for the internal raw-report ``storage_uri``.

``storage_uri`` is the physical payload pointer (internal; never exposed to clients).
Memory mode uses ``mem://<report_ref>``; local (MinIO/S3) mode uses an
``s3://bucket/...`` key partitioned by the report date.
"""

from __future__ import annotations

from datetime import UTC, datetime


def build_memory_storage_uri(report_ref: str) -> str:
    return f"mem://{report_ref}"


def build_s3_storage_uri(
    bucket: str, report_type: str, report_ref: str, report_ts: datetime
) -> str:
    # Partition by the report date (report_ts), normalized to UTC.
    ts = report_ts.astimezone(UTC)
    return f"s3://{bucket}/{report_type}/{ts:%Y/%m/%d}/{report_ref}.json.gz"
