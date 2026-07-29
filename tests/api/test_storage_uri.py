from datetime import UTC, datetime, timedelta, timezone

from fintech_feature_platform.api.storage_uri import (
    build_memory_storage_uri,
    build_s3_storage_uri,
)


def test_memory_storage_uri():
    assert build_memory_storage_uri("rep_123") == "mem://rep_123"


def test_s3_storage_uri_format():
    ts = datetime(2026, 6, 22, 10, 0, tzinfo=UTC)
    uri = build_s3_storage_uri("raw-reports", "credit_report", "rep_123", ts)
    assert uri == "s3://raw-reports/credit_report/2026/06/22/rep_123.json.gz"


def test_s3_storage_uri_normalizes_report_ts_to_utc():
    # 2026-06-22T01:00+03:00 == 2026-06-21T22:00 UTC -> partitions under 2026/06/21.
    ts = datetime(2026, 6, 22, 1, 0, tzinfo=timezone(timedelta(hours=3)))
    uri = build_s3_storage_uri("raw-reports", "credit_report", "rep_123", ts)
    assert uri == "s3://raw-reports/credit_report/2026/06/21/rep_123.json.gz"
