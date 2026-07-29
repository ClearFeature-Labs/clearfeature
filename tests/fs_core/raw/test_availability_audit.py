"""Availability-correction audit trail.

Every accepted trusted correction leaves exactly one append-only record with old/new
values and origin; duplicates and replays never do; the Postgres path commits the
metadata update and the audit row in ONE transaction.
"""

import dataclasses
import json
from datetime import UTC, datetime, timedelta

import pytest

from fintech_feature_platform.fs_core.models import EntityKey, RawReportMeta
from fintech_feature_platform.fs_core.raw.meta_repository import (
    CORRECTION_ORIGIN_JSONL,
    CorrectionContext,
    InMemoryMetaRepository,
)
from fintech_feature_platform.fs_core.raw.postgres_meta_repository import (
    _AUDIT_INSERT_SQL,
    PostgresRawReportMetaRepository,
)

_TS = datetime(2024, 1, 1, tzinfo=UTC)
_AVAILABLE_1 = datetime(2024, 2, 1, tzinfo=UTC)
_AVAILABLE_2 = datetime(2024, 3, 1, tzinfo=UTC)
_CONTEXT = CorrectionContext(
    change_origin=CORRECTION_ORIGIN_JSONL, manifest_id="sdm_test_1"
)


def _meta(available_at=None, availability_source=None) -> RawReportMeta:
    return RawReportMeta(
        report_ref="rep_audit_1",
        report_type="credit_bureau_report",
        entity_type="application",
        entity_key=EntityKey.from_mapping(
            {"user_id": "1", "application_id": "A1"},
            key_order=["user_id", "application_id"],
        ),
        report_ts=_TS,
        payload_size_bytes=10,
        content_hash="sha256:abc",
        storage_uri="s3://raw-reports/x",
        created_at=_TS + timedelta(days=900),
        available_at=available_at,
        availability_source=availability_source,
    )


def test_initial_trusted_claim_creates_no_correction_event():
    repo = InMemoryMetaRepository()
    assert repo.upsert(_meta(_AVAILABLE_1, "source_provided"),
                       correction_context=_CONTEXT) is True
    assert repo.availability_changes == []


def test_exact_duplicate_creates_no_event():
    repo = InMemoryMetaRepository()
    repo.upsert(_meta(_AVAILABLE_1, "source_provided"), correction_context=_CONTEXT)
    assert repo.upsert(_meta(_AVAILABLE_1, "source_provided"),
                       correction_context=_CONTEXT) is False
    assert repo.availability_changes == []


def test_trusted_correction_updates_meta_and_writes_exactly_one_event():
    repo = InMemoryMetaRepository()
    repo.upsert(_meta(_AVAILABLE_1, "source_provided"), correction_context=_CONTEXT)
    assert repo.upsert(_meta(_AVAILABLE_2, "source_provided"),
                       correction_context=_CONTEXT) is True
    assert repo.get_meta("rep_audit_1").available_at == _AVAILABLE_2
    assert len(repo.availability_changes) == 1
    change = repo.availability_changes[0]
    assert change.old_available_at == _AVAILABLE_1
    assert change.new_available_at == _AVAILABLE_2
    assert change.old_availability_source == "source_provided"
    assert change.new_availability_source == "source_provided"
    assert change.change_origin == CORRECTION_ORIGIN_JSONL
    assert change.manifest_id == "sdm_test_1"
    assert change.report_ref == "rep_audit_1"


def test_second_correction_appends_and_preserves_the_first():
    repo = InMemoryMetaRepository()
    repo.upsert(_meta(_AVAILABLE_1, "source_provided"), correction_context=_CONTEXT)
    repo.upsert(_meta(_AVAILABLE_2, "source_provided"), correction_context=_CONTEXT)
    third = _AVAILABLE_2 + timedelta(days=7)
    repo.upsert(_meta(third, "source_provided"), correction_context=_CONTEXT)
    assert [c.new_available_at for c in repo.availability_changes] == [
        _AVAILABLE_2, third,
    ]
    assert repo.availability_changes[0].old_available_at == _AVAILABLE_1  # preserved


def test_replay_of_the_same_correction_is_not_duplicated():
    repo = InMemoryMetaRepository()
    repo.upsert(_meta(_AVAILABLE_1, "source_provided"), correction_context=_CONTEXT)
    repo.upsert(_meta(_AVAILABLE_2, "source_provided"), correction_context=_CONTEXT)
    assert repo.upsert(_meta(_AVAILABLE_2, "source_provided"),
                       correction_context=_CONTEXT) is False  # identical claim now
    assert len(repo.availability_changes) == 1


def test_ingestion_time_fallback_upgrade_to_trusted_is_audited():
    repo = InMemoryMetaRepository()
    ingested = _TS + timedelta(days=900)
    repo.upsert(_meta(ingested, "ingestion_time"), correction_context=_CONTEXT)
    assert repo.upsert(_meta(_AVAILABLE_1, "source_provided"),
                       correction_context=_CONTEXT) is True
    change = repo.availability_changes[0]
    assert change.old_availability_source == "ingestion_time"
    assert change.new_availability_source == "source_provided"  # never silent


def test_untrusted_incoming_claim_never_corrects_or_audits():
    """The online/service projection path (ingestion_time) cannot create trusted
    corrections — mirrors the metadata writer's descriptor projection."""
    repo = InMemoryMetaRepository()
    repo.upsert(_meta(_AVAILABLE_1, "source_provided"), correction_context=_CONTEXT)
    later = _AVAILABLE_1 + timedelta(days=400)
    assert repo.upsert(_meta(later, "ingestion_time")) is False
    assert repo.get_meta("rep_audit_1").available_at == _AVAILABLE_1
    assert repo.availability_changes == []


def test_audit_record_contains_no_payload_values_or_credentials():
    repo = InMemoryMetaRepository()
    repo.upsert(_meta(_AVAILABLE_1, "source_provided"), correction_context=_CONTEXT)
    repo.upsert(_meta(_AVAILABLE_2, "source_provided"), correction_context=_CONTEXT)
    serialized = json.dumps(dataclasses.asdict(repo.availability_changes[0]),
                            default=str)
    for forbidden in ("payload", "Authorization", "Bearer", "secret", "value_json"):
        assert forbidden not in serialized
    fields = set(dataclasses.asdict(repo.availability_changes[0]))
    assert fields == {"report_ref", "old_available_at", "new_available_at",
                      "old_availability_source", "new_availability_source",
                      "changed_at", "change_origin", "manifest_id", "request_id",
                      "actor_key_id"}


# --- Postgres path: atomicity ---------------------------------------------------------


class _TxCursor:
    def __init__(self, existing_row, fail_on_audit=False):
        self._row = existing_row
        self._fail_on_audit = fail_on_audit
        self.executed: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        if self._fail_on_audit and "raw_report_availability_changes" in sql:
            raise RuntimeError("audit insert failed")
        self.executed.append((sql, params))

    def fetchone(self):
        return self._row


class _TxConnection:
    def __init__(self, existing_row, fail_on_audit=False):
        self.cursor_obj = _TxCursor(existing_row, fail_on_audit)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


def _existing_row(meta: RawReportMeta) -> dict:
    return {
        "report_ref": meta.report_ref, "report_type": meta.report_type,
        "entity_type": meta.entity_type,
        "entity_key": [[n, v] for n, v in meta.entity_key.parts],
        "report_ts": meta.report_ts, "payload_size_bytes": meta.payload_size_bytes,
        "content_hash": meta.content_hash, "storage_uri": meta.storage_uri,
        "created_at": meta.created_at, "format": meta.format,
        "compression": meta.compression, "available_at": meta.available_at,
        "availability_source": meta.availability_source,
    }


def test_postgres_correction_commits_update_and_audit_in_one_transaction():
    conn = _TxConnection(_existing_row(_meta(_AVAILABLE_1, "source_provided")))
    repo = PostgresRawReportMetaRepository(conn)
    assert repo.upsert(_meta(_AVAILABLE_2, "source_provided"),
                       correction_context=_CONTEXT) is True
    statements = [sql for sql, _ in conn.cursor_obj.executed]
    assert any("UPDATE raw_reports_meta" in sql for sql in statements)
    assert any(sql.strip() == _AUDIT_INSERT_SQL.strip() for sql in statements)
    assert conn.commits == 1  # ONE transaction for both writes


def test_postgres_failed_audit_insert_commits_nothing():
    conn = _TxConnection(_existing_row(_meta(_AVAILABLE_1, "source_provided")),
                         fail_on_audit=True)
    repo = PostgresRawReportMetaRepository(conn)
    with pytest.raises(RuntimeError, match="audit insert failed"):
        repo.upsert(_meta(_AVAILABLE_2, "source_provided"),
                    correction_context=_CONTEXT)
    assert conn.commits == 0  # neither the update nor the audit row is committed