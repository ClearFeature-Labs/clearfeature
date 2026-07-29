-- Append-only audit trail for trusted availability corrections.
--
-- Every accepted correction of a raw report's trusted available_at writes exactly one
-- row here, IN THE SAME TRANSACTION as the raw_reports_meta update — old and new
-- values, provenance, origin and time are preserved durably. Never stores payloads,
-- feature values, or credentials; actor_key_id is optional (NULL when the ingestion
-- path does not safely carry one — manifest/request ids are the origin evidence).
-- NOT auto-applied on existing volumes; apply via scripts/apply_postgres_migrations.sh.

CREATE TABLE IF NOT EXISTS raw_report_availability_changes (
    change_id               BIGSERIAL   PRIMARY KEY,
    report_ref              TEXT        NOT NULL,
    old_available_at        TIMESTAMPTZ NULL,
    new_available_at        TIMESTAMPTZ NOT NULL,
    old_availability_source TEXT        NULL,
    new_availability_source TEXT        NOT NULL,
    changed_at              TIMESTAMPTZ NOT NULL,
    -- real correction-capable paths only: jsonl_ingestion | dwh_import
    change_origin           TEXT        NOT NULL,
    manifest_id             TEXT        NULL,
    request_id              TEXT        NULL,
    actor_key_id            TEXT        NULL
);

CREATE INDEX IF NOT EXISTS raw_report_availability_changes_ref_idx
    ON raw_report_availability_changes (report_ref);
