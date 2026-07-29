-- Schema for the raw report metadata repository.
--
-- Mirrors the fs_core RawReportMeta model exactly (no extra columns). entity_key is
-- stored as an ordered JSONB array of [name, value] pairs so the deterministic key
-- order is preserved and the EntityKey can be reconstructed exactly. Timestamps are
-- TIMESTAMPTZ so values round-trip as timezone-aware datetimes.
--
-- This file is NOT auto-applied by docker-compose; apply it manually (or via a
-- future init step) against the local Postgres from docker-compose.yml.

CREATE TABLE IF NOT EXISTS raw_reports_meta (
    report_ref         TEXT PRIMARY KEY,
    report_type        TEXT        NOT NULL,
    entity_type        TEXT        NOT NULL,
    entity_key         JSONB       NOT NULL,
    report_ts          TIMESTAMPTZ NOT NULL,
    payload_size_bytes BIGINT      NOT NULL CHECK (payload_size_bytes >= 0),
    content_hash       TEXT        NOT NULL,
    storage_uri        TEXT        NOT NULL,
    format             TEXT        NOT NULL,
    compression        TEXT        NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL
);
