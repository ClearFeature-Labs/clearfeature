-- Availability clock.
--
-- Four distinct time concepts: report_ts (business/as-of), available_at (when the
-- fact/value became available to the bank), created_at/ingested (platform acceptance),
-- calc_ts (computation). All columns are ADDITIVE AND NULLABLE: legacy rows keep the
-- conservative calc_ts fallback in PIT queries (COALESCE(available_at, calc_ts)) and
-- are never rewritten.
-- NOT auto-applied on existing volumes; apply via scripts/apply_postgres_migrations.sh.

ALTER TABLE features_offline
    ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NULL;

-- 'source_provided' (fully trusted input availability -> part of replay identity)
-- | 'ingestion_time' (incidental; PIT uses it, replays ignore it)
ALTER TABLE features_offline
    ADD COLUMN IF NOT EXISTS availability_source TEXT NULL;

ALTER TABLE raw_reports_meta
    ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NULL;

-- 'source_provided' (trusted operator ingestion supplied it) | 'ingestion_time'
ALTER TABLE raw_reports_meta
    ADD COLUMN IF NOT EXISTS availability_source TEXT NULL;
