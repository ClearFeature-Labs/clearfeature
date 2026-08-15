-- Multi-manifest batch scope: a source_dataset_manifests job joins several
-- SourceDatasetManifests; batch_jobs durably retains the COMPLETE input set for audit.
-- Additive and backward compatible: legacy rows stay NULL; the singular manifest_id
-- column (008) is unchanged. Safe to re-run (IF NOT EXISTS).
-- NOT auto-applied on existing volumes; apply via scripts/apply_postgres_migrations.sh.

ALTER TABLE batch_jobs
    ADD COLUMN IF NOT EXISTS manifest_ids JSONB NULL;
