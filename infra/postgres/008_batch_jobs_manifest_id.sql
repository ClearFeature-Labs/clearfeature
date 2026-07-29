-- Record the source dataset a batch job computed from.
--
-- Dataset-scoped batch jobs (scope.type = source_dataset_manifest) reference a
-- SourceDatasetManifest; batch_jobs now carries that manifest_id for durable audit
-- (which landed dataset a job's offline rows came from). NULL for inline jobs.
-- NOT auto-applied by docker-compose; apply it manually (or via a future init step).

ALTER TABLE batch_jobs
    ADD COLUMN IF NOT EXISTS manifest_id TEXT NULL;
