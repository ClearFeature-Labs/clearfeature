-- Extend the source-dataset manifest for both landing forms.
--
-- created the manifest for landing form (a) only. DWH ingestion adds landing
-- form (b) (precomputed feature rows -> offline history), so the manifest now carries a
-- landing_form and view/view_version, and report_type is optional (unset for (b)).
-- NOT auto-applied by docker-compose; apply it manually (or via a future init step).

ALTER TABLE source_dataset_manifests
    ADD COLUMN IF NOT EXISTS landing_form TEXT NOT NULL DEFAULT 'raw_reports';

ALTER TABLE source_dataset_manifests
    ADD COLUMN IF NOT EXISTS view TEXT NULL;

ALTER TABLE source_dataset_manifests
    ADD COLUMN IF NOT EXISTS view_version INTEGER NULL;

-- report_type is (a)-specific; feature-row (b) manifests leave it NULL.
ALTER TABLE source_dataset_manifests
    ALTER COLUMN report_type DROP NOT NULL;
