-- F3 model lineage + bundle digest on offline rows.
--
-- The  FeatureResult fields (model_uri/model_digest/model_output_name/
-- bundle_digest) were carried by the in-memory store but silently dropped by the
-- Postgres store — found live by the credit demo's lineage check. Nullable and
-- additive: F1/F2/legacy rows have none. Idempotent (IF NOT EXISTS), like 001-008.

ALTER TABLE features_offline
    ADD COLUMN IF NOT EXISTS model_uri TEXT;
ALTER TABLE features_offline
    ADD COLUMN IF NOT EXISTS model_digest TEXT;
ALTER TABLE features_offline
    ADD COLUMN IF NOT EXISTS model_output_name TEXT;
ALTER TABLE features_offline
    ADD COLUMN IF NOT EXISTS bundle_digest TEXT;
