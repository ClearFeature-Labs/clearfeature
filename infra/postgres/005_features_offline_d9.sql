-- D9 write-guard metadata for offline feature history.
--
-- Adds the per-feature freshness/identity metadata next to data_ts:
--   max_input_data_ts  max(data_ts of the feature's inputs); for F1 equals data_ts
--   input_fingerprint  canonical hash over (input_id, input_data_ts, input_value_hash)
--   value_hash         canonical hash of the feature value
--
-- All nullable: legacy rows and externally supplied results (model scores, table
-- imports) have no D9 metadata and degenerate to plain data_ts semantics.
-- NOT auto-applied by docker-compose; apply it manually (or via a future init step).

ALTER TABLE features_offline
    ADD COLUMN IF NOT EXISTS max_input_data_ts TIMESTAMPTZ NULL;

ALTER TABLE features_offline
    ADD COLUMN IF NOT EXISTS input_fingerprint TEXT NULL;

ALTER TABLE features_offline
    ADD COLUMN IF NOT EXISTS value_hash TEXT NULL;
