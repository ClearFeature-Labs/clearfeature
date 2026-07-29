-- Schema for the offline feature store.
--
-- Tall, append-only history: every computed FeatureResult becomes one row; recomputing
-- a feature appends a new row (no upsert, no delete). row_id (BIGSERIAL) gives
-- deterministic append order. entity_key is stored as ordered JSONB [name, value] pairs
-- for exact EntityKey reconstruction; entity_key_encoded is EntityKey.encode() and is the
-- efficient lookup key. value_json holds the feature value (Python None -> JSONB null).
--
-- Mirrors OfflineFeatureRecord(view, view_version, FeatureResult) — no extra domain fields.
-- NOT auto-applied by docker-compose; apply it manually (or via a future init step).

CREATE TABLE IF NOT EXISTS features_offline (
    row_id             BIGSERIAL   PRIMARY KEY,
    entity_key         JSONB       NOT NULL,
    entity_key_encoded TEXT        NOT NULL,
    view               TEXT        NOT NULL,
    view_version       INTEGER     NOT NULL,
    feature_name       TEXT        NOT NULL,
    feature_version    INTEGER     NOT NULL,
    value_json         JSONB       NOT NULL,
    data_ts            TIMESTAMPTZ NOT NULL,
    calc_ts            TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_features_offline_lookup
    ON features_offline (
        entity_key_encoded,
        view,
        view_version,
        feature_name,
        feature_version
    );
