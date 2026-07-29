-- Metadata Writer V1 projection tables.
-- Written asynchronously by the Metadata Writer (a separate Kafka consumer group),
-- never on the online critical path. No feature values, no raw payloads, no DLQ
-- source_payload_b64 are stored here.

-- Durable per-request snapshot.
CREATE TABLE IF NOT EXISTS feature_requests (
    request_id TEXT PRIMARY KEY,
    job_id TEXT,
    status TEXT,
    entity_type TEXT,
    entity_key_json JSONB,
    feature_view TEXT,
    view_version INTEGER,
    requested_features_json JSONB,
    requested_feature_groups_json JSONB,
    online_write_status TEXT,
    offline_write_status TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    error TEXT
);

-- Append-only audit log; unique event_hash makes replay idempotent.
CREATE TABLE IF NOT EXISTS request_events (
    event_hash TEXT PRIMARY KEY,
    request_id TEXT,
    event_type TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS request_events_request_id_idx
    ON request_events (request_id);
