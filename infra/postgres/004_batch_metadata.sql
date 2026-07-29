-- Durable batch job/chunk audit projection.
-- Written asynchronously by the Metadata Writer from BatchChunkRequested (accepted) and
-- BatchChunkProcessed (completion) events. Never on the batch API / worker critical path.
-- Stores counts + bounded error summaries only: no feature values, inline_sources, raw
-- payloads, or object keys. No foreign key (tolerates processed-before-requested ordering).

CREATE TABLE IF NOT EXISTS batch_jobs (
    job_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    feature_view TEXT,
    view_version INTEGER,
    requested_features_json JSONB,
    requested_feature_groups_json JSONB,
    total_items INTEGER,
    chunk_count INTEGER,
    write_online BOOLEAN,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    error_summary_json JSONB
);

CREATE TABLE IF NOT EXISTS batch_chunks (
    chunk_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_count INTEGER NOT NULL,
    status TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    ok_items INTEGER NOT NULL DEFAULT 0,
    failed_items INTEGER NOT NULL DEFAULT 0,
    first_errors_json JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS batch_chunks_job_id_idx ON batch_chunks (job_id);
