-- Source-dataset ingestion manifest + item index.
--
-- Landing form (a) run accounting: one manifest per ingestion run, one item per JSONL
-- row. Stores references and counts ONLY — raw payloads live in object storage, feature
-- values live in the offline store. No payloads, no object_key/storage_uri here.
-- NOT auto-applied by docker-compose; apply it manually (or via a future init step).

CREATE TABLE IF NOT EXISTS source_dataset_manifests (
    manifest_id             TEXT        PRIMARY KEY,
    dataset_id              TEXT        NOT NULL,
    source_kind             TEXT        NOT NULL,
    entity_type             TEXT        NOT NULL,
    source_name             TEXT        NOT NULL,
    report_type             TEXT        NOT NULL,
    copy_mode               TEXT        NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL,
    status                  TEXT        NOT NULL,
    input_uri               TEXT        NULL,
    created_by              TEXT        NULL,
    item_count_read         INTEGER     NOT NULL DEFAULT 0,
    item_count_written      INTEGER     NOT NULL DEFAULT 0,
    item_count_duplicate    INTEGER     NOT NULL DEFAULT 0,
    item_count_rejected     INTEGER     NOT NULL DEFAULT 0,
    watermark_min_event_ts  TIMESTAMPTZ NULL,
    watermark_max_event_ts  TIMESTAMPTZ NULL,
    content_hash            TEXT        NULL
);

CREATE INDEX IF NOT EXISTS ix_source_dataset_manifests_dataset
    ON source_dataset_manifests (dataset_id, created_at);

CREATE TABLE IF NOT EXISTS source_dataset_items (
    manifest_id   TEXT        NOT NULL REFERENCES source_dataset_manifests (manifest_id),
    item_index    INTEGER     NOT NULL,
    status        TEXT        NOT NULL,
    source_name   TEXT        NOT NULL,
    report_type   TEXT        NOT NULL,
    entity_key    JSONB       NULL,
    report_ref    TEXT        NULL,
    event_ts      TIMESTAMPTZ NULL,
    content_hash  TEXT        NULL,
    error         TEXT        NULL,
    PRIMARY KEY (manifest_id, item_index)
);

-- Batch jobs  will scan a manifest's written items by report_ref.
CREATE INDEX IF NOT EXISTS ix_source_dataset_items_report_ref
    ON source_dataset_items (manifest_id, report_ref);
