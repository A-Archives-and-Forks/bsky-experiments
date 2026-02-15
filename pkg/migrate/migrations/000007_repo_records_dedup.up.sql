-- Convert repo_records to ReplacingMergeTree for deduplication during cursor replay
-- The time_us column becomes the version column for deduplication (higher wins)

-- Clean up from any previous failed attempts
DROP TABLE IF EXISTS repo_records_new;
DROP TABLE IF EXISTS repo_records_old;

-- Create new table with deduplication
CREATE TABLE repo_records_new (
    repo String,
    collection String,
    rkey String,
    operation Enum8('create' = 1, 'update' = 2, 'delete' = 3),
    record_json String,
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    time_us Int64,
    INDEX idx_repo repo TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_collection collection TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_created_at created_at TYPE minmax GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us)
ORDER BY (collection, repo, rkey, operation)
PARTITION BY toYYYYMM(created_at);

-- Copy data partition by partition to avoid timeout on large datasets
-- December 2025
INSERT INTO repo_records_new SELECT * FROM repo_records WHERE toYYYYMM(created_at) = 202512;

-- January 2026
INSERT INTO repo_records_new SELECT * FROM repo_records WHERE toYYYYMM(created_at) = 202601;

-- Swap tables
RENAME TABLE repo_records TO repo_records_old, repo_records_new TO repo_records;

-- Drop old table
DROP TABLE repo_records_old;
