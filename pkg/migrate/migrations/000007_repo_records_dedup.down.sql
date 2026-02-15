-- Revert repo_records to plain MergeTree (loses deduplication capability)

-- Create old-style table without deduplication
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
) ENGINE = MergeTree()
ORDER BY (collection, repo, rkey, time_us)
PARTITION BY toYYYYMM(created_at);

-- Copy data
INSERT INTO repo_records_new SELECT * FROM repo_records;

-- Swap tables
RENAME TABLE repo_records TO repo_records_old, repo_records_new TO repo_records;

-- Drop old table
DROP TABLE repo_records_old;
