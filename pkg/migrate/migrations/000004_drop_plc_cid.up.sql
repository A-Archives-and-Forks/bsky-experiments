-- Drop CID column from PLC tables to save ~6 GiB storage
-- CID is computable from operation_json if ever needed

-- 1. Create new plc_operations table without cid (also remove from ORDER BY)
CREATE TABLE plc_operations_new (
    did String,
    operation_type String,
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    handle String,
    pds String,
    operation_json String CODEC(ZSTD(3)),
    time_us Int64,
    INDEX idx_did did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_handle handle TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_pds pds TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_created_at created_at TYPE minmax GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us)
ORDER BY (did, created_at, time_us)
PARTITION BY toYYYYMM(created_at);

-- 2. Copy data (excluding cid) - extend timeout for large tables
INSERT INTO plc_operations_new
SELECT did, operation_type, created_at, indexed_at, handle, pds, operation_json, time_us
FROM plc_operations
SETTINGS max_execution_time = 3600;

-- 3. Drop the MV first (depends on plc_operations)
DROP VIEW IF EXISTS plc_did_state_mv;

-- 4. Swap tables
EXCHANGE TABLES plc_operations AND plc_operations_new;
DROP TABLE plc_operations_new;

-- 5. Drop cid from plc_did_state
ALTER TABLE plc_did_state DROP COLUMN cid;

-- 6. Recreate MV without cid
CREATE MATERIALIZED VIEW plc_did_state_mv TO plc_did_state AS
SELECT did, handle, pds, created_at, now64(6) AS indexed_at, time_us
FROM plc_operations;
