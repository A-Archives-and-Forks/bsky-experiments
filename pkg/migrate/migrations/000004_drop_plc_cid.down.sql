-- Rollback: Recreate tables with cid column (restore original schema)
-- Note: CID data will be lost and cannot be recovered

-- 1. Create plc_operations with cid
CREATE TABLE plc_operations_new (
    did String,
    cid String,
    operation_type String,
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    handle String,
    pds String,
    operation_json String,
    time_us Int64,
    INDEX idx_did did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_handle handle TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_pds pds TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_created_at created_at TYPE minmax GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us)
ORDER BY (did, created_at, cid)
PARTITION BY toYYYYMM(created_at);

-- 2. Copy data (cid will be empty string) - extend timeout for large tables
INSERT INTO plc_operations_new
SELECT did, '' AS cid, operation_type, created_at, indexed_at, handle, pds, operation_json, time_us
FROM plc_operations
SETTINGS max_execution_time = 3600;

-- 3. Drop MV
DROP VIEW IF EXISTS plc_did_state_mv;

-- 4. Swap
EXCHANGE TABLES plc_operations AND plc_operations_new;
DROP TABLE plc_operations_new;

-- 5. Add cid back to plc_did_state
ALTER TABLE plc_did_state ADD COLUMN cid String DEFAULT '' AFTER did;

-- 6. Recreate MV with cid
CREATE MATERIALIZED VIEW plc_did_state_mv TO plc_did_state AS
SELECT did, '' AS cid, handle, pds, created_at, now64(6) AS indexed_at, time_us
FROM plc_operations;
