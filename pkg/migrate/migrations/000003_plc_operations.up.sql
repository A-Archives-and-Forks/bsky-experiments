-- PLC (Public Ledger of Credentials) operations
-- Stores DID PLC operations from plc.directory/export

-- ============================================================================
-- TABLES
-- ============================================================================

-- plc_operations table - stores all PLC operations
CREATE TABLE IF NOT EXISTS plc_operations (
    did String,                              -- did:plc:xxx format
    cid String,                              -- Content identifier for this operation
    operation_type String,                   -- create, plc_operation, plc_tombstone
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),

    -- Extracted from operation payload for efficient querying
    handle String,                           -- Handle (at:// URI or bare)
    pds String,                              -- PDS endpoint URL

    -- Full operation payload for future extensibility
    operation_json String,

    -- Deduplication
    time_us Int64,

    INDEX idx_did did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_handle handle TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_pds pds TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_created_at created_at TYPE minmax GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us)
ORDER BY (did, created_at, cid)
PARTITION BY toYYYYMM(created_at);

-- plc_did_state table - latest state per DID for efficient lookups
CREATE TABLE IF NOT EXISTS plc_did_state (
    did String,
    cid String,
    handle String,
    pds String,
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    time_us Int64
) ENGINE = ReplacingMergeTree(time_us)
ORDER BY (did);

-- ============================================================================
-- MATERIALIZED VIEWS
-- ============================================================================

-- Auto-populate plc_did_state from plc_operations
CREATE MATERIALIZED VIEW IF NOT EXISTS plc_did_state_mv
TO plc_did_state AS
SELECT did, cid, handle, pds, created_at, now64(6) AS indexed_at, time_us
FROM plc_operations;
