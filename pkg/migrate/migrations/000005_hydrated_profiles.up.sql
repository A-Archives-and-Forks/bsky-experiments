-- Drop the old materialized view that expected raw record format
DROP VIEW IF EXISTS profiles_mv;

-- Drop and recreate profiles table with hydrated format
DROP TABLE IF EXISTS profiles;

CREATE TABLE profiles (
    did String,
    handle String,
    display_name String,
    description String,
    avatar_cid String,          -- Extracted from CDN URL
    banner_cid String,          -- Extracted from CDN URL
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    profile_json String,        -- Full API response for extensibility
    deleted UInt8 DEFAULT 0,
    time_us Int64,
    INDEX idx_did did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_handle handle TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us, deleted)
ORDER BY (did);
