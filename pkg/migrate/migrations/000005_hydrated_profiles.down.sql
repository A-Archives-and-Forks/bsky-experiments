-- Drop the hydrated profiles table
DROP TABLE IF EXISTS profiles;

-- Recreate the original profiles table with raw record format
CREATE TABLE IF NOT EXISTS profiles (
    did String,
    rkey String,
    display_name String,
    description String,
    avatar_cid String,
    banner_cid String,
    labels Array(String),
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    deleted UInt8 DEFAULT 0,
    time_us Int64,
    INDEX idx_did did TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us, deleted)
ORDER BY (did, rkey);

-- Recreate the materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS profiles_mv
TO profiles
AS
SELECT
    repo AS did,
    rkey,
    JSONExtractString(record_json, 'displayName') AS display_name,
    JSONExtractString(record_json, 'description') AS description,
    JSONExtractString(record_json, 'avatar', 'ref', '$link') AS avatar_cid,
    JSONExtractString(record_json, 'banner', 'ref', '$link') AS banner_cid,
    arrayMap(x -> JSONExtractString(x, 'val'), JSONExtractArrayRaw(record_json, 'labels', 'values')) AS labels,
    created_at,
    now64(6) AS indexed_at,
    if(operation = 'delete', 1, 0) AS deleted,
    time_us
FROM repo_records
WHERE collection = 'app.bsky.actor.profile';

-- Backfill profiles from existing data
INSERT INTO profiles
SELECT
    repo AS did,
    rkey,
    JSONExtractString(record_json, 'displayName') AS display_name,
    JSONExtractString(record_json, 'description') AS description,
    JSONExtractString(record_json, 'avatar', 'ref', '$link') AS avatar_cid,
    JSONExtractString(record_json, 'banner', 'ref', '$link') AS banner_cid,
    arrayMap(x -> JSONExtractString(x, 'val'), JSONExtractArrayRaw(record_json, 'labels', 'values')) AS labels,
    created_at,
    now64(6) AS indexed_at,
    if(operation = 'delete', 1, 0) AS deleted,
    time_us
FROM repo_records
WHERE collection = 'app.bsky.actor.profile';
