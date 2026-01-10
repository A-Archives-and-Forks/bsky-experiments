-- Aggregation views for user counts, profiles, list membership, and post engagement
-- Tables must be created before materialized views that depend on them
-- Note: POPULATE cannot be used with TO table, so we create MVs then INSERT to backfill

-- ============================================================================
-- PART 1: FOLLOW/BLOCK COUNT TABLES
-- ============================================================================

-- follower_counts: how many people follow each user
CREATE TABLE IF NOT EXISTS follower_counts (
    target_did String,
    follower_count SimpleAggregateFunction(sum, Int64)
) ENGINE = AggregatingMergeTree()
ORDER BY (target_did);

-- following_counts_agg: how many people each user follows (materialized version)
CREATE TABLE IF NOT EXISTS following_counts_agg (
    actor_did String,
    following_count SimpleAggregateFunction(sum, Int64)
) ENGINE = AggregatingMergeTree()
ORDER BY (actor_did);

-- block_counts: how many people each user has blocked
CREATE TABLE IF NOT EXISTS block_counts (
    actor_did String,
    block_count SimpleAggregateFunction(sum, Int64)
) ENGINE = AggregatingMergeTree()
ORDER BY (actor_did);

-- blocker_counts: how many people have blocked each user
CREATE TABLE IF NOT EXISTS blocker_counts (
    target_did String,
    blocker_count SimpleAggregateFunction(sum, Int64)
) ENGINE = AggregatingMergeTree()
ORDER BY (target_did);

-- ============================================================================
-- PART 2: PROFILES TABLE
-- ============================================================================

-- profiles: parsed user profiles from repo_records
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

-- ============================================================================
-- PART 3: LIST MEMBERSHIP TABLES
-- ============================================================================

-- list_items: parsed list membership from repo_records
CREATE TABLE IF NOT EXISTS list_items (
    actor_did String,
    rkey String,
    list_uri String,
    subject_did String,
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    deleted UInt8 DEFAULT 0,
    time_us Int64,
    INDEX idx_list list_uri TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_subject subject_did TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us, deleted)
ORDER BY (list_uri, subject_did, rkey);

-- list_member_counts: how many members each list has
CREATE TABLE IF NOT EXISTS list_member_counts (
    list_uri String,
    member_count SimpleAggregateFunction(sum, Int64)
) ENGINE = AggregatingMergeTree()
ORDER BY (list_uri);

-- list_membership_counts: how many lists each user is on
CREATE TABLE IF NOT EXISTS list_membership_counts (
    subject_did String,
    list_count SimpleAggregateFunction(sum, Int64)
) ENGINE = AggregatingMergeTree()
ORDER BY (subject_did);

-- ============================================================================
-- PART 4: POST ENGAGEMENT TABLES
-- ============================================================================

-- post_like_counts: likes per post
CREATE TABLE IF NOT EXISTS post_like_counts (
    subject_uri String,
    like_count SimpleAggregateFunction(sum, Int64)
) ENGINE = AggregatingMergeTree()
ORDER BY (subject_uri);

-- post_repost_counts: reposts per post
CREATE TABLE IF NOT EXISTS post_repost_counts (
    subject_uri String,
    repost_count SimpleAggregateFunction(sum, Int64)
) ENGINE = AggregatingMergeTree()
ORDER BY (subject_uri);

-- ============================================================================
-- MATERIALIZED VIEWS - FOLLOW/BLOCK COUNTS
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS follower_counts_mv
TO follower_counts
AS
SELECT
    target_did,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS follower_count
FROM follows
GROUP BY target_did;

CREATE MATERIALIZED VIEW IF NOT EXISTS following_counts_mv
TO following_counts_agg
AS
SELECT
    actor_did,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS following_count
FROM follows
GROUP BY actor_did;

CREATE MATERIALIZED VIEW IF NOT EXISTS block_counts_mv
TO block_counts
AS
SELECT
    actor_did,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS block_count
FROM blocks
GROUP BY actor_did;

CREATE MATERIALIZED VIEW IF NOT EXISTS blocker_counts_mv
TO blocker_counts
AS
SELECT
    target_did,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS blocker_count
FROM blocks
GROUP BY target_did;

-- ============================================================================
-- MATERIALIZED VIEWS - PROFILES
-- ============================================================================

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

-- ============================================================================
-- MATERIALIZED VIEWS - LIST MEMBERSHIP
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS list_items_mv
TO list_items
AS
SELECT
    repo AS actor_did,
    rkey,
    JSONExtractString(record_json, 'list') AS list_uri,
    JSONExtractString(record_json, 'subject') AS subject_did,
    created_at,
    now64(6) AS indexed_at,
    if(operation = 'delete', 1, 0) AS deleted,
    time_us
FROM repo_records
WHERE collection = 'app.bsky.graph.listitem';

CREATE MATERIALIZED VIEW IF NOT EXISTS list_member_counts_mv
TO list_member_counts
AS
SELECT
    list_uri,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS member_count
FROM list_items
GROUP BY list_uri;

CREATE MATERIALIZED VIEW IF NOT EXISTS list_membership_counts_mv
TO list_membership_counts
AS
SELECT
    subject_did,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS list_count
FROM list_items
GROUP BY subject_did;

-- ============================================================================
-- MATERIALIZED VIEWS - POST ENGAGEMENT
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS post_like_counts_mv
TO post_like_counts
AS
SELECT
    subject_uri,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS like_count
FROM likes
GROUP BY subject_uri;

CREATE MATERIALIZED VIEW IF NOT EXISTS post_repost_counts_mv
TO post_repost_counts
AS
SELECT
    subject_uri,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS repost_count
FROM reposts
GROUP BY subject_uri;

-- ============================================================================
-- BACKFILL EXISTING DATA
-- ============================================================================

-- Backfill follower counts
INSERT INTO follower_counts
SELECT
    target_did,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS follower_count
FROM follows
GROUP BY target_did;

-- Backfill following counts
INSERT INTO following_counts_agg
SELECT
    actor_did,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS following_count
FROM follows
GROUP BY actor_did;

-- Backfill block counts
INSERT INTO block_counts
SELECT
    actor_did,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS block_count
FROM blocks
GROUP BY actor_did;

-- Backfill blocker counts
INSERT INTO blocker_counts
SELECT
    target_did,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS blocker_count
FROM blocks
GROUP BY target_did;

-- Backfill profiles
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

-- Backfill list items
INSERT INTO list_items
SELECT
    repo AS actor_did,
    rkey,
    JSONExtractString(record_json, 'list') AS list_uri,
    JSONExtractString(record_json, 'subject') AS subject_did,
    created_at,
    now64(6) AS indexed_at,
    if(operation = 'delete', 1, 0) AS deleted,
    time_us
FROM repo_records
WHERE collection = 'app.bsky.graph.listitem';

-- Backfill list member counts (from list_items we just populated)
INSERT INTO list_member_counts
SELECT
    list_uri,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS member_count
FROM list_items
GROUP BY list_uri;

-- Backfill list membership counts
INSERT INTO list_membership_counts
SELECT
    subject_did,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS list_count
FROM list_items
GROUP BY subject_did;

-- Backfill post like counts
INSERT INTO post_like_counts
SELECT
    subject_uri,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS like_count
FROM likes
GROUP BY subject_uri;

-- Backfill post repost counts
INSERT INTO post_repost_counts
SELECT
    subject_uri,
    sumIf(1, deleted = 0) - sumIf(1, deleted = 1) AS repost_count
FROM reposts
GROUP BY subject_uri;
