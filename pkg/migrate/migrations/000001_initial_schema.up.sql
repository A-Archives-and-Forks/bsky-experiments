-- Initial schema for atproto (default database)
-- Tables must be created before views that depend on them
-- Materialized views must be created after their source tables

-- ============================================================================
-- TABLES
-- ============================================================================

-- repo_records table - stores all records with metadata
CREATE TABLE IF NOT EXISTS repo_records (
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

-- follows table - optimized for analytics on follows
CREATE TABLE IF NOT EXISTS follows (
    actor_did String,
    target_did String,
    rkey String,
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    deleted UInt8 DEFAULT 0,
    time_us Int64,
    INDEX idx_actor actor_did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_target target_did TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us, deleted)
ORDER BY (actor_did, target_did, rkey)
PARTITION BY toYYYYMM(created_at);

-- likes table - optimized for analytics on likes
CREATE TABLE IF NOT EXISTS likes (
    actor_did String,
    subject_uri String,
    subject_did String,
    subject_collection String,
    subject_rkey String,
    rkey String,
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    deleted UInt8 DEFAULT 0,
    time_us Int64,
    INDEX idx_actor actor_did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_subject_did subject_did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_subject_uri subject_uri TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us, deleted)
ORDER BY (subject_did, subject_collection, subject_rkey, actor_did, rkey)
PARTITION BY toYYYYMM(created_at);

-- reposts table - optimized for analytics on reposts
CREATE TABLE IF NOT EXISTS reposts (
    actor_did String,
    subject_uri String,
    subject_did String,
    subject_collection String,
    subject_rkey String,
    rkey String,
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    deleted UInt8 DEFAULT 0,
    time_us Int64,
    INDEX idx_actor actor_did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_subject_did subject_did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_subject_uri subject_uri TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us, deleted)
ORDER BY (subject_did, subject_collection, subject_rkey, actor_did, rkey)
PARTITION BY toYYYYMM(created_at);

-- blocks table - optimized for analytics on blocks
CREATE TABLE IF NOT EXISTS blocks (
    actor_did String,
    target_did String,
    rkey String,
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    deleted UInt8 DEFAULT 0,
    time_us Int64,
    INDEX idx_actor actor_did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_target target_did TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us, deleted)
ORDER BY (actor_did, target_did, rkey)
PARTITION BY toYYYYMM(created_at);

-- posts table - optimized for post data and analytics
CREATE TABLE IF NOT EXISTS posts (
    did String,
    rkey String,
    uri String,
    created_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    text String,
    langs Array(String),
    has_embedded_media UInt8,
    parent_uri String,
    root_uri String,
    deleted UInt8 DEFAULT 0,
    time_us Int64,
    INDEX idx_did did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_created_at created_at TYPE minmax GRANULARITY 1,
    INDEX idx_uri uri TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us, deleted)
ORDER BY (did, rkey)
PARTITION BY toYYYYMM(created_at);

-- actor_labels table - for private feed access control
CREATE TABLE IF NOT EXISTS actor_labels (
    actor_did String,
    label String,
    created_at DateTime64(6) DEFAULT now64(6),
    deleted UInt8 DEFAULT 0,
    time_us Int64,
    INDEX idx_actor actor_did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_label label TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us, deleted)
ORDER BY (actor_did, label);

-- daily_stats table - aggregated daily statistics
CREATE TABLE IF NOT EXISTS daily_stats (
    date Date,
    metric_type String,
    count SimpleAggregateFunction(sum, UInt64),
    unique_actors AggregateFunction(uniq, String),
    unique_targets AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
ORDER BY (date, metric_type);

-- api_keys table - for API key authentication
CREATE TABLE IF NOT EXISTS api_keys (
    api_key String,
    auth_entity String,
    assigned_user String,
    created_at DateTime64(6) DEFAULT now64(6),
    deleted UInt8 DEFAULT 0,
    time_us Int64,
    INDEX idx_api_key api_key TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us, deleted)
ORDER BY (api_key);

-- repo_cleanup_jobs table - for tracking repo cleanup operations
CREATE TABLE IF NOT EXISTS repo_cleanup_jobs (
    job_id String,
    repo String,
    refresh_token String,
    cleanup_types Array(String),
    delete_older_than DateTime64(6),
    num_deleted Int64,
    num_deleted_today Int64,
    est_num_remaining Int64,
    job_state String,
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6),
    last_deleted_at Nullable(DateTime64(6)),
    time_us Int64,
    INDEX idx_repo repo TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_job_state job_state TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us)
ORDER BY (job_id);

-- sentry_actions table - for tracking detected inauthentic behavior
CREATE TABLE IF NOT EXISTS sentry_actions (
    action_id String,
    actor_did String,
    heuristic_name String,
    action_type String,
    severity Enum8('low' = 1, 'medium' = 2, 'high' = 3, 'critical' = 4),
    reasoning String,
    evidence_json String,
    detected_at DateTime64(6) DEFAULT now64(6),
    time_us Int64,
    INDEX idx_actor actor_did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_heuristic heuristic_name TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_severity severity TYPE set(4) GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us)
ORDER BY (actor_did, heuristic_name, action_id)
PARTITION BY toYYYYMM(detected_at);

-- ============================================================================
-- VIEWS
-- ============================================================================

-- recent_posts view - posts from the past 72 hours
CREATE VIEW IF NOT EXISTS recent_posts AS
SELECT
    did AS actor_did,
    rkey,
    uri,
    created_at,
    indexed_at,
    text,
    langs,
    has_embedded_media,
    parent_uri,
    root_uri
FROM posts
WHERE deleted = 0
    AND created_at > now() - INTERVAL 72 HOUR
ORDER BY rkey DESC;

-- recent_posts_with_score view - hot/trending posts with engagement score
CREATE VIEW IF NOT EXISTS recent_posts_with_score AS
SELECT
    p.uri AS subject_uri,
    p.did AS actor_did,
    p.rkey AS rkey,
    p.created_at AS subject_created_at,
    p.indexed_at AS inserted_at,
    p.langs AS langs,
    p.has_embedded_media AS has_embedded_media,
    toFloat64(
        (like_count - 1) /
        pow(dateDiff('hour', p.created_at, now()) + 2, 1.8)
    ) AS score,
    like_count
FROM posts AS p
LEFT JOIN (
    SELECT
        subject_uri,
        countIf(deleted = 0) AS like_count
    FROM likes
    WHERE created_at > now() - INTERVAL 24 HOUR
    GROUP BY subject_uri
) AS like_stats ON p.uri = like_stats.subject_uri
WHERE p.deleted = 0
    AND p.created_at > now() - INTERVAL 24 HOUR
    AND p.created_at < now() - INTERVAL 5 MINUTE
    AND p.parent_uri = ''
    AND p.root_uri = ''
    AND like_count >= 15
ORDER BY score DESC;

-- following_counts view - for filtering out users following too many people
CREATE VIEW IF NOT EXISTS following_counts AS
SELECT
    actor_did,
    countIf(deleted = 0) AS num_following
FROM follows
GROUP BY actor_did;

-- ============================================================================
-- MATERIALIZED VIEWS
-- ============================================================================

-- mpls materialized view - Minneapolis feed (posts from actors with 'mpls' label)
CREATE MATERIALIZED VIEW IF NOT EXISTS mpls
ENGINE = ReplacingMergeTree()
ORDER BY (rkey)
TTL created_at + INTERVAL 72 HOUR
POPULATE AS
SELECT
    p.did AS actor_did,
    p.rkey AS rkey,
    p.created_at AS created_at
FROM posts AS p
INNER JOIN (
    SELECT DISTINCT actor_did
    FROM actor_labels
    WHERE label = 'mpls' AND deleted = 0
) AS labeled_actors ON p.did = labeled_actors.actor_did
WHERE p.deleted = 0
    AND p.parent_uri = ''
    AND p.root_uri = '';

-- tqsp materialized view - TQSP feed (posts from actors with 'tqsp' label)
CREATE MATERIALIZED VIEW IF NOT EXISTS tqsp
ENGINE = ReplacingMergeTree()
ORDER BY (rkey)
TTL created_at + INTERVAL 72 HOUR
POPULATE AS
SELECT
    p.did AS actor_did,
    p.rkey AS rkey,
    p.created_at AS created_at
FROM posts AS p
INNER JOIN (
    SELECT DISTINCT actor_did
    FROM actor_labels
    WHERE label = 'tqsp' AND deleted = 0
) AS labeled_actors ON p.did = labeled_actors.actor_did
WHERE p.deleted = 0
    AND p.parent_uri = ''
    AND p.root_uri = '';

-- daily_stats_posts materialized view - posts statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_stats_posts
TO daily_stats
AS
SELECT
    toDate(indexed_at) AS date,
    'posts' AS metric_type,
    countIf(deleted = 0) AS count,
    uniqState(if(deleted = 0, did, '')) AS unique_actors,
    uniqState('') AS unique_targets
FROM posts
GROUP BY date;

-- daily_stats_likes materialized view - likes statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_stats_likes
TO daily_stats
AS
SELECT
    toDate(indexed_at) AS date,
    'likes' AS metric_type,
    countIf(deleted = 0) AS count,
    uniqState(if(deleted = 0, actor_did, '')) AS unique_actors,
    uniqState(if(deleted = 0, subject_did, '')) AS unique_targets
FROM likes
GROUP BY date;

-- daily_stats_follows materialized view - follows statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_stats_follows
TO daily_stats
AS
SELECT
    toDate(indexed_at) AS date,
    'follows' AS metric_type,
    countIf(deleted = 0) AS count,
    uniqState(if(deleted = 0, actor_did, '')) AS unique_actors,
    uniqState(if(deleted = 0, target_did, '')) AS unique_targets
FROM follows
GROUP BY date;

-- daily_stats_blocks materialized view - blocks statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_stats_blocks
TO daily_stats
AS
SELECT
    toDate(indexed_at) AS date,
    'blocks' AS metric_type,
    countIf(deleted = 0) AS count,
    uniqState(if(deleted = 0, actor_did, '')) AS unique_actors,
    uniqState(if(deleted = 0, target_did, '')) AS unique_targets
FROM blocks
GROUP BY date;
