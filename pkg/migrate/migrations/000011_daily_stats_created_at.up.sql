-- Re-bucket daily_stats materialized views on created_at instead of indexed_at.
-- Bucketing on indexed_at attributed any catch-up backlog to the indexer's
-- recovery day, while created_at preserves the actual event date.
-- The one-off repair of the 2026-04-14..2026-04-25 window that motivated this
-- change lives in scripts/daily_stats_repair_2026-04.sql.

DROP VIEW IF EXISTS daily_stats_posts;
DROP VIEW IF EXISTS daily_stats_likes;
DROP VIEW IF EXISTS daily_stats_follows;
DROP VIEW IF EXISTS daily_stats_blocks;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_stats_posts
TO daily_stats
AS
SELECT
    toDate(created_at) AS date,
    'posts' AS metric_type,
    countIf(deleted = 0) AS count,
    uniqState(if(deleted = 0, did, '')) AS unique_actors,
    uniqState('') AS unique_targets
FROM posts
GROUP BY date;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_stats_likes
TO daily_stats
AS
SELECT
    toDate(created_at) AS date,
    'likes' AS metric_type,
    countIf(deleted = 0) AS count,
    uniqState(if(deleted = 0, actor_did, '')) AS unique_actors,
    uniqState(if(deleted = 0, subject_did, '')) AS unique_targets
FROM likes
GROUP BY date;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_stats_follows
TO daily_stats
AS
SELECT
    toDate(created_at) AS date,
    'follows' AS metric_type,
    countIf(deleted = 0) AS count,
    uniqState(if(deleted = 0, actor_did, '')) AS unique_actors,
    uniqState(if(deleted = 0, target_did, '')) AS unique_targets
FROM follows
GROUP BY date;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_stats_blocks
TO daily_stats
AS
SELECT
    toDate(created_at) AS date,
    'blocks' AS metric_type,
    countIf(deleted = 0) AS count,
    uniqState(if(deleted = 0, actor_did, '')) AS unique_actors,
    uniqState(if(deleted = 0, target_did, '')) AS unique_targets
FROM blocks
GROUP BY date;
