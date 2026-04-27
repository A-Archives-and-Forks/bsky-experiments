-- One-off repair of daily_stats for the 2026-04-14..2026-04-25 catch-up window.
--
-- The indexer was offline for ~11 days (2026-04-14..2026-04-24) and most of the
-- backlog was indexed on 2026-04-25, so the daily_stats MV (which used to bucket
-- on indexed_at) shows ~0 events for the gap days and ~11x normal on 2026-04-25.
-- Migration 000011 fixes the MV definitions going forward; this script repairs
-- the existing data.
--
-- Run AFTER migration 000011 has been applied. Run order within this file
-- (DELETE first, INSERT second) keeps the intermediate state as a brief
-- undercount rather than a brief overcount.
--
-- Expected per-day post counts after repair (from a dry run on 2026-04-27):
--   2026-04-14: 4,287       (real hole — past Jetstream's replay buffer)
--   2026-04-15..25: ~3M each (normal range)
--
-- Likes / follows / blocks scale similarly. Verify with:
--   SELECT date, sumIf(count, metric_type='posts') FROM (
--       SELECT date, metric_type, sumMerge(count) AS count
--       FROM daily_stats WHERE date BETWEEN '2026-04-14' AND '2026-04-25'
--       GROUP BY date, metric_type
--       UNION ALL
--       SELECT date, metric_type, sum(count) FROM daily_stats_backfill
--       WHERE date BETWEEN '2026-04-14' AND '2026-04-25'
--       GROUP BY date, metric_type
--   ) GROUP BY date ORDER BY date;

ALTER TABLE daily_stats
DELETE WHERE date >= '2026-04-14' AND date <= '2026-04-25'
SETTINGS mutations_sync = 2;

INSERT INTO daily_stats_backfill
SELECT toDate(created_at) AS date, 'posts' AS metric_type,
       countIf(deleted = 0) AS count,
       uniqIf(did, deleted = 0) AS unique_actors,
       toUInt64(0) AS unique_targets
FROM posts
WHERE created_at >= '2026-04-14' AND created_at < '2026-04-26'
GROUP BY date;

INSERT INTO daily_stats_backfill
SELECT toDate(created_at) AS date, 'likes' AS metric_type,
       countIf(deleted = 0) AS count,
       uniqIf(actor_did, deleted = 0) AS unique_actors,
       uniqIf(subject_did, deleted = 0) AS unique_targets
FROM likes
WHERE created_at >= '2026-04-14' AND created_at < '2026-04-26'
GROUP BY date;

INSERT INTO daily_stats_backfill
SELECT toDate(created_at) AS date, 'follows' AS metric_type,
       countIf(deleted = 0) AS count,
       uniqIf(actor_did, deleted = 0) AS unique_actors,
       uniqIf(target_did, deleted = 0) AS unique_targets
FROM follows
WHERE created_at >= '2026-04-14' AND created_at < '2026-04-26'
GROUP BY date;

INSERT INTO daily_stats_backfill
SELECT toDate(created_at) AS date, 'blocks' AS metric_type,
       countIf(deleted = 0) AS count,
       uniqIf(actor_did, deleted = 0) AS unique_actors,
       uniqIf(target_did, deleted = 0) AS unique_targets
FROM blocks
WHERE created_at >= '2026-04-14' AND created_at < '2026-04-26'
GROUP BY date;
