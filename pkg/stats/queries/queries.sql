-- name: UpsertHLL :exec
INSERT INTO hll_data (
        metric_name,
        summary,
        window_start,
        window_end,
        delete_after,
        hll,
        created_at,
        updated_at
    )
VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
ON CONFLICT (metric_name, window_start, window_end) DO
UPDATE
SET hll = EXCLUDED.hll,
    updated_at = NOW(),
    summary = EXCLUDED.summary
WHERE hll_data.metric_name = EXCLUDED.metric_name
  AND hll_data.window_start = EXCLUDED.window_start
  AND hll_data.window_end = EXCLUDED.window_end;
-- name: GetHLL :one
SELECT *
FROM hll_data
WHERE metric_name = $1
  AND window_start = $2
  AND window_end = $3;
-- name: DeleteOldHLL :exec
DELETE
FROM hll_data
WHERE delete_after < NOW();
-- name: GetActiveHLLMetrics :many
SELECT * FROM hll_data
WHERE window_start >= $1
  AND window_end <= $2;
-- name: GetHLLsByMetricInRange :many
SELECT * FROM hll_data
WHERE metric_name = $1
  AND window_start >= $2
  AND window_end <= $3;
-- name: InsertDailyStatsSummary :exec
INSERT INTO daily_stats_summary (
        date,
        "Daily Active Users",
        "Likes per Day",
        "Daily Active Likers",
        "Posts per Day",
        "Daily Active Posters",
        "Follows per Day",
        "Daily Active Followers",
        "Blocks per Day",
        "Daily Active Blockers"
    )
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (date) DO
UPDATE
SET "Daily Active Users" = EXCLUDED."Daily Active Users",
    "Likes per Day" = EXCLUDED."Likes per Day",
    "Daily Active Likers" = EXCLUDED."Daily Active Likers",
    "Posts per Day" = EXCLUDED."Posts per Day",
    "Daily Active Posters" = EXCLUDED."Daily Active Posters",
    "Follows per Day" = EXCLUDED."Follows per Day",
    "Daily Active Followers" = EXCLUDED."Daily Active Followers",
    "Blocks per Day" = EXCLUDED."Blocks per Day",
    "Daily Active Blockers" = EXCLUDED."Daily Active Blockers"
WHERE daily_stats_summary.date = EXCLUDED.date;
-- name: GetDailyStatsSummary :one
SELECT *
FROM daily_stats_summary
WHERE date = $1;

-- name: GetCursor :one
SELECT last_cursor
FROM stats_cursors
ORDER BY id DESC
LIMIT 1;
-- name: UpsertCursor :exec
INSERT INTO stats_cursors (id, last_cursor, updated_at)
VALUES (1, $1, NOW())
ON CONFLICT (id) DO
UPDATE
SET last_cursor = EXCLUDED.last_cursor,
    updated_at = NOW()
WHERE stats_cursors.id = EXCLUDED.id;