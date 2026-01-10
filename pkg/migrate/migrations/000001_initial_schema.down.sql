-- Rollback initial schema
-- Order: Materialized views first (they reference tables), then views, then tables

-- ============================================================================
-- DROP MATERIALIZED VIEWS
-- ============================================================================

DROP VIEW IF EXISTS daily_stats_blocks;
DROP VIEW IF EXISTS daily_stats_follows;
DROP VIEW IF EXISTS daily_stats_likes;
DROP VIEW IF EXISTS daily_stats_posts;
DROP VIEW IF EXISTS tqsp;
DROP VIEW IF EXISTS mpls;

-- ============================================================================
-- DROP VIEWS
-- ============================================================================

DROP VIEW IF EXISTS following_counts;
DROP VIEW IF EXISTS recent_posts_with_score;
DROP VIEW IF EXISTS recent_posts;

-- ============================================================================
-- DROP TABLES
-- ============================================================================

DROP TABLE IF EXISTS sentry_actions;
DROP TABLE IF EXISTS repo_cleanup_jobs;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS daily_stats;
DROP TABLE IF EXISTS actor_labels;
DROP TABLE IF EXISTS posts;
DROP TABLE IF EXISTS blocks;
DROP TABLE IF EXISTS reposts;
DROP TABLE IF EXISTS likes;
DROP TABLE IF EXISTS follows;
DROP TABLE IF EXISTS repo_records;
