-- Rollback aggregation views
-- Drop materialized views first, then tables (reverse dependency order)

-- ============================================================================
-- DROP MATERIALIZED VIEWS
-- ============================================================================

-- Post engagement MVs
DROP VIEW IF EXISTS post_repost_counts_mv;
DROP VIEW IF EXISTS post_like_counts_mv;

-- List membership MVs
DROP VIEW IF EXISTS list_membership_counts_mv;
DROP VIEW IF EXISTS list_member_counts_mv;
DROP VIEW IF EXISTS list_items_mv;

-- Profiles MV
DROP VIEW IF EXISTS profiles_mv;

-- Follow/block count MVs
DROP VIEW IF EXISTS blocker_counts_mv;
DROP VIEW IF EXISTS block_counts_mv;
DROP VIEW IF EXISTS following_counts_mv;
DROP VIEW IF EXISTS follower_counts_mv;

-- ============================================================================
-- DROP TABLES
-- ============================================================================

-- Post engagement tables
DROP TABLE IF EXISTS post_repost_counts;
DROP TABLE IF EXISTS post_like_counts;

-- List membership tables
DROP TABLE IF EXISTS list_membership_counts;
DROP TABLE IF EXISTS list_member_counts;
DROP TABLE IF EXISTS list_items;

-- Profiles table
DROP TABLE IF EXISTS profiles;

-- Follow/block count tables
DROP TABLE IF EXISTS blocker_counts;
DROP TABLE IF EXISTS block_counts;
DROP TABLE IF EXISTS following_counts_agg;
DROP TABLE IF EXISTS follower_counts;
