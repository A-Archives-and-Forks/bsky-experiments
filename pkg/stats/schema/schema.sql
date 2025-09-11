-- Daily Stats View
CREATE TABLE daily_stats_summary (
    date date PRIMARY KEY,
    "Likes per Day" BIGINT NOT NULL,
    "Daily Active Likers" BIGINT NOT NULL,
    "Daily Active Posters" BIGINT NOT NULL,
    "Posts per Day" BIGINT NOT NULL,
    "Posts with Images per Day" BIGINT NOT NULL,
    "Images per Day" BIGINT NOT NULL,
    "Images with Alt Text per Day" BIGINT NOT NULL,
    "First Time Posters" BIGINT NOT NULL,
    "Follows per Day" BIGINT NOT NULL,
    "Daily Active Followers" BIGINT NOT NULL,
    "Blocks per Day" BIGINT NOT NULL,
    "Daily Active Blockers" BIGINT NOT NULL
);
CREATE INDEX daily_stats_summary_date ON daily_stats_summary (date);

-- HyperLogLog Table for Approximate Distinct Counts stored as binary data
CREATE TABLE hll_data (
    id SERIAL PRIMARY KEY,
    summary BIGINT DEFAULT 0 NOT NULL,
    metric_name TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    delete_after TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    hll BYTEA NOT NULL
);
CREATE UNIQUE INDEX hll_data_metric_window ON hll_data (metric_name, window_start, window_end);
CREATE INDEX hll_data_delete_after ON hll_data (delete_after);

-- Cursor table for tracking last processed event
CREATE TABLE stats_cursors (
    id SERIAL PRIMARY KEY,
    last_cursor BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);