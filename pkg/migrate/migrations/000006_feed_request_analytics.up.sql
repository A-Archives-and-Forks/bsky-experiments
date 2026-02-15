CREATE TABLE IF NOT EXISTS feed_request_analytics (
    user_did String,
    feed_name String,
    limit_requested UInt16,
    has_cursor UInt8,
    requested_at DateTime64(6) DEFAULT now64(6),
    time_us Int64,
    INDEX idx_user_did user_did TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_feed_name feed_name TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_requested_at requested_at TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (feed_name, user_did, time_us)
PARTITION BY toYYYYMM(requested_at)
TTL requested_at + INTERVAL 90 DAY;
