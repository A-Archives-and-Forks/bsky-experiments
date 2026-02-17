CREATE TABLE IF NOT EXISTS crawl_repos (
    did String,
    pds String,
    head String,
    rev String,
    active UInt8 DEFAULT 1,
    status String DEFAULT '',
    verified UInt8 DEFAULT 0,
    prepared_at DateTime64(6) DEFAULT now64(6),
    time_us Int64
) ENGINE = ReplacingMergeTree(time_us)
ORDER BY (did)
SETTINGS index_granularity = 8192;
