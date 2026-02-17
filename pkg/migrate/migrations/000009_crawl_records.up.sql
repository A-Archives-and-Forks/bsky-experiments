CREATE TABLE IF NOT EXISTS crawl_records (
    repo String,
    collection String,
    rkey String,
    record_json String CODEC(ZSTD(3)),
    crawled_at DateTime64(6),
    indexed_at DateTime64(6) DEFAULT now64(6),
    time_us Int64,
    INDEX idx_repo repo TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_collection collection TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_crawled_at crawled_at TYPE minmax GRANULARITY 1
) ENGINE = ReplacingMergeTree(time_us)
ORDER BY (collection, repo, rkey)
PARTITION BY toYYYYMM(crawled_at);
