# Crawler

CLI tool that crawls every ATProto repo from every PDS on the network and archives them into `.rca` (Repo Archive) segment files. Uses the PLC DID directory stored in ClickHouse as its source of truth for which DIDs exist and which PDS hosts each one.

## Commands

| Command | Description | Key flags |
|---------|-------------|-----------|
| `crawl` | Full network crawl | `--workers` (50), `--default-pds-rps` (5), `--segment-size` (2GB), `--zstd-level` (3), `--page-size` (50000), `--skip-pds`, `--resume` |
| `replay` | Replay `.rca` archives into ClickHouse `repo_records` table | `--input-dir`, `--collections` (filter) |
| `inspect` | Print segment header, index, and per-collection stats | `<segment-file>` positional arg |
| `status` | Show crawl progress from Redis | `--redis-address` |
| `reset` | Clear Redis state and delete `.rca` files | `--redis-address`, `--output-dir` |

All commands share `--redis-address` and `--clickhouse-*` connection flags.

## Architecture

4-stage concurrent pipeline:

```
ClickHouse ──page──▶ Coordinator ──groups──▶ Dispatcher ──tasks──▶ Workers ──repos──▶ Writer
(plc_did_state)       (crawler.go)          (dispatcher.go)       (worker.go)        (writerLoop)
```

### 1. Coordinator (`pkg/crawler/crawler.go`)

Pages DIDs from `plc_did_state FINAL` ordered by DID, groups them by PDS URL into `map[string][]string`, and feeds pages to the dispatcher. Implements backpressure: waits when dispatcher has >10x page size remaining (unless PDS diversity is low, capped at 50 pages / ~2.5M DIDs to prevent OOM). Tracks cursor in Redis for resume. Clears cursor on successful completion.

### 2. Dispatcher (`pkg/crawler/dispatcher.go`)

Receives pages via `Feed()`, maintains per-PDS state with token-bucket rate limiting (`golang.org/x/time/rate`). Adaptive rate selection based on PDS size:
- `<100 DIDs`: 2 RPS (small self-hosted)
- `<1000 DIDs`: default RPS (5)
- `>=1000 DIDs`: 10 RPS (infrastructure PDSs like bsky.network)

Error-based adaptation: halves rate every 5 consecutive failures (floor 0.5 RPS). After 10 consecutive failures (`pdsBlockThreshold`), blocks the PDS entirely and skips its remaining DIDs. Successes reset error count and restore the original rate.

### 3. Worker Pool (`pkg/crawler/worker.go`)

50 concurrent goroutines (configurable). Each worker:
1. HTTP GET `{pds}/xrpc/com.atproto.sync.getRepo?did={did}` with `Accept: application/vnd.ipld.car`
2. Body capped at `MaxRepoSizeMB` (100MB) via `io.LimitReader`
3. Custom CAR parser (`pkg/crawler/carrepo/`) — skips SHA256 block verification for ~30-40% CPU savings
4. MST walk extracts all records, CBOR decoded to JSON with CID links stripped (`cborjson.go`)
5. Groups records by collection, sends `CrawledRepo` to writer channel

Error classification: `not_found`, `deactivated`, `takendown` (permanent/per-repo), `rate_limited`, `dns_error`, `unavailable`, `timeout`, `connection_error` (PDS-level, reported to dispatcher for blocking decisions).

### 4. Segment Writer (`writerLoop` in `crawler.go`, `pkg/repoarchive/writer.go`)

Receives `CrawledRepo` from a buffered channel (cap 100). Serializes to `.rca` format. Rotates to a new segment file when current exceeds target size (default 2GB). Saves stats to Redis every 30s. Segment files named `segment_NNNN.rca`.

## Key Packages

| Package | Purpose |
|---------|---------|
| `pkg/crawler/crawler.go` | Orchestrator, page loop, backpressure, writer goroutine |
| `pkg/crawler/dispatcher.go` | Per-PDS rate limiting, adaptive backoff, blocking |
| `pkg/crawler/worker.go` | HTTP fetch, CAR parse, error classification, `RepoError` type |
| `pkg/crawler/cborjson.go` | CBOR-to-JSON with CID link stripping |
| `pkg/crawler/progress.go` | Redis state management (cursor, completed set, stats) |
| `pkg/crawler/metrics.go` | Prometheus counters/histograms |
| `pkg/crawler/carrepo/car.go` | CARv1 reader without SHA256 verification |
| `pkg/crawler/carrepo/repo.go` | Commit parsing, `ForEach` record iteration |
| `pkg/crawler/carrepo/mst.go` | Direct MST node walking with prefix-compressed key reconstruction |
| `pkg/repoarchive/` | `.rca` binary format: types, encoding, writer, reader, index |

## RCA Binary Format

Repo Archive (`.rca`) — a binary format for storing crawled ATProto repositories in segment files.

### File Layout

```
[FileHeader 64B] [Repo Block]... [Index Section] [FileFooter 32B]
```

### FileHeader (64 bytes)

| Offset | Size | Type | Field | Notes |
|--------|------|------|-------|-------|
| 0 | 8 | `[8]byte` | Magic | `REPOARCH` |
| 8 | 2 | `uint16` | Version | `1` |
| 10 | 2 | `uint16` | Flags | Reserved (0) |
| 12 | 8 | `int64` | CreatedAt | Unix microseconds |
| 20 | 4 | `uint32` | RepoCount | Set at finalize |
| 24 | 8 | `int64` | IndexOffset | Byte offset of index section, set at finalize |
| 32 | 32 | `[32]byte` | Reserved | Zero-filled padding |

### Repo Block

Each repo block is self-contained and skippable via `TotalBlockSize`:

```
[TotalBlockSize uint32]
[DID            uint16-prefixed string]
[PDS            uint16-prefixed string]
[Rev            uint16-prefixed string]
[CrawledAt      int64 (unix microseconds)]
[CollectionCount uint16]
[CRC32          uint32 (of TOC + data blocks)]
[Collection TOC entries...]
[Compressed data blocks...]
```

**Collection TOC Entry:**
```
[Name           uint16-prefixed string]
[RecordCount    uint32]
[CompressedSize uint32]
```

**Compressed Data Block** (one per collection, zstd-compressed):

Inside each decompressed block, records are sequential:
```
[RKey  uint16-prefixed string]
[JSON  uint32-prefixed bytes]
... (repeated RecordCount times)
```

The CRC32 covers everything after the CRC32 field (all TOC entries + all compressed data blocks).

### Index Section

Sorted by DID for binary search. Written after all repo blocks, before the footer.

```
[EntryCount  uint32]
[IndexEntry]...
```

**IndexEntry:**
```
[DID    uint16-prefixed string]
[Offset int64  (byte offset of repo block)]
[Size   uint32 (byte size of repo block)]
[CRC32  uint32 (CRC32 of entire repo block bytes)]
```

### FileFooter (32 bytes)

| Offset | Size | Type | Field | Notes |
|--------|------|------|-------|-------|
| 0 | 8 | `int64` | IndexOffset | Byte offset of index section |
| 8 | 4 | `uint32` | IndexSize | Byte size of index section |
| 12 | 4 | `uint32` | RepoCount | Total repos in segment |
| 16 | 4 | `uint32` | CRC32 | CRC32 of index section bytes |
| 20 | 4 | `[4]byte` | Reserved | Padding |
| 24 | 8 | `[8]byte` | Magic | `REPOARCH` |

### Encoding Rules

- All multi-byte integers: **little-endian**
- Checksums: **CRC32-IEEE** (`crc32.ChecksumIEEE`)
- Compression: **zstd** (default level 3, configurable 1-19)
- Length-prefixed strings: `uint16` prefix for short strings (DID, PDS, Rev, rkey, collection name), `uint32` prefix for JSON record data
- Collections sorted alphabetically within each repo block

## Redis State

All keys prefixed with `crawler:`:

| Key | Type | Purpose |
|-----|------|---------|
| `crawler:cursor` | string | Last DID seen for cursor-based pagination resume |
| `crawler:completed` | set | DIDs successfully crawled or permanently failed |
| `crawler:segment_num` | string | Current segment number |
| `crawler:stats` | hash | `total_crawled`, `bytes_written` |
| `crawler:failed:{category}` | set | DIDs per failure category (not_found, deactivated, takendown, rate_limited, parse_error, http_error, dns_error, unavailable, connection_error, timeout, unknown) |

Permanent failures (not_found, deactivated, takendown, parse_error) are also added to the `completed` set so `FilterPage` only needs one SISMEMBER check per DID.

## Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `crawler_repos_processed_total` | counter (label: `result`) | Repos by result category |
| `crawler_repo_duration_seconds` | histogram | Fetch + parse time per repo |
| `crawler_repo_records` | histogram | Records per repo |
| `crawler_repo_collections` | histogram | Collections per repo |
| `crawler_repo_compressed_bytes` | histogram | Compressed bytes per repo |
| `crawler_http_requests_total` | counter (label: `status_code`) | HTTP requests to PDSs |
| `crawler_http_request_duration_seconds` | histogram | HTTP latency |
| `crawler_segments_finalized_total` | counter | Completed segment files |
| `crawler_bytes_written_total` | counter | Total bytes to disk |
| `crawler_dids_processed_total` | counter | Running DID count |
| `crawler_pages_fetched_total` | counter | ClickHouse pages fetched |
| `crawler_page_dids` | histogram | DIDs per page (post-filter) |
| `crawler_pds_count` | gauge | Distinct PDSs tracked |
| `crawler_dispatched_total` | counter | DIDs dispatched to workers |
| `crawler_dispatch_remaining` | gauge | Undispatched DIDs |
| `crawler_pds_blocked_total` | counter | PDSs blocked |
| `crawler_active_pds` | gauge | PDSs with remaining work |
