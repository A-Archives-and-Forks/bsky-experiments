# ATProto Package

Go-based ATProto (Bluesky) ecosystem for firehose indexing, feed generation, and search.

## Major Projects

### Indexer (`cmd/indexer`, `pkg/indexer`)
Subscribes to Jetstream WebSocket, consumes firehose events, and indexes Bluesky records into ClickHouse. Tracks progress in Redis with cursor recovery for restarts.

### Feed Generator (`cmd/feedgen`, `pkg/feed-generator`)
HTTP service providing Bluesky-compatible feed endpoints. Implements trending/hot feeds, label-based feeds (private communities), and static feeds. JWT and API key authentication.

### Search Service (`cmd/search`, `pkg/search`)
HTTP service for search, statistics, and repository maintenance. Provides site-wide analytics, stats caching, and cleanup operations.

## Development Workflow

```bash
# Start services (uses sops for secret decryption)
just common        # Start Redis
just indexer       # Start indexer
just feedgen       # Start feed generator
just search        # Start search service

# Stop services
just indexer-down
just feedgen-down
just search-down
just common-down
```

### Building

All services use multi-stage Docker builds. Binaries compile with `CGO_ENABLED=0 GOOS=linux`.

```bash
# Build locally
go build -o indexer ./cmd/indexer
go build -o feedgen ./cmd/feedgen
go build -o search ./cmd/search

# Run tests
go test ./...
```

### Service Ports
- Indexer: 8091 (metrics only)
- Feedgen: 8094 (app), 8095 (metrics)
- Search: 8092 (app), 8093 (metrics)
- Redis: 6379

## Data Schemas

ClickHouse schema is managed via migrations in [pkg/migrate/migrations/](pkg/migrate/migrations/).
Full schema reference: [../../.claude/skills/clickhouse-analyst/references/databases/default/_full_schema.sql](../../.claude/skills/clickhouse-analyst/references/databases/default/_full_schema.sql)

```bash
# Migration commands
just migrate-up           # Apply pending migrations
just migrate-down         # Rollback last migration
just migrate-version      # Show current version
just migrate-create foo   # Create new migration files
just migrate-dump-schema  # Regenerate full schema reference
```

### Core Tables
| Table | Purpose |
|-------|---------|
| `repo_records` | Raw firehose events (repo, collection, rkey, operation, record_json) |
| `posts` | Post content (did, uri, text, langs, parent_uri, root_uri) |
| `follows` | Social graph follow relationships |
| `likes` | Post engagement |
| `reposts` | Post sharing |
| `blocks` | User blocking relationships |
| `actor_labels` | Access control labels for private feeds |
| `daily_stats` | Aggregated daily statistics |
| `api_keys` | API authentication keys |
| `repo_cleanup_jobs` | Maintenance job tracking |

### Views
- `recent_posts` - Posts from last 72 hours
- `recent_posts_with_score` - Trending posts with engagement scoring
- `following_counts` - User following statistics
- `daily_stats_*` - Materialized views for daily aggregations

## Key Packages

| Package | Location | Purpose |
|---------|----------|---------|
| Store | [pkg/indexer/store/](pkg/indexer/store/) | ClickHouse operations (batch inserts, feeds, labels, auth, stats) |
| Feeds | [pkg/feeds/](pkg/feeds/) | Feed implementations (hot, authorlabel, static) |
| Auth | [pkg/auth/](pkg/auth/) | JWT + API key authentication |
| Endpoints | [pkg/feed-generator/endpoints/](pkg/feed-generator/endpoints/) | Feed REST endpoints |
| Search Endpoints | [pkg/search/endpoints/](pkg/search/endpoints/) | Search/stats REST endpoints |

## Feed Implementations

Located in [pkg/feeds/](pkg/feeds/):

- **Hot** (`hot/feed.go`): "whats-hot", "top-1h", "top-24h" - trending algorithm with Redis caching
- **Author Label** (`authorlabel/feed.go`): "a-mpls", "cl-tqsp" - private/public community feeds based on actor labels
- **Static** (`static/feed.go`): "bangers", "at-bangers", etc. - pinned posts

All feeds implement the interface in [pkg/feed-generator/feed.go](pkg/feed-generator/feed.go).

## Configuration

Environment files in `env/` (encrypted with sops):
- `indexer.env` / `indexer.enc.env`
- `feedgen.env` / `feedgen.enc.env`
- `search.env` / `search.enc.env`

Key environment variables:
- `WS_URL` - Jetstream WebSocket endpoint
- `REDIS_ADDRESS` - Redis connection
- `CLICKHOUSE_ADDRESS`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD` - ClickHouse connection
- `OTEL_EXPORTER_OTLP_ENDPOINT` - OpenTelemetry endpoint

## Dependencies

- ATProto: `github.com/bluesky-social/indigo`, `github.com/bluesky-social/jetstream`
- Database: `github.com/ClickHouse/clickhouse-go/v2`, `github.com/redis/go-redis/v9`
- Web: `github.com/labstack/echo/v4`, `github.com/samber/slog-echo`
- Telemetry: `go.opentelemetry.io/otel`, local `packages/telemetry`

## Patterns

- **Soft deletes**: ReplacingMergeTree with `deleted UInt8 DEFAULT 0`, queries filter on `deleted = 0`
- **Time versioning**: `time_us` column for ClickHouse deduplication
- **OpenTelemetry**: Each module creates tracer via `otel.Tracer("module-name")`
- **Graceful shutdown**: Signal handling with timeout-based cleanup
