package crawler

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const (
	keyPrefix     = "crawler"
	keyCompleted  = keyPrefix + ":completed"
	keyCursor     = keyPrefix + ":cursor"
	keySegmentNum = keyPrefix + ":segment_num"
	keyStats      = keyPrefix + ":stats"
)

func failedKey(category string) string {
	return fmt.Sprintf("%s:failed:%s", keyPrefix, category)
}

// Progress tracks crawl progress in Redis.
type Progress struct {
	redis  *redis.Client
	logger *slog.Logger
}

// NewProgress creates a new Redis-backed progress tracker.
func NewProgress(redisClient *redis.Client, logger *slog.Logger) *Progress {
	return &Progress{
		redis:  redisClient,
		logger: logger.With("component", "progress"),
	}
}

// MarkCompleted adds a DID to the completed set.
func (p *Progress) MarkCompleted(ctx context.Context, did string) {
	if err := p.redis.SAdd(ctx, keyCompleted, did).Err(); err != nil {
		p.logger.Error("failed to mark completed", "did", did, "error", err)
	}
	didsProcessedTotal.Inc()
}

// MarkFailed adds a DID to the appropriate failed set.
// Permanent failures (not_found, deactivated, takendown, parse_error) are also
// added to the completed set so FilterPage only needs to check one set.
func (p *Progress) MarkFailed(ctx context.Context, did string, category string) {
	pipe := p.redis.Pipeline()
	pipe.SAdd(ctx, failedKey(category), did)

	switch category {
	case "not_found", "deactivated", "takendown", "parse_error":
		pipe.SAdd(ctx, keyCompleted, did)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		p.logger.Error("failed to mark failed", "did", did, "category", category, "error", err)
	}
	didsProcessedTotal.Inc()
}

// FilterPage removes already-completed DIDs from the pdsGroups map in place
// using pipelined SISMEMBER for efficiency. Returns the count of DIDs filtered out.
func (p *Progress) FilterPage(ctx context.Context, pdsGroups map[string][]string) (int, error) {
	// Collect all DIDs from the page.
	type didRef struct {
		pds string
		idx int
	}
	var allDIDs []didRef
	for pds, dids := range pdsGroups {
		for i := range dids {
			allDIDs = append(allDIDs, didRef{pds: pds, idx: i})
		}
	}
	if len(allDIDs) == 0 {
		return 0, nil
	}

	// Pipeline SISMEMBER for each DID against the completed set.
	pipe := p.redis.Pipeline()
	cmds := make([]*redis.BoolCmd, len(allDIDs))
	for i, ref := range allDIDs {
		cmds[i] = pipe.SIsMember(ctx, keyCompleted, pdsGroups[ref.pds][ref.idx])
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, fmt.Errorf("pipelined SISMEMBER: %w", err)
	}

	// Build set of completed DIDs from results.
	completedSet := make(map[string]bool)
	for i, cmd := range cmds {
		if cmd.Val() {
			ref := allDIDs[i]
			completedSet[pdsGroups[ref.pds][ref.idx]] = true
		}
	}
	if len(completedSet) == 0 {
		return 0, nil
	}

	// Filter completed DIDs from pdsGroups in place.
	filtered := 0
	for pds, dids := range pdsGroups {
		var remaining []string
		for _, did := range dids {
			if completedSet[did] {
				filtered++
			} else {
				remaining = append(remaining, did)
			}
		}
		if len(remaining) == 0 {
			delete(pdsGroups, pds)
		} else {
			pdsGroups[pds] = remaining
		}
	}

	return filtered, nil
}

// GetCursor returns the pagination cursor from Redis. Returns "" if not set.
func (p *Progress) GetCursor(ctx context.Context) (string, error) {
	val, err := p.redis.Get(ctx, keyCursor).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", err
	}
	return val, nil
}

// SetCursor persists the pagination cursor to Redis.
func (p *Progress) SetCursor(ctx context.Context, cursor string) {
	if err := p.redis.Set(ctx, keyCursor, cursor, 0).Err(); err != nil {
		p.logger.Error("failed to set cursor", "error", err)
	}
}

// ClearCursor removes the pagination cursor from Redis on successful completion.
func (p *Progress) ClearCursor(ctx context.Context) {
	if err := p.redis.Del(ctx, keyCursor).Err(); err != nil {
		p.logger.Error("failed to clear cursor", "error", err)
	}
}

// IsCompleted checks if a DID has already been crawled.
func (p *Progress) IsCompleted(ctx context.Context, did string) bool {
	ok, err := p.redis.SIsMember(ctx, keyCompleted, did).Result()
	if err != nil {
		return false
	}
	return ok
}

// GetSegmentNum returns the current segment number from Redis.
func (p *Progress) GetSegmentNum(ctx context.Context) (int, error) {
	val, err := p.redis.Get(ctx, keySegmentNum).Result()
	if err != nil {
		if err == redis.Nil {
			return 1, nil
		}
		return 1, err
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 1, err
	}
	return n, nil
}

// SetSegmentNum stores the current segment number in Redis.
func (p *Progress) SetSegmentNum(ctx context.Context, n int) {
	if err := p.redis.Set(ctx, keySegmentNum, strconv.Itoa(n), 0).Err(); err != nil {
		p.logger.Error("failed to set segment num", "error", err)
	}
}

// SaveStats saves crawl statistics to Redis.
func (p *Progress) SaveStats(ctx context.Context, totalCrawled uint32, bytesWritten int64) {
	pipe := p.redis.Pipeline()
	pipe.HSet(ctx, keyStats, "total_crawled", strconv.FormatUint(uint64(totalCrawled), 10))
	pipe.HSet(ctx, keyStats, "bytes_written", strconv.FormatInt(bytesWritten, 10))
	if _, err := pipe.Exec(ctx); err != nil {
		p.logger.Error("failed to save stats", "error", err)
	}
}

// GetStats returns current crawl statistics.
func (p *Progress) GetStats(ctx context.Context) (map[string]string, error) {
	result, err := p.redis.HGetAll(ctx, keyStats).Result()
	if err != nil {
		return nil, err
	}

	// Add completed/failed counts.
	completedCount, _ := p.redis.SCard(ctx, keyCompleted).Result()
	result["completed_count"] = strconv.FormatInt(completedCount, 10)

	for _, category := range []string{"not_found", "deactivated", "takendown", "rate_limited", "parse_error", "http_error", "dns_error", "unavailable", "connection_error", "timeout", "unknown"} {
		count, _ := p.redis.SCard(ctx, failedKey(category)).Result()
		if count > 0 {
			result["failed_"+category] = strconv.FormatInt(count, 10)
		}
	}

	segNum, _ := p.redis.Get(ctx, keySegmentNum).Result()
	if segNum != "" {
		result["segment_num"] = segNum
	}

	cursor, _ := p.redis.Get(ctx, keyCursor).Result()
	if cursor != "" {
		result["cursor"] = cursor
	}

	return result, nil
}

// CompletedCount returns the number of completed DIDs.
func (p *Progress) CompletedCount(ctx context.Context) (int64, error) {
	return p.redis.SCard(ctx, keyCompleted).Result()
}

// ClearAll deletes all crawler keys from Redis (progress, cursor, stats, completed, failed).
func (p *Progress) ClearAll(ctx context.Context) (int64, error) {
	keys, err := p.redis.Keys(ctx, keyPrefix+":*").Result()
	if err != nil {
		return 0, fmt.Errorf("listing crawler keys: %w", err)
	}
	if len(keys) == 0 {
		return 0, nil
	}
	deleted, err := p.redis.Del(ctx, keys...).Result()
	if err != nil {
		return 0, fmt.Errorf("deleting crawler keys: %w", err)
	}
	return deleted, nil
}
