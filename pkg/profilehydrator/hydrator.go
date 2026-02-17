package profilehydrator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"golang.org/x/time/rate"
)

var tracer = otel.Tracer("profile-hydrator")

const (
	DefaultBatchSize    = 25 // Max DIDs per API request
	DefaultRateLimit    = 5.0
	MaxRetries          = 5
	InitialBackoff      = 1 * time.Second
	MaxBackoff          = 60 * time.Second
	DefaultDIDQuerySize = 5_000 // DIDs to fetch from DB per query
)

// cdnCIDRegex extracts CID from CDN URLs like:
// https://cdn.bsky.app/img/avatar/plain/did:plc:xxx/bafkreiabc@jpeg
var cdnCIDRegex = regexp.MustCompile(`/plain/[^/]+/([^@]+)@`)

// HydratorConfig contains configuration for the profile hydrator
type HydratorConfig struct {
	APIHost     string
	RateLimit   float64
	BatchSize   int
	RedisClient *redis.Client
	RedisPrefix string
	DB          driver.Conn
	Logger      *slog.Logger
}

// Hydrator handles fetching profiles from the Bluesky API and storing them
type Hydrator struct {
	config        HydratorConfig
	httpClient    *http.Client
	rateLimiter   *rate.Limiter
	batchInserter *BatchInserter

	repoRecordsCursorKey   string
	plcOperationsCursorKey string

	repoRecordsCursor   int64
	plcOperationsCursor int64
	cursorMu            sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// APIProfile represents a profile from the Bluesky API response
type APIProfile struct {
	DID            string     `json:"did"`
	Handle         string     `json:"handle"`
	DisplayName    string     `json:"displayName"`
	Description    string     `json:"description"`
	Avatar         string     `json:"avatar"`
	Banner         string     `json:"banner"`
	FollowersCount uint64     `json:"followersCount"`
	FollowsCount   uint64     `json:"followsCount"`
	PostsCount     uint64     `json:"postsCount"`
	Labels         []APILabel `json:"labels"`
	CreatedAt      string     `json:"createdAt"`
}

// APILabel represents a label in the API response
type APILabel struct {
	Val string `json:"val"`
}

// GetProfilesResponse represents the API response
type GetProfilesResponse struct {
	Profiles []APIProfile `json:"profiles"`
}

// DIDWithTimeUS represents a DID with its max time_us for cursor tracking
type DIDWithTimeUS struct {
	DID    string
	TimeUS int64
}

// NewHydrator creates a new profile hydrator
func NewHydrator(config HydratorConfig) (*Hydrator, error) {
	if config.RateLimit <= 0 {
		config.RateLimit = DefaultRateLimit
	}
	if config.BatchSize <= 0 || config.BatchSize > 25 {
		config.BatchSize = DefaultBatchSize
	}

	ctx, cancel := context.WithCancel(context.Background())

	h := &Hydrator{
		config: config,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		rateLimiter:            rate.NewLimiter(rate.Limit(config.RateLimit), 1),
		batchInserter:          NewBatchInserter(config.DB, config.Logger),
		repoRecordsCursorKey:   fmt.Sprintf("%s:profile_hydrator_repo_cursor", config.RedisPrefix),
		plcOperationsCursorKey: fmt.Sprintf("%s:profile_hydrator_plc_cursor", config.RedisPrefix),
		ctx:                    ctx,
		cancel:                 cancel,
	}

	// Load cursors from Redis
	if err := h.loadCursors(ctx); err != nil {
		config.Logger.Warn("failed to load profile hydrator cursors from Redis, starting from beginning", "error", err)
	}

	return h, nil
}

// Run starts the hydrator main loop
func (h *Hydrator) Run() {
	h.wg.Add(1)
	defer h.wg.Done()

	logger := h.config.Logger.With("component", "profile_hydrator")
	logger.Info("starting profile hydrator",
		"api_host", h.config.APIHost,
		"rate_limit", h.config.RateLimit,
		"batch_size", h.config.BatchSize,
	)

	backoff := InitialBackoff
	consecutiveErrors := 0

	for {
		select {
		case <-h.ctx.Done():
			logger.Info("profile hydrator shutting down")
			return
		default:
		}

		// Fetch and process a batch of DIDs
		count, err := h.processBatch(h.ctx)
		if err != nil {
			consecutiveErrors++
			fetchErrorsCounter.Inc()
			logger.Error("failed to process profile batch", "error", err, "consecutive_errors", consecutiveErrors)

			// Exponential backoff
			if consecutiveErrors >= MaxRetries {
				logger.Error("max retries exceeded, backing off", "backoff", backoff)
				select {
				case <-h.ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, MaxBackoff)
			}
			continue
		}

		// Reset backoff on success
		consecutiveErrors = 0
		backoff = InitialBackoff

		// If we got no DIDs, we're caught up - wait before polling again
		if count == 0 {
			logger.Debug("caught up with DIDs, waiting before next poll")
			select {
			case <-h.ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
		} else {
			logger.Info("processed profile batch",
				"count", count,
				"repo_cursor", h.getRepoRecordsCursor(),
				"plc_cursor", h.getPLCOperationsCursor(),
			)
		}
	}
}

// processBatch fetches DIDs from both sources, dedupes, and hydrates profiles
func (h *Hydrator) processBatch(ctx context.Context) (int, error) {
	ctx, span := tracer.Start(ctx, "processBatch")
	defer span.End()

	// Fetch DIDs from both sources
	repoDIDs, err := h.fetchDIDsFromRepoRecords(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch DIDs from repo_records: %w", err)
	}

	plcDIDs, err := h.fetchDIDsFromPLCOperations(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch DIDs from plc_operations: %w", err)
	}

	// Merge and dedupe DIDs
	didMap := make(map[string]int64)
	var maxRepoTimeUS, maxPLCTimeUS int64

	for _, d := range repoDIDs {
		didMap[d.DID] = d.TimeUS
		if d.TimeUS > maxRepoTimeUS {
			maxRepoTimeUS = d.TimeUS
		}
	}

	for _, d := range plcDIDs {
		if _, exists := didMap[d.DID]; !exists {
			didMap[d.DID] = d.TimeUS
		}
		if d.TimeUS > maxPLCTimeUS {
			maxPLCTimeUS = d.TimeUS
		}
	}

	if len(didMap) == 0 {
		return 0, nil
	}

	// Convert map to slice
	dids := make([]string, 0, len(didMap))
	for did := range didMap {
		dids = append(dids, did)
	}

	didsQueuedCounter.Add(float64(len(dids)))

	// Fetch profiles in batches of BatchSize
	totalFetched := 0
	for i := 0; i < len(dids); i += h.config.BatchSize {
		end := min(i+h.config.BatchSize, len(dids))
		batch := dids[i:end]

		// Rate limit
		if err := h.rateLimiter.Wait(ctx); err != nil {
			if ctx.Err() != nil {
				return totalFetched, nil
			}
			rateLimitHitsCounter.Inc()
			continue
		}

		profiles, err := h.fetchProfiles(ctx, batch)
		if err != nil {
			h.config.Logger.Warn("failed to fetch profiles batch", "error", err, "batch_size", len(batch))
			continue
		}

		// Add profiles to batch inserter
		for _, profile := range profiles {
			h.batchInserter.Add(profile)
		}
		totalFetched += len(profiles)
	}

	// Update cursors
	if maxRepoTimeUS > 0 {
		h.setRepoRecordsCursor(maxRepoTimeUS)
		repoRecordsCursorGauge.Set(float64(maxRepoTimeUS))
	}
	if maxPLCTimeUS > 0 {
		h.setPLCOperationsCursor(maxPLCTimeUS)
		plcOperationsCursorGauge.Set(float64(maxPLCTimeUS))
	}

	// Save cursors to Redis
	if err := h.saveCursors(ctx); err != nil {
		h.config.Logger.Error("failed to save profile hydrator cursors to Redis", "error", err)
	}

	return totalFetched, nil
}

// fetchDIDsFromRepoRecords fetches DIDs from repo_records that don't have profiles yet
func (h *Hydrator) fetchDIDsFromRepoRecords(ctx context.Context) ([]DIDWithTimeUS, error) {
	cursor := h.getRepoRecordsCursor()

	query := `
		SELECT DISTINCT repo AS did, max(time_us) AS max_time_us
		FROM repo_records
		WHERE time_us > $1
		GROUP BY repo
		HAVING did NOT IN (SELECT did FROM profiles FINAL WHERE deleted = 0)
		ORDER BY max_time_us
		LIMIT $2
	`

	rows, err := h.config.DB.Query(ctx, query, cursor, DefaultDIDQuerySize)
	if err != nil {
		return nil, fmt.Errorf("failed to query repo_records: %w", err)
	}
	defer rows.Close()

	var result []DIDWithTimeUS
	for rows.Next() {
		var d DIDWithTimeUS
		if err := rows.Scan(&d.DID, &d.TimeUS); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		result = append(result, d)
	}

	return result, nil
}

// fetchDIDsFromPLCOperations fetches DIDs from plc_operations that don't have profiles yet
func (h *Hydrator) fetchDIDsFromPLCOperations(ctx context.Context) ([]DIDWithTimeUS, error) {
	cursor := h.getPLCOperationsCursor()

	query := `
		SELECT DISTINCT did, max(time_us) AS max_time_us
		FROM plc_operations
		WHERE time_us > $1
		GROUP BY did
		HAVING did NOT IN (SELECT did FROM profiles FINAL WHERE deleted = 0)
		ORDER BY max_time_us
		LIMIT $2
	`

	rows, err := h.config.DB.Query(ctx, query, cursor, DefaultDIDQuerySize)
	if err != nil {
		return nil, fmt.Errorf("failed to query plc_operations: %w", err)
	}
	defer rows.Close()

	var result []DIDWithTimeUS
	for rows.Next() {
		var d DIDWithTimeUS
		if err := rows.Scan(&d.DID, &d.TimeUS); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		result = append(result, d)
	}

	return result, nil
}

// fetchProfiles fetches profiles from the Bluesky API
func (h *Hydrator) fetchProfiles(ctx context.Context, dids []string) ([]*ProfileBatch, error) {
	ctx, span := tracer.Start(ctx, "fetchProfiles")
	defer span.End()

	if len(dids) == 0 {
		return nil, nil
	}

	// Build query parameters
	params := make([]string, len(dids))
	for i, did := range dids {
		params[i] = "actors=" + did
	}
	url := fmt.Sprintf("%s/xrpc/app.bsky.actor.getProfiles?%s", h.config.APIHost, strings.Join(params, "&"))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", "jaz-profile-hydrator/1.0")
	req.Header.Set("Accept", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		rateLimitHitsCounter.Inc()
		return nil, fmt.Errorf("rate limited by API server")
	}

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if readErr != nil {
			return nil, fmt.Errorf("unexpected status %d (failed to read body: %v)", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var apiResp GetProfilesResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Convert API profiles to batch profiles
	now := time.Now()
	result := make([]*ProfileBatch, 0, len(apiResp.Profiles))

	for _, p := range apiResp.Profiles {
		createdAt, _ := time.Parse(time.RFC3339, p.CreatedAt)
		if createdAt.IsZero() {
			createdAt = now
		}

		// Marshal the individual profile to JSON for storage
		profileJSON, err := json.Marshal(p)
		if err != nil {
			h.config.Logger.Warn("failed to marshal profile JSON", "did", p.DID, "error", err)
			profileJSON = []byte("{}")
		}

		profile := &ProfileBatch{
			DID:         p.DID,
			Handle:      p.Handle,
			DisplayName: p.DisplayName,
			Description: p.Description,
			AvatarCID:   extractCIDFromURL(p.Avatar),
			BannerCID:   extractCIDFromURL(p.Banner),
			CreatedAt:   createdAt,
			ProfileJSON: string(profileJSON),
			TimeUS:      now.UnixMicro(),
		}
		result = append(result, profile)
		profilesFetchedCounter.WithLabelValues("success").Inc()
	}

	// Count DIDs that weren't found
	foundDIDs := make(map[string]bool)
	for _, p := range apiResp.Profiles {
		foundDIDs[p.DID] = true
	}
	for _, did := range dids {
		if !foundDIDs[did] {
			profilesFetchedCounter.WithLabelValues("not_found").Inc()
		}
	}

	return result, nil
}

// extractCIDFromURL extracts the CID from a CDN URL
// Example: https://cdn.bsky.app/img/avatar/plain/did:plc:xxx/bafkreiabc@jpeg -> bafkreiabc
func extractCIDFromURL(url string) string {
	if url == "" {
		return ""
	}
	matches := cdnCIDRegex.FindStringSubmatch(url)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

// Cursor getters/setters

func (h *Hydrator) getRepoRecordsCursor() int64 {
	h.cursorMu.RLock()
	defer h.cursorMu.RUnlock()
	return h.repoRecordsCursor
}

func (h *Hydrator) setRepoRecordsCursor(cursor int64) {
	h.cursorMu.Lock()
	defer h.cursorMu.Unlock()
	h.repoRecordsCursor = cursor
}

func (h *Hydrator) getPLCOperationsCursor() int64 {
	h.cursorMu.RLock()
	defer h.cursorMu.RUnlock()
	return h.plcOperationsCursor
}

func (h *Hydrator) setPLCOperationsCursor(cursor int64) {
	h.cursorMu.Lock()
	defer h.cursorMu.Unlock()
	h.plcOperationsCursor = cursor
}

// loadCursors loads both cursors from Redis
func (h *Hydrator) loadCursors(ctx context.Context) error {
	// Load repo_records cursor
	repoCursor, err := h.config.RedisClient.Get(ctx, h.repoRecordsCursorKey).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to get repo cursor from Redis: %w", err)
	}
	if repoCursor != "" {
		if val, err := strconv.ParseInt(repoCursor, 10, 64); err == nil {
			h.setRepoRecordsCursor(val)
			h.config.Logger.Info("loaded repo_records cursor from Redis", "cursor", val)
		}
	}

	// Load plc_operations cursor
	plcCursor, err := h.config.RedisClient.Get(ctx, h.plcOperationsCursorKey).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to get plc cursor from Redis: %w", err)
	}
	if plcCursor != "" {
		if val, err := strconv.ParseInt(plcCursor, 10, 64); err == nil {
			h.setPLCOperationsCursor(val)
			h.config.Logger.Info("loaded plc_operations cursor from Redis", "cursor", val)
		}
	}

	return nil
}

// saveCursors saves both cursors to Redis
func (h *Hydrator) saveCursors(ctx context.Context) error {
	repoCursor := h.getRepoRecordsCursor()
	if repoCursor > 0 {
		if err := h.config.RedisClient.Set(ctx, h.repoRecordsCursorKey, strconv.FormatInt(repoCursor, 10), 0).Err(); err != nil {
			return fmt.Errorf("failed to save repo cursor: %w", err)
		}
	}

	plcCursor := h.getPLCOperationsCursor()
	if plcCursor > 0 {
		if err := h.config.RedisClient.Set(ctx, h.plcOperationsCursorKey, strconv.FormatInt(plcCursor, 10), 0).Err(); err != nil {
			return fmt.Errorf("failed to save plc cursor: %w", err)
		}
	}

	return nil
}

// Shutdown gracefully shuts down the hydrator
func (h *Hydrator) Shutdown() {
	h.config.Logger.Info("shutting down profile hydrator")

	// Cancel context to stop main loop
	h.cancel()

	// Wait for main loop to finish
	h.wg.Wait()

	// Shutdown batch inserter (flushes remaining batches)
	h.batchInserter.Shutdown()

	// Save final cursors
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.saveCursors(ctx); err != nil {
		h.config.Logger.Error("failed to save final profile hydrator cursors", "error", err)
	}

	h.config.Logger.Info("profile hydrator shut down successfully")
}
