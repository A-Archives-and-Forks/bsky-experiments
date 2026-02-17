package hot

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/jazware/bsky-experiments/pkg/indexer/store"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
)

const (
	hotCacheKey = "whats-hot"
	hotCacheTTL = 1 * time.Minute
	maxPosts    = 3000
)

var topCacheKeys = map[int]string{
	1:  "top-1h",
	24: "top-24h",
}

var topCacheTTLs = map[int]time.Duration{
	1:  1 * time.Minute,
	24: 10 * time.Minute,
}

type Feed struct {
	FeedActorDID string
	Store        *store.Store
	Redis        *redis.Client
	init         bool
	initLk       sync.Mutex
}

type NotFoundError struct {
	error
}

var supportedFeeds = []string{"whats-hot", "top-1h", "top-24h"}

var tracer = otel.Tracer("hot-feed")

type postRef struct {
	ActorDid string   `json:"did"`
	Rkey     string   `json:"rkey"`
	Langs    []string `json:"langs"`
	HasMedia bool     `json:"has_media"`
}

func NewFeed(ctx context.Context, feedActorDID string, chStore *store.Store, redis *redis.Client) (*Feed, []string, error) {
	f := Feed{
		FeedActorDID: feedActorDID,
		Store:        chStore,
		Redis:        redis,
	}

	go func() {
		_, err := f.fetchAndCacheHotPosts(ctx)
		if err != nil {
			slog.Error("error fetching and caching posts for feed (whats-hot)", "error", err)
			return
		}

		for hours := range topCacheKeys {
			_, err := f.fetchAndCacheTopPosts(ctx, hours)
			if err != nil {
				slog.Error("error fetching and caching posts for feed", "top_feed", hours, "error", err)
				return
			}
		}

		go func() {
			t := time.NewTicker(hotCacheTTL)
			logger := slog.With("source", "whats-hot-refresh")
			defer t.Stop()
			for range t.C {
				ctx := context.Background()
				logger.Info("refreshing cache")
				_, err := f.fetchAndCacheHotPosts(ctx)
				if err != nil {
					logger.Error("error refreshing cache", "error", err)
				}
				logger.Info("cache refreshed")
			}
		}()

		for hours, key := range topCacheKeys {
			go func(hours int, key string) {
				t := time.NewTicker(topCacheTTLs[hours])
				logger := slog.With("source", fmt.Sprintf("top-%dh-refresh", hours))
				defer t.Stop()
				for range t.C {
					ctx := context.Background()
					logger.Info("refreshing cache")
					_, err := f.fetchAndCacheTopPosts(ctx, hours)
					if err != nil {
						logger.Error("error refreshing cache", "error", err)
					}
					logger.Info("cache refreshed")
				}
			}(hours, key)
		}

		f.setReady()
	}()

	return &f, supportedFeeds, nil
}

func (f *Feed) isReady() bool {
	f.initLk.Lock()
	defer f.initLk.Unlock()
	return f.init
}

func (f *Feed) setReady() {
	f.initLk.Lock()
	defer f.initLk.Unlock()
	f.init = true
}

func (f *Feed) fetchAndCacheHotPosts(ctx context.Context) ([]postRef, error) {
	rawPosts, err := f.Store.GetHotPosts(ctx, maxPosts, 0)
	if err != nil {
		return nil, err
	}

	p := f.Redis.Pipeline()

	p.Del(ctx, hotCacheKey)

	postRefs := []postRef{}
	for _, post := range rawPosts {
		postRef := postRef{
			ActorDid: post.ActorDID,
			Rkey:     post.Rkey,
			Langs:    post.Langs,
			HasMedia: post.HasEmbeddedMedia,
		}
		cacheValue, err := json.Marshal(postRef)
		if err != nil {
			return nil, fmt.Errorf("error marshalling post: %w", err)
		}
		p.RPush(ctx, hotCacheKey, cacheValue)
		postRefs = append(postRefs, postRef)
	}

	_, err = p.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("error caching posts for feed (whats-hot): %w", err)
	}

	return postRefs, nil
}

func (f *Feed) fetchAndCacheTopPosts(ctx context.Context, hours int) ([]postRef, error) {
	rawPosts, err := f.Store.GetTopPostsInWindow(ctx, hours, maxPosts)
	if err != nil {
		return nil, err
	}

	p := f.Redis.Pipeline()

	key, ok := topCacheKeys[hours]
	if !ok {
		return nil, fmt.Errorf("no cache key for hours: %d", hours)
	}

	p.Del(ctx, key)

	postRefs := []postRef{}
	for _, post := range rawPosts {
		postRef := postRef{
			ActorDid: post.ActorDID,
			Rkey:     post.Rkey,
			Langs:    post.Langs,
			HasMedia: post.HasEmbeddedMedia,
		}
		cacheValue, err := json.Marshal(postRef)
		if err != nil {
			return nil, fmt.Errorf("error marshalling post: %w", err)
		}
		p.RPush(ctx, key, cacheValue)
		postRefs = append(postRefs, postRef)
	}

	_, err = p.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("error caching posts for feed (top-%dh): %w", hours, err)
	}

	return postRefs, nil
}

func (f *Feed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	if !f.isReady() {
		return nil, nil, fmt.Errorf("feed starting up...")
	}

	offset := int64(0)
	var err error

	if cursor != "" {
		offset, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing cursor: %w", err)
		}
	}

	key := hotCacheKey
	if after, ok := strings.CutPrefix(feed, "top-"); ok {
		hours, err := strconv.Atoi(strings.TrimSuffix(after, "h"))
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing hours from feed: %w", err)
		}
		var ok bool
		key, ok = topCacheKeys[hours]
		if !ok {
			return nil, nil, fmt.Errorf("no cache key for hours: %d", hours)
		}
	}

	cached, err := f.Redis.LRange(ctx, key, offset, offset+(limit*10)).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("error getting posts from cache for feed (%s): %w", feed, err)
	}

	posts := make([]postRef, len(cached))
	for i, cachedValue := range cached {
		json.Unmarshal([]byte(cachedValue), &posts[i])
	}

	postsSeen := int64(len(posts))
	lastPostAdded := int(offset) + len(posts) - 1

	feedPosts := make([]*appbsky.FeedDefs_SkeletonFeedPost, len(posts))
	for i, post := range posts {
		feedPosts[i] = &appbsky.FeedDefs_SkeletonFeedPost{
			Post: fmt.Sprintf("at://%s/app.bsky.feed.post/%s", post.ActorDid, post.Rkey),
		}
	}

	if postsSeen < limit {
		return feedPosts, nil, nil
	}

	newCursor := strconv.FormatInt(int64(lastPostAdded)+1, 10)
	return feedPosts, &newCursor, nil
}

func (f *Feed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{}

	for _, feed := range supportedFeeds {
		feeds = append(feeds, appbsky.FeedDescribeFeedGenerator_Feed{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + feed,
		})
	}

	return feeds, nil
}
