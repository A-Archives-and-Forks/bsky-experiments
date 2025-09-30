package static

import (
	"context"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/jazware/bsky-experiments/pkg/consumer/store"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
)

type Feed struct {
	FeedActorDID string
	RKeys        []string
}

type NotFoundError struct {
	error
}

func NewFeed(ctx context.Context, feedActorDID string, store *store.Store, redis *redis.Client) (*Feed, []string, error) {
	keys := []string{
		"bangers",
		"at-bangers",
		"cl-tqsp",
		"my-followers-ex",
		"my-followers",
		"cv:cat",
		"cv:dog",
		"cv:bird",
		"cl-japanese",
		"cl-brasil",
		"whats-hot",
		"wh-ja",
		"wh-ja-txt",
		"top-1h",
		"top-24h",
		"firehose",
	}
	return &Feed{
		FeedActorDID: feedActorDID,
		RKeys:        keys,
	}, keys, nil
}

var tracer = otel.Tracer("static-feed")

var pinnedPost = "at://did:plc:q6gjnaw2blty4crticxkmujt/app.bsky.feed.post/3lzrksmt6jk2b"

func (f *Feed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	ctx, span := tracer.Start(ctx, "GetPage")
	defer span.End()

	feedPosts := []*appbsky.FeedDefs_SkeletonFeedPost{
		{
			Post: pinnedPost,
			Reason: &appbsky.FeedDefs_SkeletonFeedPost_Reason{
				FeedDefs_SkeletonReasonPin: &appbsky.FeedDefs_SkeletonReasonPin{},
			},
		},
	}
	return feedPosts, nil, nil
}

func (f *Feed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	ctx, span := tracer.Start(ctx, "Describe")
	defer span.End()

	feeds := []appbsky.FeedDescribeFeedGenerator_Feed{
		{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + "bangers",
		},
		{
			Uri: "at://" + f.FeedActorDID + "/app.bsky.feed.generator/" + "at-bangers",
		},
	}

	return feeds, nil
}
