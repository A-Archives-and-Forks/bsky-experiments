package endpoints

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type StatsCacheEntry struct {
	Stats      AuthorStatsResponse
	Expiration time.Time
}

type DailyDatapoint struct {
	Date                    string `json:"date"`
	LikesPerDay             int64  `json:"num_likes"`
	DailyActiveLikers       int64  `json:"num_likers"`
	DailyActivePosters      int64  `json:"num_posters"`
	PostsPerDay             int64  `json:"num_posts"`
	PostsWithImagesPerDay   int64  `json:"num_posts_with_images"`
	ImagesPerDay            int64  `json:"num_images"`
	ImagesWithAltTextPerDay int64  `json:"num_images_with_alt_text"`
	FirstTimePosters        int64  `json:"num_first_time_posters"`
	FollowsPerDay           int64  `json:"num_follows"`
	DailyActiveFollowers    int64  `json:"num_followers"`
	BlocksPerDay            int64  `json:"num_blocks"`
	DailyActiveBlockers     int64  `json:"num_blockers"`
}

type StatPercentile struct {
	Percentile float64 `json:"percentile"`
	Value      float64 `json:"value"`
}

type AuthorStatsResponse struct {
	TotalUsers   int              `json:"total_users"`
	TotalPosts   int64            `json:"total_posts"`
	TotalFollows int64            `json:"total_follows"`
	TotalLikes   int64            `json:"total_likes"`
	UpdatedAt    time.Time        `json:"updated_at"`
	DailyData    []DailyDatapoint `json:"daily_data"`
}

func (api *API) GetStats(c *gin.Context) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "GetStats")
	defer span.End()

	timeout := 30 * time.Second
	timeWaited := 0 * time.Second
	sleepTime := 100 * time.Millisecond

	// Wait for the stats cache to be populated
	if api.StatsCache == nil {
		span.AddEvent("GetStats:WaitForStatsCache")
		for api.StatsCache == nil {
			if timeWaited > timeout {
				c.JSON(http.StatusRequestTimeout, gin.H{"error": "timed out waiting for stats cache to populate"})
				return
			}

			time.Sleep(sleepTime)
			timeWaited += sleepTime
		}
	}

	// Lock the stats mux for reading
	api.StatsCacheRWMux.RLock()
	statsFromCache := api.StatsCache.Stats
	api.StatsCacheRWMux.RUnlock()

	c.JSON(http.StatusOK, statsFromCache)
}

func (api *API) RefreshSiteStats(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "RefreshSiteStats")
	defer span.End()

	// Get usercount from UserCount service
	userCount, err := api.UserCount.GetUserCount(ctx)
	if err != nil {
		log.Printf("Error getting user count: %v", err)
		return fmt.Errorf("error getting user count: %w", err)
	}
	totalUsers.Set(float64(userCount))

	dailyDatapointsRaw, err := api.Stats.GetAllDailyStatsSummaries(ctx, 10_000) // Get up to 10,000 days of data
	if err != nil {
		log.Printf("Error getting daily datapoints: %v", err)
		return fmt.Errorf("error getting daily datapoints: %w", err)
	}

	dailyDatapoints := []DailyDatapoint{}

	var totalPosts, totalLikes, totalFollows int64

	for _, datapoint := range dailyDatapointsRaw {
		// Filter out datapoints before 2023-03-01 and after tomorrow
		if datapoint.Date.Before(time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC)) || datapoint.Date.After(time.Now().AddDate(0, 0, 1)) {
			continue
		}
		dailyDatapoints = append(dailyDatapoints, DailyDatapoint{
			Date:                 datapoint.Date.UTC().Format("2006-01-02"),
			LikesPerDay:          datapoint.LikesPerDay,
			DailyActiveLikers:    datapoint.DailyActiveLikers,
			DailyActivePosters:   datapoint.DailyActivePosters,
			PostsPerDay:          datapoint.PostsPerDay,
			FollowsPerDay:        datapoint.FollowsPerDay,
			DailyActiveFollowers: datapoint.DailyActiveFollowers,
			BlocksPerDay:         datapoint.BlocksPerDay,
			DailyActiveBlockers:  datapoint.DailyActiveBlockers,
		})
		totalPosts += datapoint.PostsPerDay
		totalLikes += datapoint.LikesPerDay
		totalFollows += datapoint.FollowsPerDay
	}

	newStats := StatsCacheEntry{
		Stats: AuthorStatsResponse{
			TotalUsers:   userCount,
			TotalPosts:   totalPosts,
			TotalFollows: totalFollows,
			TotalLikes:   totalLikes,
			DailyData:    dailyDatapoints,
			UpdatedAt:    time.Now(),
		},
		Expiration: time.Now().Add(api.StatsCacheTTL),
	}

	api.StatsCacheRWMux.Lock()
	api.StatsCache = &newStats
	api.StatsCacheRWMux.Unlock()

	return nil
}
