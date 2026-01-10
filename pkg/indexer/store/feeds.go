package store

import (
	"context"
	"fmt"
	"time"
)

// HotPost represents a trending post with engagement score
type HotPost struct {
	SubjectURI       string
	ActorDID         string
	Rkey             string
	SubjectCreatedAt time.Time
	InsertedAt       time.Time
	Langs            []string
	HasEmbeddedMedia bool
	Score            float64
	LikeCount        uint64
}

// GetHotPosts retrieves hot/trending posts ordered by score
func (s *Store) GetHotPosts(ctx context.Context, limit int, minScore float64) ([]HotPost, error) {
	query := `
		SELECT
			subject_uri,
			actor_did,
			rkey,
			subject_created_at,
			inserted_at,
			langs,
			has_embedded_media,
			score,
			like_count
		FROM recent_posts_with_score
		WHERE score >= ?
		ORDER BY score DESC
		LIMIT ?
	`

	rows, err := s.DB.Query(ctx, query, minScore, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query hot posts: %w", err)
	}
	defer rows.Close()

	var posts []HotPost
	for rows.Next() {
		var post HotPost
		if err := rows.Scan(
			&post.SubjectURI,
			&post.ActorDID,
			&post.Rkey,
			&post.SubjectCreatedAt,
			&post.InsertedAt,
			&post.Langs,
			&post.HasEmbeddedMedia,
			&post.Score,
			&post.LikeCount,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		posts = append(posts, post)
	}

	return posts, rows.Err()
}

// GetTopPostsInWindow retrieves top posts by like count within a time window
func (s *Store) GetTopPostsInWindow(ctx context.Context, hours int, limit int) ([]HotPost, error) {
	query := `
		SELECT
			p.uri AS subject_uri,
			p.did AS actor_did,
			p.rkey AS rkey,
			p.created_at AS subject_created_at,
			p.indexed_at AS inserted_at,
			p.langs AS langs,
			p.has_embedded_media AS has_embedded_media,
			like_count
		FROM posts AS p
		INNER JOIN (
			SELECT
				subject_uri,
				countIf(deleted = 0) AS like_count
			FROM likes
			WHERE created_at > now() - INTERVAL ? HOUR
			GROUP BY subject_uri
		) AS like_stats ON p.uri = like_stats.subject_uri
		WHERE p.deleted = 0
			AND p.created_at > now() - INTERVAL ? HOUR
			AND like_count > 10
		ORDER BY like_count DESC
		LIMIT ?
	`

	rows, err := s.DB.Query(ctx, query, hours, hours, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top posts: %w", err)
	}
	defer rows.Close()

	var posts []HotPost
	for rows.Next() {
		var post HotPost
		if err := rows.Scan(
			&post.SubjectURI,
			&post.ActorDID,
			&post.Rkey,
			&post.SubjectCreatedAt,
			&post.InsertedAt,
			&post.Langs,
			&post.HasEmbeddedMedia,
			&post.LikeCount,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		posts = append(posts, post)
	}

	return posts, rows.Err()
}

// LabelPost represents a post from a label feed
type LabelPost struct {
	ActorDID  string
	Rkey      string
	CreatedAt time.Time
}

// ListMPLS retrieves posts from the mpls feed
func (s *Store) ListMPLS(ctx context.Context, cursor string, limit int) ([]LabelPost, error) {
	query := `
		SELECT actor_did, rkey, created_at
		FROM mpls
		WHERE rkey < ?
		ORDER BY rkey DESC
		LIMIT ?
	`

	rows, err := s.DB.Query(ctx, query, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query mpls posts: %w", err)
	}
	defer rows.Close()

	var posts []LabelPost
	for rows.Next() {
		var post LabelPost
		if err := rows.Scan(&post.ActorDID, &post.Rkey, &post.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		posts = append(posts, post)
	}

	return posts, rows.Err()
}

// ListTQSP retrieves posts from the tqsp feed
func (s *Store) ListTQSP(ctx context.Context, cursor string, limit int) ([]LabelPost, error) {
	query := `
		SELECT actor_did, rkey, created_at
		FROM tqsp
		WHERE rkey < ?
		ORDER BY rkey DESC
		LIMIT ?
	`

	rows, err := s.DB.Query(ctx, query, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query tqsp posts: %w", err)
	}
	defer rows.Close()

	var posts []LabelPost
	for rows.Next() {
		var post LabelPost
		if err := rows.Scan(&post.ActorDID, &post.Rkey, &post.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		posts = append(posts, post)
	}

	return posts, rows.Err()
}
