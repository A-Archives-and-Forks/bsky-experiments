package store

import (
	"context"
	"fmt"
	"time"
)

// APIKey represents an API key with its metadata
type APIKey struct {
	APIKey       string
	AuthEntity   []byte
	AssignedUser string
	CreatedAt    time.Time
}

// CreateAPIKey creates a new API key
func (s *Store) CreateAPIKey(ctx context.Context, apiKey, authEntity, assignedUser string) error {
	query := `
		INSERT INTO api_keys (api_key, auth_entity, assigned_user, time_us, deleted)
		VALUES (?, ?, ?, ?, 0)
	`

	timeUs := time.Now().UnixMicro()
	err := s.DB.Exec(ctx, query, apiKey, authEntity, assignedUser, timeUs)
	if err != nil {
		return fmt.Errorf("failed to create API key: %w", err)
	}

	return nil
}

// GetAPIKey retrieves an API key by its value
func (s *Store) GetAPIKey(ctx context.Context, apiKey string) (*APIKey, error) {
	query := `
		SELECT api_key, auth_entity, assigned_user, created_at
		FROM api_keys
		WHERE api_key = ? AND deleted = 0
		ORDER BY time_us DESC
		LIMIT 1
	`

	var key APIKey
	err := s.DB.QueryRow(ctx, query, apiKey).Scan(
		&key.APIKey,
		&key.AuthEntity,
		&key.AssignedUser,
		&key.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get API key: %w", err)
	}

	return &key, nil
}
