package auth

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jazware/bsky-experiments/pkg/indexer/store"
)

type StoreProvider struct {
	Store *store.Store
}

func NewStoreProvider(s *store.Store) *StoreProvider {
	return &StoreProvider{
		Store: s,
	}
}

func (p *StoreProvider) UpdateAPIKeyFeedMapping(ctx context.Context, apiKey string, feedAuthEntity *FeedAuthEntity) error {
	authBytes, err := json.Marshal(feedAuthEntity)
	if err != nil {
		return fmt.Errorf("failed to marshal feedAuthEntity: %w", err)
	}

	err = p.Store.CreateAPIKey(ctx, apiKey, string(authBytes), feedAuthEntity.UserDID)
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}

	return nil
}

func (p *StoreProvider) GetEntityFromAPIKey(ctx context.Context, apiKey string) (*FeedAuthEntity, error) {
	key, err := p.Store.GetAPIKey(ctx, apiKey)
	if err != nil {
		// ClickHouse doesn't return sql.ErrNoRows, so we check for the error message
		if err.Error() == "failed to get API key: EOF" || err.Error() == "failed to get API key: sql: no rows in result set" {
			return nil, ErrAPIKeyNotFound
		}
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	var authEntity FeedAuthEntity
	err = json.Unmarshal(key.AuthEntity, &authEntity)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal auth entity: %w", err)
	}

	return &authEntity, nil
}
