package store

import (
	"context"
	"fmt"
	"time"
)

// ActorHasLabel checks if an actor has a specific label
func (s *Store) ActorHasLabel(ctx context.Context, actorDID, label string) (bool, error) {
	query := `
		SELECT count() > 0
		FROM actor_labels
		WHERE actor_did = ? AND label = ? AND deleted = 0
	`

	var hasLabel bool
	err := s.DB.QueryRow(ctx, query, actorDID, label).Scan(&hasLabel)
	if err != nil {
		return false, fmt.Errorf("failed to check actor label: %w", err)
	}

	return hasLabel, nil
}

// CreateActorLabel assigns a label to an actor
func (s *Store) CreateActorLabel(ctx context.Context, actorDID, label string) error {
	query := `
		INSERT INTO actor_labels (actor_did, label, time_us, deleted)
		VALUES (?, ?, ?, 0)
	`

	timeUs := time.Now().UnixMicro()
	err := s.DB.Exec(ctx, query, actorDID, label, timeUs)
	if err != nil {
		return fmt.Errorf("failed to create actor label: %w", err)
	}

	return nil
}

// DeleteActorLabel removes a label from an actor (marks as deleted)
func (s *Store) DeleteActorLabel(ctx context.Context, actorDID, label string) error {
	query := `
		INSERT INTO actor_labels (actor_did, label, time_us, deleted)
		VALUES (?, ?, ?, 1)
	`

	timeUs := time.Now().UnixMicro()
	err := s.DB.Exec(ctx, query, actorDID, label, timeUs)
	if err != nil {
		return fmt.Errorf("failed to delete actor label: %w", err)
	}

	return nil
}

// ListActorsByLabel returns all actors with a specific label
func (s *Store) ListActorsByLabel(ctx context.Context, label string) ([]string, error) {
	query := `
		SELECT DISTINCT actor_did
		FROM actor_labels
		WHERE label = ? AND deleted = 0
		ORDER BY actor_did
	`

	rows, err := s.DB.Query(ctx, query, label)
	if err != nil {
		return nil, fmt.Errorf("failed to list actors by label: %w", err)
	}
	defer rows.Close()

	var actors []string
	for rows.Next() {
		var actorDID string
		if err := rows.Scan(&actorDID); err != nil {
			return nil, fmt.Errorf("failed to scan actor DID: %w", err)
		}
		actors = append(actors, actorDID)
	}

	return actors, rows.Err()
}
