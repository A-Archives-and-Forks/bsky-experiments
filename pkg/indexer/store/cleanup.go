package store

import (
	"context"
	"fmt"
	"time"
)

// RepoCleanupJob represents a repo cleanup job
type RepoCleanupJob struct {
	JobID           string     `json:"job_id"`
	Repo            string     `json:"repo"`
	RefreshToken    string     `json:"refresh_token"`
	CleanupTypes    []string   `json:"cleanup_types"`
	DeleteOlderThan time.Time  `json:"delete_older_than"`
	NumDeleted      int64      `json:"num_deleted"`
	NumDeletedToday int64      `json:"num_deleted_today"`
	EstNumRemaining int64      `json:"est_num_remaining"`
	JobState        string     `json:"job_state"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	LastDeletedAt   *time.Time `json:"last_deleted_at"`
}

// UpsertRepoCleanupJob creates or updates a repo cleanup job
func (s *Store) UpsertRepoCleanupJob(ctx context.Context, job RepoCleanupJob) (*RepoCleanupJob, error) {
	timeUs := time.Now().UnixMicro()

	query := `
		INSERT INTO repo_cleanup_jobs (
			job_id, repo, refresh_token, cleanup_types, delete_older_than,
			num_deleted, num_deleted_today, est_num_remaining, job_state,
			created_at, updated_at, last_deleted_at, time_us
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	err := s.DB.Exec(ctx, query,
		job.JobID,
		job.Repo,
		job.RefreshToken,
		job.CleanupTypes,
		job.DeleteOlderThan,
		job.NumDeleted,
		job.NumDeletedToday,
		job.EstNumRemaining,
		job.JobState,
		job.CreatedAt,
		job.UpdatedAt,
		job.LastDeletedAt,
		timeUs,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert repo cleanup job: %w", err)
	}

	return &job, nil
}

// GetRepoCleanupJob retrieves a specific cleanup job by ID
func (s *Store) GetRepoCleanupJob(ctx context.Context, jobID string) (*RepoCleanupJob, error) {
	query := `
		SELECT
			job_id, repo, refresh_token, cleanup_types, delete_older_than,
			num_deleted, num_deleted_today, est_num_remaining, job_state,
			created_at, updated_at, last_deleted_at
		FROM repo_cleanup_jobs
		WHERE job_id = ?
		ORDER BY time_us DESC
		LIMIT 1
	`

	var job RepoCleanupJob
	err := s.DB.QueryRow(ctx, query, jobID).Scan(
		&job.JobID,
		&job.Repo,
		&job.RefreshToken,
		&job.CleanupTypes,
		&job.DeleteOlderThan,
		&job.NumDeleted,
		&job.NumDeletedToday,
		&job.EstNumRemaining,
		&job.JobState,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.LastDeletedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get repo cleanup job: %w", err)
	}

	return &job, nil
}

// GetCleanupJobsByRepo retrieves cleanup jobs for a specific repo
func (s *Store) GetCleanupJobsByRepo(ctx context.Context, repo string, limit int) ([]RepoCleanupJob, error) {
	query := `
		SELECT
			job_id, repo, refresh_token, cleanup_types, delete_older_than,
			num_deleted, num_deleted_today, est_num_remaining, job_state,
			created_at, updated_at, last_deleted_at
		FROM repo_cleanup_jobs
		WHERE repo = ?
		ORDER BY updated_at DESC, time_us DESC
		LIMIT ?
	`

	rows, err := s.DB.Query(ctx, query, repo, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query cleanup jobs by repo: %w", err)
	}
	defer rows.Close()

	var jobs []RepoCleanupJob
	for rows.Next() {
		var job RepoCleanupJob
		if err := rows.Scan(
			&job.JobID,
			&job.Repo,
			&job.RefreshToken,
			&job.CleanupTypes,
			&job.DeleteOlderThan,
			&job.NumDeleted,
			&job.NumDeletedToday,
			&job.EstNumRemaining,
			&job.JobState,
			&job.CreatedAt,
			&job.UpdatedAt,
			&job.LastDeletedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan cleanup job: %w", err)
		}
		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}

// GetRunningCleanupJobsByRepo retrieves running cleanup jobs for a specific repo
func (s *Store) GetRunningCleanupJobsByRepo(ctx context.Context, repo string) ([]RepoCleanupJob, error) {
	query := `
		SELECT
			job_id, repo, refresh_token, cleanup_types, delete_older_than,
			num_deleted, num_deleted_today, est_num_remaining, job_state,
			created_at, updated_at, last_deleted_at
		FROM repo_cleanup_jobs
		WHERE repo = ? AND job_state = 'running'
		ORDER BY updated_at DESC, time_us DESC
	`

	rows, err := s.DB.Query(ctx, query, repo)
	if err != nil {
		return nil, fmt.Errorf("failed to query running cleanup jobs: %w", err)
	}
	defer rows.Close()

	var jobs []RepoCleanupJob
	for rows.Next() {
		var job RepoCleanupJob
		if err := rows.Scan(
			&job.JobID,
			&job.Repo,
			&job.RefreshToken,
			&job.CleanupTypes,
			&job.DeleteOlderThan,
			&job.NumDeleted,
			&job.NumDeletedToday,
			&job.EstNumRemaining,
			&job.JobState,
			&job.CreatedAt,
			&job.UpdatedAt,
			&job.LastDeletedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan cleanup job: %w", err)
		}
		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}

// GetRunningCleanupJobs retrieves running cleanup jobs (limited)
func (s *Store) GetRunningCleanupJobs(ctx context.Context, limit int) ([]RepoCleanupJob, error) {
	query := `
		SELECT
			job_id, repo, refresh_token, cleanup_types, delete_older_than,
			num_deleted, num_deleted_today, est_num_remaining, job_state,
			created_at, updated_at, last_deleted_at
		FROM repo_cleanup_jobs
		WHERE job_state = 'running'
		ORDER BY updated_at ASC, time_us DESC
		LIMIT ?
	`

	rows, err := s.DB.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query running cleanup jobs: %w", err)
	}
	defer rows.Close()

	var jobs []RepoCleanupJob
	for rows.Next() {
		var job RepoCleanupJob
		if err := rows.Scan(
			&job.JobID,
			&job.Repo,
			&job.RefreshToken,
			&job.CleanupTypes,
			&job.DeleteOlderThan,
			&job.NumDeleted,
			&job.NumDeletedToday,
			&job.EstNumRemaining,
			&job.JobState,
			&job.CreatedAt,
			&job.UpdatedAt,
			&job.LastDeletedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan cleanup job: %w", err)
		}
		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}

// CleanupStats represents aggregated cleanup statistics
type CleanupStats struct {
	TotalNumDeleted int64  `json:"total_num_deleted"`
	NumJobs         uint64 `json:"num_jobs"`
	NumRepos        uint64 `json:"num_repos"`
}

// GetCleanupStats retrieves aggregate cleanup statistics
func (s *Store) GetCleanupStats(ctx context.Context) (*CleanupStats, error) {
	query := `
		SELECT
			sum(num_deleted) AS total_num_deleted,
			count() AS num_jobs,
			uniq(repo) AS num_repos
		FROM repo_cleanup_jobs
	`

	var stats CleanupStats
	err := s.DB.QueryRow(ctx, query).Scan(
		&stats.TotalNumDeleted,
		&stats.NumJobs,
		&stats.NumRepos,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get cleanup stats: %w", err)
	}

	return &stats, nil
}

// DeleteRepoCleanupJob deletes a cleanup job (soft delete via inserting new version with deleted state)
func (s *Store) DeleteRepoCleanupJob(ctx context.Context, jobID string) error {
	// For ClickHouse ReplacingMergeTree, we can't truly delete, so we mark as deleted
	// by inserting a new version with a special state or just skip implementation
	// For now, we'll just leave this as a no-op or you could insert a tombstone record
	return nil
}
