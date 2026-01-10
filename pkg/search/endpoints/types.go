package endpoints

// GetCleanupStatusRequest for GET /repo/cleanup
type GetCleanupStatusRequest struct {
	JobID string `query:"job_id"`
	DID   string `query:"did"`
}

// CancelCleanupJobRequest for DELETE /repo/cleanup
type CancelCleanupJobRequest struct {
	JobID string `query:"job_id"`
}

// GetRepoRequest for GET /repo/:did
type GetRepoRequest struct {
	DID string `param:"did"`
}

// RedirectAtURIRequest for GET /redir
type RedirectAtURIRequest struct {
	URI string `query:"uri"`
}

// CleanupOldRecordsRequest for POST /repo/cleanup
type CleanupOldRecordsRequest struct {
	Identifier          string   `json:"identifier"`
	AppPassword         string   `json:"app_password"`
	CleanupTypes        []string `json:"cleanup_types"`
	DeleteUntilDaysAgo  int      `json:"delete_until_days_ago"`
	ActuallyDeleteStuff bool     `json:"actually_delete_stuff"`
}

// GetListMembersRequest for GET /list/members
type GetListMembersRequest struct {
	URI string `query:"uri"`
}
