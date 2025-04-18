package common

import "time"

// JobStatus defines the possible statuses for a checksum job.
type JobStatus string

const (
	JobStatusPending           JobStatus = "pending"
	JobStatusRunning           JobStatus = "running"
	JobStatusCompleted         JobStatus = "completed"
	JobStatusCompletedMismatch JobStatus = "completed_mismatch"
	JobStatusFailed            JobStatus = "failed"
	// Add other potential statuses here if needed

	ROW_CHECKSUM_MAX_RETRY_TIMES = 2
	JOB_ID_KEY                   = "job_id"
	CHECKSUM_RETRY_INTERVAL      = 3 * time.Second
)
