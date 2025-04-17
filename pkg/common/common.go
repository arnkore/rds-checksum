package common

// JobStatus defines the possible statuses for a checksum job.
type JobStatus string

const (
	JobStatusPending           JobStatus = "pending"
	JobStatusRunning           JobStatus = "running"
	JobStatusCompleted         JobStatus = "completed"
	JobStatusCompletedMismatch JobStatus = "completed_mismatch"
	JobStatusFailed            JobStatus = "failed"
	// Add other potential statuses here if needed
)
