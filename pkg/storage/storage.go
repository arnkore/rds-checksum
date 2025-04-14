package storage

import (
	"database/sql"
	"fmt"
	"time"
	// Add other necessary imports like "errors" if needed
)

// Store handles database operations for checksum results.
type Store struct {
	db *sql.DB
}

// NewStore creates a new Store instance.
func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

const (
	createChecksumJobsTableSQL = `
CREATE TABLE IF NOT EXISTS checksum_jobs (
    job_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    source_info TEXT,
    target_info TEXT,
    rows_per_batch INT,
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, running, completed, failed
    overall_match BOOLEAN NULL,
    source_total_rows BIGINT NULL,
    target_total_rows BIGINT NULL,
    mismatched_partitions_count INT DEFAULT 0,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP NULL,
    error_message TEXT NULL,
    INDEX idx_job_table_status (table_name, status),
    INDEX idx_job_start_time (start_time)
);`

	createPartitionResultsTableSQL = `
CREATE TABLE IF NOT EXISTS partition_results (
    result_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id BIGINT NOT NULL,
    partition_index INT NOT NULL,
    source_checksum VARCHAR(64),
    target_checksum VARCHAR(64),
    source_row_count BIGINT,
    target_row_count BIGINT,
    checksum_match BOOLEAN,
    row_count_match BOOLEAN,
    source_error TEXT NULL,
    target_error TEXT NULL,
    comparison_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES checksum_jobs(job_id) ON DELETE CASCADE,
    INDEX idx_part_job_id (job_id),
    INDEX idx_part_job_mismatch (job_id, checksum_match)
);`
)

// InitializeSchema creates the necessary database tables if they don't exist.
func (s *Store) InitializeSchema() error {
	if s.db == nil {
		return fmt.Errorf("database connection is nil")
	}

	_, err := s.db.Exec(createChecksumJobsTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create checksum_jobs table: %w", err)
	}

	_, err = s.db.Exec(createPartitionResultsTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create partition_results table: %w", err)
	}

	return nil
}

// CreateJob inserts a new job record and returns its ID.
func (s *Store) CreateJob(tableName, sourceInfo, targetInfo string, rowsPerBatch int) (int64, error) {
	if s.db == nil {
		return 0, fmt.Errorf("database connection is nil")
	}
	// Placeholder implementation
	query := `INSERT INTO checksum_jobs (table_name, source_info, target_info, rows_per_batch, status, start_time)
	          VALUES (?, ?, ?, ?, ?, ?)`
	startTime := time.Now()
	result, err := s.db.Exec(query, tableName, sourceInfo, targetInfo, rowsPerBatch, "pending", startTime)
	if err != nil {
		return 0, fmt.Errorf("failed to insert new job: %w", err)
	}
	jobID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert ID for job: %w", err)
	}
	return jobID, nil
}

// UpdateJobStatus updates the status and optionally an error message for a job.
func (s *Store) UpdateJobStatus(jobID int64, status string, errorMessage string) error {
	if s.db == nil {
		return fmt.Errorf("database connection is nil")
	}
	// Placeholder implementation
	var query string
	var args []interface{}

	if errorMessage != "" {
		query = `UPDATE checksum_jobs SET status = ?, error_message = ? WHERE job_id = ?`
		args = append(args, status, errorMessage, jobID)
	} else {
		query = `UPDATE checksum_jobs SET status = ? WHERE job_id = ?`
		args = append(args, status, jobID)
	}

	_, err := s.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to update job status for job %d: %w", jobID, err)
	}
	return nil
}

// UpdateJobCompletion updates the job record upon successful or failed completion.
func (s *Store) UpdateJobCompletion(jobID int64, status string, overallMatch bool, srcTotalRows, targetTotalRows int64, mismatchedCount int, errorMessage string) error {
	if s.db == nil {
		return fmt.Errorf("database connection is nil")
	}
	// Placeholder implementation
	query := `UPDATE checksum_jobs
	          SET status = ?, overall_match = ?, source_total_rows = ?, target_total_rows = ?,
	              mismatched_partitions_count = ?, end_time = ?, error_message = ?
	          WHERE job_id = ?`
	endTime := time.Now()
	_, err := s.db.Exec(query, status, overallMatch, srcTotalRows, targetTotalRows, mismatchedCount, endTime, sql.NullString{String: errorMessage, Valid: errorMessage != ""}, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job completion for job %d: %w", jobID, err)
	}
	return nil
}

// PartitionResultData holds data for a single partition result to be saved.
type PartitionResultData struct {
	JobID           int64
	PartitionIndex  int
	SourceChecksum  string
	TargetChecksum  string
	SourceRowCount  int64
	TargetRowCount  int64
	ChecksumMatch   bool
	RowCountMatch   bool
	SourceError     string
	TargetError     string
}

// SavePartitionResult inserts a result record for a specific partition.
func (s *Store) SavePartitionResult(data PartitionResultData) error {
	if s.db == nil {
		return fmt.Errorf("database connection is nil")
	}
	// Placeholder implementation
	query := `INSERT INTO partition_results (job_id, partition_index, source_checksum, target_checksum,
	                                        source_row_count, target_row_count, checksum_match, row_count_match,
	                                        source_error, target_error, comparison_time)
	          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	comparisonTime := time.Now()
	_, err := s.db.Exec(query, data.JobID, data.PartitionIndex,
		sql.NullString{String: data.SourceChecksum, Valid: data.SourceChecksum != ""},
		sql.NullString{String: data.TargetChecksum, Valid: data.TargetChecksum != ""},
		sql.NullInt64{Int64: data.SourceRowCount, Valid: true}, // Assuming row count is always known
		sql.NullInt64{Int64: data.TargetRowCount, Valid: true},
		data.ChecksumMatch, data.RowCountMatch,
		sql.NullString{String: data.SourceError, Valid: data.SourceError != ""},
		sql.NullString{String: data.TargetError, Valid: data.TargetError != ""},
		comparisonTime,
	)
	if err != nil {
		return fmt.Errorf("failed to save partition result for job %d, partition %d: %w", data.JobID, data.PartitionIndex, err)
	}
	return nil
}

// Add helper functions to convert potential errors to strings, handle NULLs etc. as needed. 