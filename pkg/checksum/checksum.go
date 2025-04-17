package checksum

import (
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/common"
	"github.com/rs/zerolog/log"
	"sort"
	"sync"

	"github.com/arnkore/rds-checksum/pkg/batch"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/arnkore/rds-checksum/pkg/storage" // Added storage import

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

// CompareChecksumResults compares checksum results from two sources (e.g., master and replica).
type CompareChecksumResults struct {
	Match            bool
	MismatchBatches  []int // Indices of batches that don't match
	RowCountMismatch bool
	SrcTotalRows     int64
	TargetTotalRows  int64
	TargetFailedRows map[interface{}]string // PK -> Hash/Error for rows that failed comparison on Source 2
	SrcError         error                  // Errors during source processing (e.g., fetching rows)
	TargetError      error                  // Errors during target processing
}

// ChecksumValidator orchestrates the checksum validation process.
type ChecksumValidator struct {
	SrcConfig     *metadata.Config // Source Config (e.g., Master)
	TargetConfig  *metadata.Config // Target Config (e.g., Replica)
	TableName     string
	RowsPerBatch  int // Target number of rows per batch/batch
	CalcCRC32InDB bool
	dbStore       *storage.Store // Added: Database store for results
	jobID         int64          // Added: ID of the current job in the DB
	concurrency   int
}

// NewChecksumValidator creates a new validator.
func NewChecksumValidator(src, target *metadata.Config, tableName string, rowsPerBatch int, concurrency int, calcCrc32InDb bool, store *storage.Store) *ChecksumValidator {
	return &ChecksumValidator{
		SrcConfig:     src,
		TargetConfig:  target,
		TableName:     tableName,
		RowsPerBatch:  rowsPerBatch,
		CalcCRC32InDB: calcCrc32InDb,
		dbStore:       store, // Added
		jobID:         0,     // Initialized
		concurrency:   concurrency,
	}
}

// GetJobID returns the database job ID associated with this validation run.
func (cv *ChecksumValidator) GetJobID() int64 {
	return cv.jobID
}

// Represents the outcome of comparing a single batch
type BatchComparisonSummary struct {
	Index         int
	Match         bool // True if checksums and row counts match and no errors
	ChecksumMatch bool
	RowCountMatch bool
	SourceErr     error
	TargetErr     error
}

// updateJobStatus updates the job status in the database.
func (cv *ChecksumValidator) updateJobStatus(comparisonResult *CompareChecksumResults, finalError error) {
	var status common.JobStatus // Use the enum type
	errMsg := ""
	mismatchedCount := 0

	// Determine status based on comparisonResult and finalError
	if finalError != nil {
		status = common.JobStatusFailed
		errMsg = finalError.Error() // Capture the final error message
	} else if comparisonResult != nil {
		if comparisonResult.Match {
			status = common.JobStatusCompleted
		} else {
			status = common.JobStatusCompletedMismatch
		}
		mismatchedCount = len(comparisonResult.MismatchBatches)
		// Capture specific errors if finalError was nil but comparison had issues
		if comparisonResult.SrcError != nil {
			errMsg = fmt.Sprintf("Source error: %s;", comparisonResult.SrcError.Error())
		}
		if comparisonResult.TargetError != nil {
			errMsg += fmt.Sprintf("Target error: %s", comparisonResult.TargetError.Error())
		}
	} else {
		// This case should ideally be covered by finalError, but as a fallback:
		status = common.JobStatusFailed
		errMsg = "Unknown early failure, comparison result is nil"
		finalError = fmt.Errorf(errMsg) // Ensure finalError reflects this state
	}

	// Safely access comparisonResult fields, providing defaults if nil
	matchStatus := false
	srcRows, tgtRows := int64(0), int64(0)
	if comparisonResult != nil {
		matchStatus = comparisonResult.Match
		srcRows = comparisonResult.SrcTotalRows
		tgtRows = comparisonResult.TargetTotalRows
	}

	// Use slog for structured logging
	log.Info().Int64("job_id", cv.jobID).
		Str("status", string(status)).
		Bool("match", matchStatus).
		Int64("source_rows", srcRows).
		Int64("target_rows", tgtRows).
		Int("mismatched_batches", mismatchedCount).
		Str("error_message", errMsg).
		Msg("Attempting to update job completion status")
	// Convert enum back to string for the database layer
	err := cv.dbStore.UpdateJobCompletion(cv.jobID, string(status), matchStatus, srcRows, tgtRows, mismatchedCount, errMsg)
	if err != nil {
		log.Error().Int64("job_id", cv.jobID).Err(err).Msg("Failed to update final job status")
	}
}

// createJobRecord creates a job record in the database.
func (cv *ChecksumValidator) createJobRecord() error {
	var err error
	cv.jobID, err = cv.dbStore.CreateJob(cv.TableName, cv.RowsPerBatch)
	return err
}

// setupMetadata establishes DB connections, fetches metadata, and performs initial checks.
func (cv *ChecksumValidator) setupMetadata(srcConnProvider, targetConnProvider *metadata.DbConnProvider) (*metadata.TableInfo, *metadata.TableInfo, error) {
	var srcInfo, targetInfo *metadata.TableInfo
	var srcMetaErr, targetMetaErr error
	// Get metadata concurrently
	var metaWg sync.WaitGroup
	metaWg.Add(2)
	go func() {
		defer metaWg.Done()
		srcMetaProvider := metadata.NewTableMetaProvider(srcConnProvider, cv.SrcConfig.Database, cv.TableName)
		srcInfo, srcMetaErr = srcMetaProvider.QueryTableInfo(cv.TableName)
	}()
	go func() {
		defer metaWg.Done()
		targetMetaProvider := metadata.NewTableMetaProvider(targetConnProvider, cv.TargetConfig.Database, cv.TableName)
		targetInfo, targetMetaErr = targetMetaProvider.QueryTableInfo(cv.TableName)
	}()
	metaWg.Wait()

	if srcMetaErr != nil {
		return nil, nil, fmt.Errorf("failed to setup metadata for table '%s': %cv", cv.TableName, srcMetaErr)
	}
	if targetMetaErr != nil {
		return nil, nil, fmt.Errorf("failed to setup metadata for table '%s': %cv", cv.TableName, targetMetaErr)
	}

	// Check for table existence mismatch
	if !srcInfo.TableExists || !targetInfo.TableExists {
		return srcInfo, targetInfo, fmt.Errorf("table existence failed: source exists=%t, target exists=%t", srcInfo.TableExists, targetInfo.TableExists)
	}

	// Check for column definition mismatch
	if !equalStringSlices(srcInfo.Columns, targetInfo.Columns) {
		return srcInfo, targetInfo, fmt.Errorf("column definition mismatch for table %s", cv.TableName)
	}

	return srcInfo, targetInfo, nil
}

// calculateBatches determines the batches for checksum based on source table info.
func (cv *ChecksumValidator) calculateBatches(srcInfo *metadata.TableInfo) ([]metadata.Batch, error) {
	batchCalculator := batch.NewBatchCalculator(srcInfo, cv.RowsPerBatch)
	batches, err := batchCalculator.CalculateBatches()
	if err != nil {
		return nil, fmt.Errorf("failed to calculate batches: %w", err)
	}
	log.Info().Int("count", len(batches)).Str("table", cv.TableName).Msg("Calculated batches")
	return batches, nil
}

// processBatchesConcurrently calculates checksums, compares, saves results, and returns summaries.
func (cv *ChecksumValidator) processBatchesConcurrently(srcInfo *metadata.TableInfo, batches []metadata.Batch) ([]BatchComparisonSummary, error) {
	var eg errgroup.Group
	eg.SetLimit(cv.concurrency)
	resultsChan := make(chan BatchComparisonSummary, len(batches))
	summaries := make([]BatchComparisonSummary, 0, len(batches))
	columns := srcInfo.Columns
	srcProvider := metadata.NewDBConnProvider(cv.SrcConfig)       // Assuming DBConnProvider doesn't need logger yet
	targetProvider := metadata.NewDBConnProvider(cv.TargetConfig) // Assuming DBConnProvider doesn't need logger yet

	log.Info().Int("batch_count", len(batches)).
		Int("concurrency", cv.concurrency).
		Msg("Starting concurrent batch processing")

	for _, p := range batches {
		part := p                                                                                     // Capture loop variable
		pcc := batch.NewBatchChecksumCalculator(&part, cv.CalcCRC32InDB, srcProvider, targetProvider) // Assuming BatchChecksumCalculator doesn't need logger yet
		eg.Go(func() error {
			log.Debug().Int("batch_index", part.Index).
				Msg("Starting batch processing")
			var checksumInfo = pcc.CalculateAndCompareChecksum(columns) // Assuming CalculateAndCompareChecksum doesn't need logger yet
			cv.saveBatchResultToDB(part.Index, checksumInfo)

			resultsChan <- BatchComparisonSummary{
				Index:         part.Index,
				Match:         checksumInfo.IsOverallMatch(),
				ChecksumMatch: checksumInfo.IsChecksumMatch(),
				RowCountMatch: checksumInfo.IsRowCountMatch(),
				SourceErr:     checksumInfo.SrcErr,
				TargetErr:     checksumInfo.TargetErr,
			}
			log.Info().Int("batch_index", part.Index).
				Msg("Finished batch processing")
			// Propagate errors from CalculateAndCompareChecksum if necessary (currently handled in BatchComparisonSummary)
			// return checksumInfo.SrcErr // Or combine errors
			return nil // errgroup handles errors returned here
		})
	}

	groupErr := eg.Wait() // This captures the first non-nil error returned by any goroutine
	close(resultsChan)

	for summary := range resultsChan {
		summaries = append(summaries, summary)
	}
	log.Info().Int("processed_count", len(summaries)).
		Msg("Finished processing all batches")

	if groupErr != nil {
		log.Error().Err(groupErr).Msg("Error occurred during concurrent batch processing")
	}

	// Return summaries and the critical error from the group
	return summaries, groupErr
}

// saveBatchResultToDB saves the detailed batch results to the database if configured.
func (cv *ChecksumValidator) saveBatchResultToDB(batchIndex int, checksumInfo *batch.BatchChecksumInfo) {
	// Log errors regardless of DB storage
	if checksumInfo.SrcErr != nil {
		log.Error().Int("batch_index", batchIndex).
			Err(checksumInfo.SrcErr).
			Msg("Batch source error")
	}
	if checksumInfo.TargetErr != nil {
		log.Error().Int("batch_index", batchIndex).
			Err(checksumInfo.TargetErr).
			Msg("Batch target error")
	}

	// Log mismatches immediately
	if !checksumInfo.IsRowCountMatch() {
		log.Warn().Int("batch_index", batchIndex).
			Int("source_rows", checksumInfo.SrcRowCount).
			Int("target_rows", checksumInfo.TargetRowCount).
			Msg("Batch row count mismatch")
	} else if !checksumInfo.IsChecksumMatch() {
		// Only log checksum mismatch if row counts matched (otherwise it's implied)
		log.Warn().Int("batch_index", batchIndex).
			Uint32("source_checksum", checksumInfo.SrcChecksum).
			Uint32("target_checksum", checksumInfo.TargetChecksum).
			Msg("Batch checksum mismatch")
	}

	// Save to DB if configured and job created
	if cv.dbStore != nil && cv.jobID > 0 {
		// ... (rest of the saveBatchResultToDB logic, converting existing log.Printf if any to cv.logger) ...
		// Example: If there were internal logs here, convert them.
		batchResultData := storage.BatchResultData{
			JobID:          cv.jobID,
			BatchIndex:     batchIndex,
			SourceChecksum: checksumInfo.SrcChecksum,
			TargetChecksum: checksumInfo.TargetChecksum,
			SourceRowCount: checksumInfo.SrcRowCount,
			TargetRowCount: checksumInfo.TargetRowCount,
			ChecksumMatch:  checksumInfo.IsChecksumMatch(),
			RowCountMatch:  checksumInfo.IsRowCountMatch(),
			SourceError:    errorToString(checksumInfo.SrcErr),
			TargetError:    errorToString(checksumInfo.TargetErr),
		}
		dbErr := cv.dbStore.SaveBatchResult(batchResultData)

		if dbErr != nil {
			log.Error().Int64("job_id", cv.jobID).
				Int("batch_index", batchIndex).
				Err(dbErr).
				Msg("Failed to save batch result to database")
		} else {
			log.Debug().Int64("job_id", cv.jobID).
				Int("batch_index", batchIndex).
				Msg("Saved batch result to database")
		}
	} else {
		log.Debug().Int64("job_id", cv.jobID).
			Int("batch_index", batchIndex).
			Bool("db_store_nil", cv.dbStore == nil).
			Msg("Skipping DB save for batch result")
	}
}

// aggregateBatchResults processes summaries and updates the final comparison result.
func (cv *ChecksumValidator) aggregateBatchResults(summaries []BatchComparisonSummary, comparisonResult *CompareChecksumResults) {
	mismatchedBatchesMap := make(map[int]bool)
	comparisonResult.Match = true

	for _, summary := range summaries {
		if !summary.Match {
			comparisonResult.Match = false
			mismatchedBatchesMap[summary.Index] = true
			if !summary.RowCountMatch {
				comparisonResult.RowCountMismatch = true
			}
			if summary.SourceErr != nil && comparisonResult.SrcError == nil {
				comparisonResult.SrcError = summary.SourceErr
			}
			if summary.TargetErr != nil && comparisonResult.TargetError == nil {
				comparisonResult.TargetError = summary.TargetErr
			}
		}
	}

	// Populate MismatchBatches list from the map
	comparisonResult.MismatchBatches = make([]int, 0, len(mismatchedBatchesMap))
	for idx := range mismatchedBatchesMap {
		comparisonResult.MismatchBatches = append(comparisonResult.MismatchBatches, idx)
	}
	sort.Ints(comparisonResult.MismatchBatches)
}

// Run performs the checksum validation and stores results in the DB if configured.
func (cv *ChecksumValidator) Run() (finalResult *CompareChecksumResults, finalError error) {
	// Initialize final result structure
	finalResult = &CompareChecksumResults{
		Match:            false, // Assume not match until proven otherwise
		MismatchBatches:  []int{},
		TargetFailedRows: map[interface{}]string{}, // Initialize map
	}

	// Defer the status update to capture final state
	defer func() {
		// Use the captured finalError and finalResult
		cv.updateJobStatus(finalResult, finalError)
		// Maybe add a final log summarizing the run outcome based on finalError and finalResult.Match
		if finalError != nil {
			log.Error().Int64("job_id", cv.jobID).Err(finalError).
				Msg("Validation run finished with error")
		} else if finalResult != nil && !finalResult.Match {
			log.Warn().Int64("job_id", cv.jobID).
				Int("mismatched_batches", len(finalResult.MismatchBatches)).
				Msg("Validation run finished with mismatches")
		} else {
			log.Info().Bool("match", finalResult.Match).
				Int64("job_id", cv.jobID).
				Msg("Validation run finished successfully")
		}
	}()

	// Create job record first
	err := cv.createJobRecord()
	if err != nil {
		log.Error().Err(err).Msg("Failed to create checksum job in database. Proceeding without DB logging.")
	}

	// --- Metadata Setup ---
	srcConnProvider := metadata.NewDBConnProvider(cv.SrcConfig)
	targetConnProvider := metadata.NewDBConnProvider(cv.TargetConfig)
	srcInfo, targetInfo, metaErr := cv.setupMetadata(srcConnProvider, targetConnProvider)
	if metaErr != nil {
		log.Error().Err(metaErr).Msg("Metadata setup failed")
		finalError = fmt.Errorf("metadata setup failed: %w", metaErr)
		return finalResult, finalError // Return immediately on critical setup error
	}
	finalResult.SrcTotalRows = srcInfo.RowCount
	finalResult.TargetTotalRows = targetInfo.RowCount

	// Further checks could be added here, e.g., ensuring PK is numeric for range partitioning.

	// --- Batch Calculation ---
	batches, batchErr := cv.calculateBatches(srcInfo)
	if batchErr != nil {
		log.Error().Err(batchErr).Msg("Failed to calculate batches")
		finalError = fmt.Errorf("batch calculation failed: %w", batchErr) // Keep fmt for error wrapping
		return finalResult, finalError                                    // Return immediately
	}
	if len(batches) == 0 {
		log.Warn().Str("table", cv.TableName).
			Msg("No batches calculated, possibly an empty table or configuration issue.")
		// Consider this a success (empty tables match), or handle as needed.
		finalResult.Match = true // Explicitly set match for empty table case
		return finalResult, nil  // Nothing more to do
	}

	// --- Process Batches ---
	batchSummaries, processErr := cv.processBatchesConcurrently(srcInfo, batches)
	if processErr != nil {
		finalError = fmt.Errorf("concurrent batch processing failed: %w", processErr)
		log.Warn().Msg("Aggregating potentially incomplete batch results due to processing error")
	}

	// --- Aggregate Results ---
	log.Info().Int("count", len(batchSummaries)).Msg("Aggregating batch results")
	cv.aggregateBatchResults(batchSummaries, finalResult)
	return finalResult, finalError
}

// Helper function to compare string slices
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// Helper function to safely convert error to string for DB storage
func errorToString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
