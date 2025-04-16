package checksum

import (
	"fmt"
	"log" // Added for logging DB errors
	"sort"
	"sync"

	"github.com/arnkore/rds-checksum/pkg/batch"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/arnkore/rds-checksum/pkg/storage" // Added storage import

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

// ChecksumResult holds the result of a checksum calculation.
type ChecksumResult struct {
	Count    int64
	Checksum string
}

// RowData holds data for a single row.
type RowData struct {
	PK    interface{} // Primary Key value
	Hash  string      // Hash of the row data
	Error error
}

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
	SrcConfig    *metadata.Config // Source Config (e.g., Master)
	TargetConfig *metadata.Config // Target Config (e.g., Replica)
	TableName    string
	RowsPerBatch int            // Target number of rows per batch/batch
	dbStore      *storage.Store // Added: Database store for results
	jobID        int64          // Added: ID of the current job in the DB
	concurrency  int
}

// NewChecksumValidator creates a new validator.
func NewChecksumValidator(src, target *metadata.Config, tableName string, rowsPerBatch int, concurrency int, store *storage.Store) *ChecksumValidator {
	return &ChecksumValidator{
		SrcConfig:    src,
		TargetConfig: target,
		TableName:    tableName,
		RowsPerBatch: rowsPerBatch,
		dbStore:      store, // Added
		jobID:        0,     // Initialized
		concurrency:  concurrency,
	}
}

// GetJobID returns the database job ID associated with this validation run.
func (v *ChecksumValidator) GetJobID() int64 {
	return v.jobID
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
func (v *ChecksumValidator) updateJobStatus(comparisonResult *CompareChecksumResults, finalError error) {
	if v.dbStore != nil && v.jobID > 0 {
		status := "completed"
		errMsg := ""
		mismatchedCount := 0

		// Ensure comparisonResult is not nil before accessing
		if comparisonResult != nil {
			if !comparisonResult.Match {
				status = "completed_mismatch"
			}
			mismatchedCount = len(comparisonResult.MismatchBatches)
		} else {
			// If comparisonResult is nil, it implies an early failure
			status = "failed"
			if finalError == nil {
				finalError = fmt.Errorf("unknown early failure, comparison result is nil")
			}
		}

		if finalError != nil {
			status = "failed"
			errMsg = errorToString(finalError)
		}

		// Safely access comparisonResult fields, providing defaults if nil
		matchStatus := false
		srcRows, tgtRows := int64(0), int64(0)
		if comparisonResult != nil {
			matchStatus = comparisonResult.Match
			srcRows = comparisonResult.SrcTotalRows
			tgtRows = comparisonResult.TargetTotalRows
		}

		log.Printf("Attempting to update job %d completion status: %s, match: %t, srcRows: %d, tgtRows: %d, mismatches: %d, error: '%s'",
			v.jobID, status, matchStatus, srcRows, tgtRows, mismatchedCount, errMsg)

		err := v.dbStore.UpdateJobCompletion(v.jobID, status, matchStatus, srcRows, tgtRows, mismatchedCount, errMsg)
		if err != nil {
			log.Printf("ERROR: Failed to update final job status for job %d: %v\n", v.jobID, err)
		} else {
			fmt.Printf("Job %d final status updated to %s.\n", v.jobID, status)
		}
	} else if finalError != nil {
		log.Printf("Validation finished with error (DB not configured or job creation failed): %v", finalError)
	} else if comparisonResult != nil {
		log.Printf("Validation finished (DB not configured or job creation failed). Match status: %t", comparisonResult.Match)
	}
}

// createJobRecord creates a job record in the database.
func (v *ChecksumValidator) createJobRecord() error {
	jobID, err := v.dbStore.CreateJob(v.TableName, v.RowsPerBatch)
	if err != nil {
		log.Printf("ERROR: Failed to create checksum job in database: %v. Proceeding without DB logging.\n", err)
		return err
	}
	v.jobID = jobID
	fmt.Printf("Created database job with ID: %d\n", v.jobID)
	return nil
}

// setupMetadata establishes DB connections, fetches metadata, and performs initial checks.
func (v *ChecksumValidator) setupMetadata(srcConnProvider, targetConnProvider *metadata.DbConnProvider) (*metadata.TableInfo, *metadata.TableInfo, error) {
	var srcInfo, targetInfo *metadata.TableInfo
	var srcMetaErr, targetMetaErr error
	// Get metadata concurrently
	var metaWg sync.WaitGroup
	metaWg.Add(2)
	go func() {
		defer metaWg.Done()
		srcMetaProvider := metadata.NewTableMetaProvider(srcConnProvider, v.SrcConfig.Database, v.TableName)
		srcInfo, srcMetaErr = srcMetaProvider.QueryTableInfo(v.TableName)
	}()
	go func() {
		defer metaWg.Done()
		targetMetaProvider := metadata.NewTableMetaProvider(targetConnProvider, v.TargetConfig.Database, v.TableName)
		targetInfo, targetMetaErr = targetMetaProvider.QueryTableInfo(v.TableName)
	}()
	metaWg.Wait()

	if srcMetaErr != nil {
		return nil, nil, fmt.Errorf("failed to setup metadata for table '%s': %v", v.TableName, srcMetaErr)
	}
	if targetMetaErr != nil {
		return nil, nil, fmt.Errorf("failed to setup metadata for table '%s': %v", v.TableName, targetMetaErr)
	}

	// Check for table existence mismatch
	if !srcInfo.TableExists || !targetInfo.TableExists {
		return srcInfo, targetInfo, fmt.Errorf("table existence failed: source exists=%t, target exists=%t", srcInfo.TableExists, targetInfo.TableExists)
	}

	// Check for column definition mismatch
	if !equalStringSlices(srcInfo.Columns, targetInfo.Columns) {
		return srcInfo, targetInfo, fmt.Errorf("column definition mismatch for table %s", v.TableName)
	}

	return srcInfo, targetInfo, nil
}

// calculateBatches determines the batches for checksumming based on source table info.
func (v *ChecksumValidator) calculateBatches(srcInfo *metadata.TableInfo) ([]metadata.Batch, error) {
	// Ensure the batch calculator uses RowsPerBatch if intended, or adjust constructor call.
	batchCalculator, err := batch.NewBatchCalculator(srcInfo, v.RowsPerBatch)
	if err != nil {
		return nil, fmt.Errorf("failed to create batch calculator: %w", err)
	}
	// Assuming calculator internally uses RowsPerBatch or other logic to define batches
	batches, err := batchCalculator.CalculateBatches()
	if err != nil {
		return nil, fmt.Errorf("failed to calculate batches: %w", err)
	}
	fmt.Printf("Calculated %d batches for table %s based on source 1.\n", len(batches), v.TableName)
	return batches, nil
}

// processBatchesConcurrently calculates checksums, compares, saves results, and returns summaries.
func (v *ChecksumValidator) processBatchesConcurrently(srcInfo *metadata.TableInfo, batches []metadata.Batch) ([]BatchComparisonSummary, error) {
	var eg errgroup.Group
	eg.SetLimit(v.concurrency)
	resultsChan := make(chan BatchComparisonSummary, len(batches))
	summaries := make([]BatchComparisonSummary, 0, len(batches))
	columns := srcInfo.Columns
	srcProvider := metadata.NewDBConnProvider(v.SrcConfig)
	targetProvider := metadata.NewDBConnProvider(v.TargetConfig)

	for _, p := range batches {
		part := p // Capture loop variable
		pcc := batch.NewBatchChecksumCalculator(&part, srcProvider, targetProvider)
		eg.Go(func() error {
			var checksumInfo = pcc.CalculateAndCompareChecksum(columns)
			v.saveBatchResultToDB(part.Index, checksumInfo)

			resultsChan <- BatchComparisonSummary{
				Index:         part.Index,
				Match:         checksumInfo.IsOverallMatch(),
				ChecksumMatch: checksumInfo.IsChecksumMatch(),
				RowCountMatch: checksumInfo.IsRowCountMatch(),
				SourceErr:     checksumInfo.SrcErr,
				TargetErr:     checksumInfo.TargetErr,
			}
			return nil
		})
	}

	groupErr := eg.Wait()
	close(resultsChan)

	for summary := range resultsChan {
		summaries = append(summaries, summary)
	}
	log.Printf("Processed results for %d batches.", len(summaries))

	// Return summaries and the critical error from the group
	return summaries, groupErr
}

// saveBatchResultToDB saves the detailed batch results to the database if configured.
func (v *ChecksumValidator) saveBatchResultToDB(batchIndex int, checksumInfo *batch.BatchChecksumInfo) {
	if checksumInfo.SrcErr != nil || checksumInfo.TargetErr != nil {
		log.Printf("Batch %d encountered errors: Src=%v, Target=%v\n", batchIndex, checksumInfo.SrcErr, checksumInfo.TargetErr)
	}

	// Log mismatches immediately
	if !checksumInfo.IsRowCountMatch() {
		log.Printf("Batch %d: Row count mismatch (Src:%d, Tgt:%d)\n", batchIndex, checksumInfo.SrcRowCount, checksumInfo.TargetRowCount)
	} else if !checksumInfo.IsChecksumMatch() {
		log.Printf("Batch %d: Checksum mismatch (Src:%s, Tgt:%s)\n", batchIndex, checksumInfo.SrcChecksum, checksumInfo.TargetChecksum)
	}

	batchResultData := storage.BatchResultData{
		JobID:          v.jobID,
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
	dbErr := v.dbStore.SaveBatchResult(batchResultData)
	if dbErr != nil {
		log.Printf("ERROR: Failed to save batch %d result for job %d: %v\n", batchIndex, v.jobID, dbErr)
	}
}

// aggregateBatchResults processes summaries and updates the final comparison result.
func (v *ChecksumValidator) aggregateBatchResults(summaries []BatchComparisonSummary, comparisonResult *CompareChecksumResults) {
	mismatchedBatchesMap := make(map[int]bool)

	for _, summary := range summaries {
		if !summary.Match {
			comparisonResult.Match = false
			mismatchedBatchesMap[summary.Index] = true
			if !summary.RowCountMatch && summary.SourceErr == nil && summary.TargetErr == nil {
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

// printFinalSummary prints the validation outcome to the console.
func (v *ChecksumValidator) printFinalSummary(comparisonResult *CompareChecksumResults) {
	if comparisonResult.Match {
		fmt.Printf("Validation completed: Table %s checksums match.\n", v.TableName)
	} else {
		fmt.Printf("Validation completed: Table %s checksums DO NOT match.\n", v.TableName)
		if comparisonResult.RowCountMismatch {
			fmt.Printf("  Row count mismatch detected in one or more batches.\n")
		}
		if len(comparisonResult.MismatchBatches) > 0 {
			fmt.Printf("  Mismatched/Errored batches indices: %v\n", comparisonResult.MismatchBatches)
		}
		if comparisonResult.SrcError != nil || comparisonResult.TargetError != nil {
			fmt.Printf("  Errors occurred during batch processing: First Source Err=%v, First Target Err=%v\n", comparisonResult.SrcError, comparisonResult.TargetError)
		}
	}
}

// RunValidation performs the checksum validation and stores results in the DB if configured.
func (v *ChecksumValidator) RunValidation() (finalResult *CompareChecksumResults, finalError error) {
	fmt.Printf("Starting checksum job for table '%s' with batches of approximately %d rows...\n", v.TableName, v.RowsPerBatch)

	// Initialize result struct
	comparisonResult := &CompareChecksumResults{
		Match:            true, // Assume match initially
		TargetFailedRows: make(map[interface{}]string),
		MismatchBatches:  []int{}, // Initialize slice
	}

	// Defer the final status update
	defer func() {
		// The deferred call uses the final values of comparisonResult and finalError
		v.updateJobStatus(comparisonResult, finalError)
	}()

	// 1. Create Job Record (ignore error for checksum logic, but log)
	_ = v.createJobRecord()

	// 2. Setup Connections & Metadata
	srcConnProvider := metadata.NewDBConnProvider(v.SrcConfig)
	targetConnProvider := metadata.NewDBConnProvider(v.TargetConfig)
	srcInfo, targetInfo, err := v.setupMetadata(srcConnProvider, targetConnProvider)
	if err != nil {
		// If setup fails, comparisonResult might be partially populated by setup function
		// Need to ensure comparisonResult is updated correctly for early exit
		if srcInfo != nil {
			comparisonResult.SrcTotalRows = srcInfo.RowCount
		}
		if targetInfo != nil {
			comparisonResult.TargetTotalRows = targetInfo.RowCount
		}
		comparisonResult.Match = false
		finalError = fmt.Errorf("setup failed: %w", err)
		return comparisonResult, finalError // Return immediately on setup failure
	}

	// 3. Handle non-existent table case
	if !srcInfo.TableExists || !targetInfo.TableExists {
		fmt.Printf("Table %s does not exist on either source. Skipping validation.\n", v.TableName)
		comparisonResult.Match = true
		comparisonResult.SrcTotalRows = 0
		comparisonResult.TargetTotalRows = 0
		return comparisonResult, nil // Success, nothing to compare
	}

	// 5. Calculate batches
	batches, err := v.calculateBatches(srcInfo)
	if err != nil {
		comparisonResult.Match = false
		finalError = fmt.Errorf("batch calculation failed: %w", err)
		return comparisonResult, finalError
	}

	// 6. Process batches if any exist
	if len(batches) > 0 {
		batchSummaries, groupErr := v.processBatchesConcurrently(srcInfo, batches)

		// Aggregate results from batch processing
		v.aggregateBatchResults(batchSummaries, comparisonResult)

		if groupErr != nil {
			// A critical error occurred during concurrent processing
			log.Printf("Critical error during batch processing: %v", groupErr)
			comparisonResult.Match = false // Ensure mismatch on critical error
			if finalError == nil {         // Don't overwrite earlier errors
				finalError = groupErr
			}
		}
	} else {
		fmt.Println("Table appears to be empty or has no batches. Validation complete based on metadata row counts.")
	}

	// 7. Optional Row-by-Row verification (Placeholder)
	// TODO: Implement if needed

	// 8. Print Summary
	v.printFinalSummary(comparisonResult)

	// 9. Return results (defer handles DB update)
	finalResult = comparisonResult
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
