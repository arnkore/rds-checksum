package checksum

import (
	"database/sql"
	"fmt"
	"log" // Added for logging DB errors
	"runtime" // For default concurrency
	"sort"
	"strings"
	"sync"

	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/arnkore/rds-checksum/pkg/partition"
	"github.com/arnkore/rds-checksum/pkg/storage" // Added storage import

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

// ChecksumResult holds the result of a checksum calculation.
type ChecksumResult struct {
	Count    int64
	Checksum string
}

// PartitionChecksumResult holds checksum results for a single partition.
type PartitionChecksumResult struct {
	Partition metadata.Partition
	Checksum  string
	RowCount  int64
	Error     error
}

// RowData holds data for a single row.
type RowData struct {
	PK    interface{} // Primary Key value
	Hash  string      // Hash of the row data
	Error error
}

// CompareChecksumResults compares checksum results from two sources (e.g., master and replica).
type CompareChecksumResults struct {
	Match              bool
	MismatchPartitions []int // Indices of partitions that don't match
	RowCountMismatch   bool
	SrcTotalRows       int64
	TargetTotalRows    int64
	TargetFailedRows   map[interface{}]string // PK -> Hash/Error for rows that failed comparison on Source 2
	SrcError           error                  // Errors during source processing (e.g., fetching rows)
	TargetError        error                  // Errors during target processing
}

// UnifiedChecksumValidator orchestrates the checksum validation process.
type UnifiedChecksumValidator struct {
	SrcConfig    metadata.Config // Source Config (e.g., Master)
	TargetConfig metadata.Config // Target Config (e.g., Replica)
	TableName    string
	RowsPerBatch int            // Target number of rows per batch/partition
	dbStore      *storage.Store // Added: Database store for results
	jobID        int64          // Added: ID of the current job in the DB
}

// NewUnifiedChecksumValidator creates a new validator.
func NewUnifiedChecksumValidator(src, target metadata.Config, tableName string, rowsPerBatch int, store *storage.Store) *UnifiedChecksumValidator {
	return &UnifiedChecksumValidator{
		SrcConfig:    src,
		TargetConfig: target,
		TableName:    tableName,
		RowsPerBatch: rowsPerBatch,
		dbStore:      store, // Added
		jobID:        0,     // Initialized
	}
}

// GetJobID returns the database job ID associated with this validation run.
func (v *UnifiedChecksumValidator) GetJobID() int64 {
	return v.jobID
}

// Helper function to safely convert error to string for DB storage
func errorToString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Helper function to create a simplified connection info string (masking password)
func getSimplifiedConnInfo(config *metadata.Config) string {
	// Rudimentary parsing, assumes DSN format user:pass@tcp(host:port)/db
	// This should be made more robust in a real application
	dsn := config.DSN
	user := ""
	passEnd := -1
	if idx := strings.Index(dsn, ":"); idx != -1 {
		user = dsn[:idx]
		passEnd = strings.Index(dsn, "@")
	}
	start := 0
	if passEnd != -1 {
		start = passEnd + 1
	}
	info := dsn[start:]
	return fmt.Sprintf("%s@%s", user, info)
}

// Represents the outcome of comparing a single partition
type PartitionComparisonSummary struct {
	Index           int
	Match           bool // True if checksums and row counts match and no errors
	ChecksumMatch   bool
	RowCountMatch   bool
	SourceErr       error
	TargetErr       error
}

// updateJobStatus updates the job status in the database.
func (v *UnifiedChecksumValidator) updateJobStatus(comparisonResult *CompareChecksumResults, finalError error) {
	if v.dbStore != nil && v.jobID > 0 {
		status := "completed"
		errMsg := ""
		mismatchedCount := 0

		// Ensure comparisonResult is not nil before accessing
		if comparisonResult != nil {
			if !comparisonResult.Match {
				status = "completed_mismatch"
			}
			mismatchedCount = len(comparisonResult.MismatchPartitions)
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
func (v *UnifiedChecksumValidator) createJobRecord() error {
	if v.dbStore != nil {
		sourceInfo := getSimplifiedConnInfo(&v.SrcConfig)
		targetInfo := getSimplifiedConnInfo(&v.TargetConfig)
		jobID, err := v.dbStore.CreateJob(v.TableName, sourceInfo, targetInfo, v.RowsPerBatch)
		if err != nil {
			log.Printf("ERROR: Failed to create checksum job in database: %v. Proceeding without DB logging.\n", err)
			v.dbStore = nil // Disable DB store if creation fails
			return nil      // Don't treat failure to create job as fatal for checksum itself
		}
		v.jobID = jobID
		fmt.Printf("Created database job with ID: %d\n", v.jobID)
		err = v.dbStore.UpdateJobStatus(v.jobID, "running", "")
		if err != nil {
			// Log error but continue, status update isn't critical for checksum logic
			log.Printf("ERROR: Failed to update job %d status to running: %v\n", v.jobID, err)
		}
	}
	return nil
}

// setupConnectionsAndMetadata establishes DB connections, fetches metadata, and performs initial checks.
func (v *UnifiedChecksumValidator) setupConnectionsAndMetadata() (*metadata.DBConnProvider, *metadata.DBConnProvider, *metadata.TableInfo, *metadata.TableInfo, error) {
	var srcInfo, targetInfo *metadata.TableInfo
	var srcMetaErr, targetMetaErr error

	srcProvider, srcMetaErr := metadata.NewDBConnProvider(&v.SrcConfig)
	if srcMetaErr != nil {
		return nil, nil, nil, nil, fmt.Errorf("source provider init failed: %w", srcMetaErr)
	}

	targetProvider, targetMetaErr := metadata.NewDBConnProvider(&v.TargetConfig)
	if targetMetaErr != nil {
		srcProvider.Close() // Close source provider if target fails
		return nil, nil, nil, nil, fmt.Errorf("target provider init failed: %w", targetMetaErr)
	}

	// Get metadata concurrently
	var wgMeta sync.WaitGroup
	wgMeta.Add(2)
	go func() {
		defer wgMeta.Done()
		srcMetaProvider := metadata.NewTableMetaProvider(srcProvider, v.TableName)
		srcInfo, srcMetaErr = srcMetaProvider.GetTableInfo(v.TableName)
	}()
	go func() {
		defer wgMeta.Done()
		targetMetaProvider := metadata.NewTableMetaProvider(targetProvider, v.TableName)
		targetInfo, targetMetaErr = targetMetaProvider.GetTableInfo(v.TableName)
	}()
	wgMeta.Wait()

	if srcMetaErr != nil {
		srcProvider.Close()
		targetProvider.Close()
		return nil, nil, nil, nil, fmt.Errorf("failed getting metadata from source 1: %w", srcMetaErr)
	}
	if targetMetaErr != nil {
		srcProvider.Close()
		targetProvider.Close()
		return nil, nil, nil, nil, fmt.Errorf("failed getting metadata from source 2: %w", targetMetaErr)
	}

	// Check for table existence mismatch
	if !srcInfo.TableExists || !targetInfo.TableExists {
		if srcInfo.TableExists != targetInfo.TableExists {
			srcProvider.Close()
			targetProvider.Close()
			return nil, nil, srcInfo, targetInfo, fmt.Errorf("table existence mismatch: source exists=%t, target exists=%t", srcInfo.TableExists, targetInfo.TableExists)
		}
		// If neither exists, it's handled later as a success case
	}

	// Check for column definition mismatch
	if srcInfo.TableExists && targetInfo.TableExists && !equalStringSlices(srcInfo.Columns, targetInfo.Columns) {
		srcProvider.Close()
		targetProvider.Close()
		return nil, nil, srcInfo, targetInfo, fmt.Errorf("column definition mismatch for table %s", v.TableName)
	}

	return srcProvider, targetProvider, srcInfo, targetInfo, nil
}

// calculatePartitions determines the partitions for checksumming based on source table info.
func (v *UnifiedChecksumValidator) calculatePartitions(srcInfo *metadata.TableInfo) ([]metadata.Partition, error) {
	// Ensure the partition calculator uses RowsPerBatch if intended, or adjust constructor call.
	partitionCalculator, err := partition.NewPartitionCalculator(srcInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition calculator: %w", err)
	}
	// Assuming calculator internally uses RowsPerBatch or other logic to define partitions
	partitions, err := partitionCalculator.CalculatePartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to calculate partitions: %w", err)
	}
	fmt.Printf("Calculated %d partitions for table %s based on source 1.\n", len(partitions), v.TableName)
	return partitions, nil
}

// processPartitionsConcurrently calculates checksums, compares, saves results, and returns summaries.
func (v *UnifiedChecksumValidator) processPartitionsConcurrently(
	srcProvider *metadata.DBConnProvider,
	targetProvider *metadata.DBConnProvider,
	srcInfo *metadata.TableInfo,
	targetInfo *metadata.TableInfo,
	partitions []metadata.Partition,
) ([]PartitionComparisonSummary, error) {

	var eg errgroup.Group
	concurrencyLimit := runtime.NumCPU() * 2
	if concurrencyLimit < 2 { concurrencyLimit = 2 }
	if concurrencyLimit > 16 { concurrencyLimit = 16 }
	eg.SetLimit(concurrencyLimit)
	fmt.Printf("Processing %d partitions with concurrency limit %d...\n", len(partitions), concurrencyLimit)

	resultsChan := make(chan PartitionComparisonSummary, len(partitions))
	summaries := make([]PartitionComparisonSummary, 0, len(partitions))

	srcDB := srcProvider.GetDbConn()
	targetDB := targetProvider.GetDbConn()
	columns := srcInfo.Columns

	for _, p := range partitions {
		part := p // Capture loop variable

		eg.Go(func() error {
			srcPcc := NewPartitionChecksumCalculator(&part)
			targetPcc := NewPartitionChecksumCalculator(&part)

			var srcChecksum, targetChecksum string
			var srcRowCount, targetRowCount int64
			var srcErr, targetErr error
			var wgPart sync.WaitGroup
			wgPart.Add(2)

			go func() {
				defer wgPart.Done()
				srcChecksum, srcRowCount, srcErr = srcPcc.CalculateChecksum(srcDB, columns)
			}()
			go func() {
				defer wgPart.Done()
				targetChecksum, targetRowCount, targetErr = targetPcc.CalculateChecksum(targetDB, columns)
			}()
			wgPart.Wait()

			checksumMatch, rowCountMatch, partitionOverallMatch := v.comparePartitionResults(part.Index, srcChecksum, targetChecksum, srcRowCount, targetRowCount, srcErr, targetErr)

			v.savePartitionResultToDB(part.Index, srcChecksum, targetChecksum, srcRowCount, targetRowCount, checksumMatch, rowCountMatch, srcErr, targetErr)

			resultsChan <- PartitionComparisonSummary{
				Index:         part.Index,
				Match:         partitionOverallMatch,
				ChecksumMatch: checksumMatch,
				RowCountMatch: rowCountMatch,
				SourceErr:     srcErr,
				TargetErr:     targetErr,
			}
			return nil
		})
	}

	groupErr := eg.Wait()
	close(resultsChan)

	for summary := range resultsChan {
		summaries = append(summaries, summary)
	}
	log.Printf("Processed results for %d partitions.", len(summaries))

	// Return summaries and the critical error from the group
	return summaries, groupErr
}

// comparePartitionResults performs the comparison logic for a single partition's results.
func (v *UnifiedChecksumValidator) comparePartitionResults(partitionIndex int, srcChecksum, targetChecksum string, srcRowCount, targetRowCount int64, srcErr, targetErr error) (checksumMatch, rowCountMatch, overallMatch bool) {
	checksumMatch = false
	rowCountMatch = false
	overallMatch = false

	if srcErr == nil && targetErr == nil {
		rowCountMatch = (srcRowCount == targetRowCount)
		if rowCountMatch {
			checksumMatch = (srcChecksum == targetChecksum)
			if checksumMatch {
				overallMatch = true
			}
		}
		// Log mismatches immediately
		if !rowCountMatch {
			log.Printf("Partition %d: Row count mismatch (Src:%d, Tgt:%d)\n", partitionIndex, srcRowCount, targetRowCount)
		} else if !checksumMatch {
			log.Printf("Partition %d: Checksum mismatch (Src:%s, Tgt:%s)\n", partitionIndex, srcChecksum, targetChecksum)
		}
	} else {
		log.Printf("Partition %d encountered errors: Src=%v, Target=%v\n", partitionIndex, srcErr, targetErr)
	}
	return checksumMatch, rowCountMatch, overallMatch
}

// savePartitionResultToDB saves the detailed partition results to the database if configured.
func (v *UnifiedChecksumValidator) savePartitionResultToDB(partitionIndex int, srcChecksum, targetChecksum string, srcRowCount, targetRowCount int64, checksumMatch, rowCountMatch bool, srcErr, targetErr error) {
	if v.dbStore != nil && v.jobID > 0 {
		partitionData := storage.PartitionResultData{
			JobID:          v.jobID,
			PartitionIndex: partitionIndex,
			SourceChecksum: srcChecksum,
			TargetChecksum: targetChecksum,
			SourceRowCount: srcRowCount,
			TargetRowCount: targetRowCount,
			ChecksumMatch:  checksumMatch,
			RowCountMatch:  rowCountMatch,
			SourceError:    errorToString(srcErr),
			TargetError:    errorToString(targetErr),
		}
		dbErr := v.dbStore.SavePartitionResult(partitionData)
		if dbErr != nil {
			log.Printf("ERROR: Failed to save partition %d result for job %d: %v\n", partitionIndex, v.jobID, dbErr)
		}
	}
}

// aggregatePartitionResults processes summaries and updates the final comparison result.
func (v *UnifiedChecksumValidator) aggregatePartitionResults(summaries []PartitionComparisonSummary, comparisonResult *CompareChecksumResults) {
	mismatchedPartitionsMap := make(map[int]bool)

	for _, summary := range summaries {
		if !summary.Match {
			comparisonResult.Match = false
			mismatchedPartitionsMap[summary.Index] = true
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

	// Populate MismatchPartitions list from the map
	comparisonResult.MismatchPartitions = make([]int, 0, len(mismatchedPartitionsMap))
	for idx := range mismatchedPartitionsMap {
		comparisonResult.MismatchPartitions = append(comparisonResult.MismatchPartitions, idx)
	}
	sort.Ints(comparisonResult.MismatchPartitions)
}

// printFinalSummary prints the validation outcome to the console.
func (v *UnifiedChecksumValidator) printFinalSummary(comparisonResult *CompareChecksumResults) {
	if comparisonResult.Match {
		fmt.Printf("Validation completed: Table %s checksums match.\n", v.TableName)
	} else {
		fmt.Printf("Validation completed: Table %s checksums DO NOT match.\n", v.TableName)
		if comparisonResult.RowCountMismatch {
			fmt.Printf("  Row count mismatch detected in one or more partitions.\n")
		}
		if len(comparisonResult.MismatchPartitions) > 0 {
			fmt.Printf("  Mismatched/Errored partitions indices: %v\n", comparisonResult.MismatchPartitions)
		}
		if comparisonResult.SrcError != nil || comparisonResult.TargetError != nil {
			fmt.Printf("  Errors occurred during partition processing: First Source Err=%v, First Target Err=%v\n", comparisonResult.SrcError, comparisonResult.TargetError)
		}
	}
}

// RunValidation performs the checksum validation and stores results in the DB if configured.
func (v *UnifiedChecksumValidator) RunValidation() (finalResult *CompareChecksumResults, finalError error) {
	fmt.Printf("Starting unified checksum validation for table %s...\n", v.TableName)

	// Initialize result struct
	comparisonResult := &CompareChecksumResults{
		Match:            true, // Assume match initially
		TargetFailedRows: make(map[interface{}]string),
		MismatchPartitions: []int{}, // Initialize slice
	}

	// Defer the final status update
	defer func() {
		// The deferred call uses the final values of comparisonResult and finalError
		v.updateJobStatus(comparisonResult, finalError)
	}()

	// 1. Create Job Record (ignore error for checksum logic, but log)
	_ = v.createJobRecord()

	// 2. Setup Connections & Metadata
	srcProvider, targetProvider, srcInfo, targetInfo, err := v.setupConnectionsAndMetadata()
	if err != nil {
		// If setup fails, comparisonResult might be partially populated by setup function
		// Need to ensure comparisonResult is updated correctly for early exit
		if srcInfo != nil { comparisonResult.SrcTotalRows = srcInfo.RowCount }
		if targetInfo != nil { comparisonResult.TargetTotalRows = targetInfo.RowCount }
		comparisonResult.Match = false 
		finalError = fmt.Errorf("setup failed: %w", err)
		return comparisonResult, finalError // Return immediately on setup failure
	}
	defer srcProvider.Close()
	defer targetProvider.Close()

	// 3. Handle non-existent table case
	if !srcInfo.TableExists {
		fmt.Printf("Table %s does not exist on either source. Skipping validation.\n", v.TableName)
		comparisonResult.Match = true
		comparisonResult.SrcTotalRows = 0
		comparisonResult.TargetTotalRows = 0
		return comparisonResult, nil // Success, nothing to compare
	}

	// 4. Populate initial comparison results from metadata
	comparisonResult.SrcTotalRows = srcInfo.RowCount
	comparisonResult.TargetTotalRows = targetInfo.RowCount
	if comparisonResult.SrcTotalRows != comparisonResult.TargetTotalRows {
		fmt.Printf("Overall row count mismatch detected (from metadata): Source=%d, Target=%d\n", comparisonResult.SrcTotalRows, comparisonResult.TargetTotalRows)
		comparisonResult.RowCountMismatch = true
		comparisonResult.Match = false
	}

	// 5. Calculate Partitions
	partitions, err := v.calculatePartitions(srcInfo)
	if err != nil {
		comparisonResult.Match = false
		finalError = fmt.Errorf("partition calculation failed: %w", err)
		return comparisonResult, finalError
	}

	// 6. Process Partitions if any exist
	if len(partitions) > 0 {
		partitionSummaries, groupErr := v.processPartitionsConcurrently(srcProvider, targetProvider, srcInfo, targetInfo, partitions)
		
		// Aggregate results from partition processing
		v.aggregatePartitionResults(partitionSummaries, comparisonResult)

		if groupErr != nil {
			// A critical error occurred during concurrent processing
			log.Printf("Critical error during partition processing: %v", groupErr)
			comparisonResult.Match = false // Ensure mismatch on critical error
			if finalError == nil { // Don't overwrite earlier errors
				finalError = groupErr
			}
		}
	} else {
		fmt.Println("Table appears to be empty or has no partitions. Validation complete based on metadata row counts.")
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
