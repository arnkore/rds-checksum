package checksum

import (
	"context"
	"errors"
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/batch"
	"github.com/arnkore/rds-checksum/pkg/common"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/arnkore/rds-checksum/pkg/storage" // Added storage import
	"github.com/enriquebris/goconcurrentqueue"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus" // Changed from zerolog/log
	"golang.org/x/sync/errgroup"
	"sort"
	"sync"
	"sync/atomic"
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
	TableName               string
	RowsPerBatch            int // Target number of rows per batch/batch
	CalcCRC32InDB           bool
	DbStore                 *storage.Store // Added: Database store for results
	RetryQueue              *goconcurrentqueue.FIFO
	JobID                   int64 // Added: ID of the current job in the DB
	Concurrency             int
	SrcConnProvider         *metadata.DbConnProvider
	TargetConnProvider      *metadata.DbConnProvider
	SrcTableMetaProvider    *metadata.TableMetaProvider
	TargetTableMetaProvider *metadata.TableMetaProvider
}

// NewChecksumValidator creates a new validator.
func NewChecksumValidator(srcConfig, targetConfig *metadata.Config, tableName string, rowsPerBatch int, concurrency int, calcCrc32InDb bool, store *storage.Store) *ChecksumValidator {
	srcConnProvider := metadata.NewDBConnProvider(srcConfig)
	targetConnProvider := metadata.NewDBConnProvider(targetConfig)
	srcMetaProvider := metadata.NewTableMetaProvider(srcConnProvider, srcConfig.Database, tableName)
	targetMetaProvider := metadata.NewTableMetaProvider(targetConnProvider, targetConfig.Database, tableName)
	return &ChecksumValidator{
		TableName:               tableName,
		RowsPerBatch:            rowsPerBatch,
		CalcCRC32InDB:           calcCrc32InDb,
		DbStore:                 store, // Added
		RetryQueue:              goconcurrentqueue.NewFIFO(),
		JobID:                   0, // Initialized
		Concurrency:             concurrency,
		SrcConnProvider:         srcConnProvider,
		TargetConnProvider:      targetConnProvider,
		SrcTableMetaProvider:    srcMetaProvider,
		TargetTableMetaProvider: targetMetaProvider,
	}
}

func (v *ChecksumValidator) GetSrcTableMetaProvider() *metadata.TableMetaProvider {
	return v.SrcTableMetaProvider
}

// GetJobID returns the database job ID associated with this validation run.
func (cv *ChecksumValidator) GetJobID() int64 {
	return cv.JobID
}

// Represents the outcome of comparing a single batch
type BatchComparisonSummary struct {
	Index         int
	Match         bool // True if checksums and row counts match and no errors
	ChecksumMatch bool
	RowCountMatch bool
	RetryRowMap   map[int64]*RetryCheckRow
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

	// Use logrus for structured logging
	log.WithFields(log.Fields{
		"job_id":             cv.JobID,
		"status":             status,
		"match":              matchStatus,
		"source_rows":        srcRows,
		"target_rows":        tgtRows,
		"mismatched_batches": mismatchedCount,
		"error_message":      errMsg,
	}).Info("Attempting to update job status")
	// Convert enum back to string for the database layer
	err := cv.DbStore.UpdateJobCompletion(cv.JobID, string(status), matchStatus, srcRows, tgtRows, mismatchedCount, errMsg)
	if err != nil {
		log.WithField("job_id", cv.JobID).WithError(err).Error("Failed to update final job status")
	}
}

// createJobRecord creates a job record in the database.
func (cv *ChecksumValidator) createJobRecord() error {
	var err error
	cv.JobID, err = cv.DbStore.CreateJob(cv.TableName, cv.RowsPerBatch)
	return err
}

// setupMetadata establishes DB connections, fetches metadata, and performs initial checks.
func (cv *ChecksumValidator) setupMetadata() (*metadata.TableInfo, *metadata.TableInfo, error) {
	var srcInfo, targetInfo *metadata.TableInfo
	var srcMetaErr, targetMetaErr error
	// Get metadata concurrently
	var metaWg sync.WaitGroup
	metaWg.Add(2)
	go func() {
		defer metaWg.Done()
		srcInfo, srcMetaErr = cv.SrcTableMetaProvider.QueryTableInfo(cv.TableName)
	}()
	go func() {
		defer metaWg.Done()
		targetInfo, targetMetaErr = cv.TargetTableMetaProvider.QueryTableInfo(cv.TableName)
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
	log.WithFields(log.Fields{"count": len(batches), "table": cv.TableName}).Info("Calculated batches")
	return batches, nil
}

// processBatchesConcurrently calculates checksums, compares, saves results, and returns summaries.
func (cv *ChecksumValidator) processBatchesConcurrently(srcInfo *metadata.TableInfo, batches []metadata.Batch) ([]BatchComparisonSummary, error) {
	var eg errgroup.Group
	eg.SetLimit(cv.Concurrency)
	resultsChan := make(chan BatchComparisonSummary, len(batches))
	summaries := make([]BatchComparisonSummary, 0, len(batches))
	columns := srcInfo.Columns

	log.WithFields(log.Fields{"batch_count": len(batches), "Concurrency": cv.Concurrency}).Info("Starting concurrent batch processing")

	for _, p := range batches {
		part := p                                                                                             // Capture loop variable
		pcc := NewBatchChecksumCalculator(&part, cv.CalcCRC32InDB, cv.SrcConnProvider, cv.TargetConnProvider) // Assuming BatchChecksumCalculator doesn't need logger yet
		eg.Go(func() error {
			log.WithField("batch_index", part.Index).Debug("Starting batch processing")
			var checksumInfo = pcc.CalculateAndCompareChecksum(columns) // Assuming CalculateAndCompareChecksum doesn't need logger yet
			cv.saveBatchResultToDB(part.Index, checksumInfo)            // FIXME 这个地方不应该记录全部信息，应该记录summary信息。
			retryRowsMap := cv.buildRetryRowsMap(checksumInfo)
			batchSummary := BatchComparisonSummary{
				Index:         part.Index,
				Match:         checksumInfo.IsOverallMatch(),
				ChecksumMatch: checksumInfo.IsChecksumMatch(),
				RowCountMatch: checksumInfo.IsRowCountMatch(),
				RetryRowMap:   retryRowsMap,
				SourceErr:     checksumInfo.SrcErr,
				TargetErr:     checksumInfo.TargetErr,
			}
			resultsChan <- batchSummary
			if !batchSummary.ChecksumMatch {
				cv.RetryQueue.Enqueue(retryRowsMap)
			}

			log.WithField("batch_index", part.Index).Info("Finished batch processing")
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

	log.WithField("processed_count", len(summaries)).Info("Finished processing all batches")
	if groupErr != nil {
		log.WithError(groupErr).Error("Error occurred during concurrent batch processing")
	}

	return summaries, groupErr
}

func (cv *ChecksumValidator) buildRetryRowsMap(checksumInfo *BatchChecksumInfo) map[int64]*RetryCheckRow {
	inconsistentPKeys := checksumInfo.CompareChecksumMap()
	retryRowsMap := make(map[int64]*RetryCheckRow, len(inconsistentPKeys))
	for _, pkey := range inconsistentPKeys {
		retryRowsMap[pkey] = &RetryCheckRow{
			PKey:           pkey,
			RetryTimes:     &atomic.Int32{},
			lastCheckTime:  checksumInfo.SrcCheckTime,
			firstCheckTime: checksumInfo.SrcCheckTime,
		}
	}
	return retryRowsMap
}

// saveBatchResultToDB saves the detailed batch results to the database if configured.
func (cv *ChecksumValidator) saveBatchResultToDB(batchIndex int, checksumInfo *BatchChecksumInfo) {
	// Log errors regardless of DB storage
	if checksumInfo.SrcErr != nil {
		log.WithField("batch_index", batchIndex).WithError(checksumInfo.SrcErr).Error("Processing source batch error")
	}
	if checksumInfo.TargetErr != nil {
		log.WithField("batch_index", batchIndex).WithError(checksumInfo.TargetErr).Error("Processing target batch error")
	}

	// Log mismatches immediately
	if !checksumInfo.IsRowCountMatch() {
		log.WithFields(log.Fields{
			"batch_index": batchIndex,
			"source_rows": checksumInfo.SrcRowCount,
			"target_rows": checksumInfo.TargetRowCount,
		}).Info("Batch row count mismatch")
	} else if !checksumInfo.IsChecksumMatch() {
		// Only log checksum mismatch if row counts matched (otherwise it's implied)
		log.WithFields(log.Fields{
			"batch_index":     batchIndex,
			"source_checksum": checksumInfo.SrcChecksum,
			"target_checksum": checksumInfo.TargetChecksum,
		}).Info("Batch checksum mismatch")
	}

	// Save to DB if configured and job created
	if cv.DbStore != nil && cv.JobID > 0 {
		// ... (rest of the saveBatchResultToDB logic, converting existing log.Printf if any to cv.logger) ...
		// Example: If there were internal logs here, convert them.
		batchResultData := storage.BatchResultData{
			JobID:          cv.JobID,
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
		dbErr := cv.DbStore.SaveBatchResult(batchResultData)

		if dbErr != nil {
			log.WithFields(log.Fields{"batch_index": batchIndex, "job_id": cv.JobID}).WithError(dbErr).Error("Failed to save batch result to database")
		}
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
			log.WithField("job_id", cv.JobID).WithError(finalError).Error("Validation run finished with error")
		} else if finalResult != nil && !finalResult.Match {
			log.WithFields(log.Fields{"job_id": cv.JobID, "mismatched_batches": len(finalResult.MismatchBatches)}).
				Error("Validation run finished with mismatches")
		} else {
			log.WithFields(log.Fields{"job_id": cv.JobID, "match": finalResult.Match}).Info("Validation run finished successfully")
		}
	}()

	// Create job record first
	err := cv.createJobRecord()
	if err != nil {
		log.WithError(err).Error("Failed to create checksum job in database. Proceeding without DB logging.")
	}

	// --- Metadata Setup ---
	srcInfo, targetInfo, metaErr := cv.setupMetadata()
	if metaErr != nil {
		log.WithError(metaErr).Error("Metadata setup failed")
		finalError = fmt.Errorf("metadata setup failed: %w", metaErr)
		return finalResult, finalError // Return immediately on critical setup error
	}
	finalResult.SrcTotalRows = srcInfo.RowCount
	finalResult.TargetTotalRows = targetInfo.RowCount

	// Further checks could be added here, e.g., ensuring PK is numeric for range partitioning.

	// --- Batch Calculation ---
	batches, batchErr := cv.calculateBatches(srcInfo)
	if batchErr != nil {
		log.WithError(batchErr).Error("Failed to calculate batches")
		finalError = fmt.Errorf("batch calculation failed: %w", batchErr) // Keep fmt for error wrapping
		return finalResult, finalError                                    // Return immediately
	}
	if len(batches) == 0 {
		log.WithField("table", cv.TableName).Warn("No batches calculated, possibly an empty table or configuration issue.")
		// Consider this a success (empty tables match), or handle as needed.
		finalResult.Match = true // Explicitly set match for empty table case
		return finalResult, nil  // Nothing more to do
	}

	// --- Process Batches ---
	batchSummaries, processErr := cv.processBatchesConcurrently(srcInfo, batches)
	if processErr != nil {
		finalError = fmt.Errorf("concurrent batch processing failed: %w", processErr)
		log.Warn("Aggregating potentially incomplete batch results due to processing error")
	}

	// --- Aggregate Results ---
	log.WithField("count", len(batchSummaries)).Info("Aggregating batch results")
	cv.aggregateBatchResults(batchSummaries, finalResult)
	return finalResult, finalError
}

func (cv *ChecksumValidator) RetryRowChecksum(cancelFunc context.CancelFunc, firstRunChecksumFinished, normalCancelFlag *atomic.Bool, srcTableInfo *metadata.TableInfo) {
	firstRunFinished := firstRunChecksumFinished.Load()
	_retryRowMap, err := cv.RetryQueue.Dequeue()
	if err != nil {
		if isQueueEmpty(err) {
			if firstRunFinished {
				log.Info("RetryQueue is empty and first run finished, stopping retry process.")
				NormalCancel(cancelFunc, normalCancelFlag)
			}
		} else if cv.RetryQueue.IsLocked() {
			log.WithError(err).Error("RetryQueue is locked, stopping retry process.")
			AbnormalCancel(cancelFunc, normalCancelFlag)
		}
		return
	}

	retryRowMap, ok := _retryRowMap.(map[int64]*RetryCheckRow)
	if !ok {
		log.WithField("dequeued_item", _retryRowMap).Error("Dequeued item is not of expected type")
		AbnormalCancel(cancelFunc, normalCancelFlag)
		return // Skip processing invalid item
	}

	log.WithField("retry_rows_count", len(retryRowMap)).Info("Dequeued rows for retry check")
	// TODO 待重试的batch中失败的row可能比较少，尝试合并之
	// FIXME checksum中间发生了DDL，需要直接失败，怎么检测呢？

	rowChecksum := NewRowChecksum(retryRowMap, srcTableInfo, cv.SrcConnProvider, cv.TargetConnProvider, cv.CalcCRC32InDB)
	newRetryRowMap, checksumErr := rowChecksum.CalculateAndCompareChecksum(srcTableInfo.Columns)
	if checksumErr != nil {
		log.WithField("initial_row_count", len(retryRowMap)).WithError(checksumErr).
			Error("Error during row checksum calculation/comparison")
	}

	if len(newRetryRowMap) > 0 {
		log.WithField("retry_rows_count", len(retryRowMap)).
			WithField("new_retry_count", len(newRetryRowMap)).
			Warn("Some rows still mismatched after retry, re-enqueuing")

		if retryCheck(newRetryRowMap) { // 达到最大重试次数，退出重试任务。
			log.Warn("Some rows are still mismatched after retry 2 times, stopping retry process.")
			AbnormalCancel(cancelFunc, normalCancelFlag)
			return
		}
		enqueueErr := cv.RetryQueue.Enqueue(newRetryRowMap)
		if enqueueErr != nil {
			log.WithField("row_count", len(newRetryRowMap)).WithError(enqueueErr).
				Error("Failed to re-enqueue rows for retry")
			AbnormalCancel(cancelFunc, normalCancelFlag)
		}
	} else if checksumErr == nil { // Only log success if there wasn't a calculation error
		log.WithField("checked_count", len(retryRowMap)).
			Info("Successfully retried and verified rows, no mismatches found in this batch.")
	}
}

func retryCheck(retryMap map[int64]*RetryCheckRow) bool {
	for _, v := range retryMap {
		if v.RetryTimes.Load() >= common.ROW_CHECKSUM_MAX_RETRY_TIMES {
			return true
		}
	}
	return false
}

func NormalCancel(cancelFunc context.CancelFunc, normalCancelFlag *atomic.Bool) {
	cancelFunc()
	normalCancelFlag.Store(true)
}

func AbnormalCancel(cancelFunc context.CancelFunc, normalCancelFlag *atomic.Bool) {
	cancelFunc()
	normalCancelFlag.Store(false)
}

func isQueueEmpty(err error) bool {
	if err != nil {
		var queryErr *goconcurrentqueue.QueueError
		if errors.As(err, &queryErr) && queryErr.Code() == goconcurrentqueue.QueueErrorCodeEmptyQueue {
			return true
		}
	}

	return false
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
