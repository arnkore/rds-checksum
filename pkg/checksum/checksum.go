package checksum

import (
	"database/sql"
	"fmt"
	"mychecksum/pkg/metadata"
	"mychecksum/pkg/partition"
	"sort"
	"sync"

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
	SrcConfig     metadata.Config // Source Config (e.g., Master)
	TargetConfig  metadata.Config // Target Config (e.g., Replica)
	TableName     string
	NumPartitions int // Number of partitions to use
}

// NewUnifiedChecksumValidator creates a new validator.
func NewUnifiedChecksumValidator(src, target metadata.Config, tableName string, numPartitions int) *UnifiedChecksumValidator {
	return &UnifiedChecksumValidator{
		SrcConfig:     src,
		TargetConfig:  target,
		TableName:     tableName,
		NumPartitions: numPartitions,
	}
}

// CalculateChecksumInPartitions calculates checksums for all partitions in parallel.
func (c *UnifiedChecksumValidator) CalculateChecksumInPartitions(dbConn *sql.DB, tableInfo *metadata.TableInfo, partitions []metadata.Partition) (map[int]PartitionChecksumResult, error) {
	if tableInfo == nil || !tableInfo.TableExists {
		return nil, fmt.Errorf("table %s does not exist or info not loaded", tableInfo)
	}
	resultsChan := make(chan PartitionChecksumResult, len(partitions))
	var eg errgroup.Group
	columns := tableInfo.Columns

	for _, p := range partitions {
		part := p // Capture loop variable for goroutine
		pcc := NewPartitionChecksumCalculator(&part)
		eg.Go(func() error {
			checksum, rowCount, err := pcc.CalculateChecksum(dbConn, columns)
			resultsChan <- PartitionChecksumResult{
				Partition: part,
				Checksum:  checksum,
				RowCount:  rowCount,
				Error:     err, // Pass error along in the result struct
			}
			// Only return non-nil error to errgroup if it's critical and should stop everything
			// For checksum, often we want all results even if some fail, so return nil here.
			// The error is handled when collecting results.
			return err // If we want to stop on first error, return err here. Otherwise return nil.
		})
	}

	// Wait for all goroutines to complete. errgroup.Wait() returns the first non-nil error.
	waitErr := eg.Wait() // Capture potential error from the errgroup

	close(resultsChan) // Close channel once all goroutines are done

	// Collect results
	results := make(map[int]PartitionChecksumResult)
	var totalRowCount int64 = 0
	var firstError = waitErr // Initialize with potential error from Wait()

	for result := range resultsChan {
		if result.Error != nil && firstError == nil {
			firstError = result.Error // Capture the first error encountered
		}
		results[result.Partition.Index] = result
		totalRowCount += result.RowCount
	}

	// If an error occurred in any goroutine (captured by firstError), return it.
	if firstError != nil {
		return results, fmt.Errorf("error during partition checksum calculation: %w", firstError)
	}

	return results, nil
}

// RunValidation performs the checksum validation.
func (v *UnifiedChecksumValidator) RunValidation() (*CompareChecksumResults, error) {
	fmt.Printf("Starting unified checksum validation for table %s...\n", v.TableName)
	comparisonResult := &CompareChecksumResults{
		TargetFailedRows: make(map[interface{}]string),
	}

	// 1. Get Table Metadata for both sources concurrently
	var sourceTableInfo, targetTableInfo *metadata.TableInfo
	var srcMetaErr, targetMetaErr error
	srcDbConnProvider, srcMetaErr := metadata.NewDBConnProvider(&v.SrcConfig)
	if srcMetaErr != nil {
		srcMetaErr = fmt.Errorf("source1 meta provider: %w", srcMetaErr)
		return comparisonResult, srcMetaErr
	}
	targetDbConnProvider, targetMetaErr := metadata.NewDBConnProvider(&v.TargetConfig)
	if targetMetaErr != nil {
		targetMetaErr = fmt.Errorf("source1 meta provider: %w", targetMetaErr)
		return comparisonResult, targetMetaErr
	}

	var wgMeta sync.WaitGroup
	wgMeta.Add(2)
	go func() {
		defer wgMeta.Done()
		srcTableMetaProvider := metadata.NewTableMetaProvider(srcDbConnProvider, v.TableName)
		sourceTableInfo, srcMetaErr = srcTableMetaProvider.GetTableInfo(v.TableName)
	}()
	go func() {
		defer wgMeta.Done()
		targetTableMetaProvider := metadata.NewTableMetaProvider(targetDbConnProvider, v.TableName)
		targetTableInfo, targetMetaErr = targetTableMetaProvider.GetTableInfo(v.TableName)
	}()
	wgMeta.Wait()

	if srcMetaErr != nil {
		return nil, fmt.Errorf("failed getting metadata from source 1: %w", srcMetaErr)
	}
	if targetMetaErr != nil {
		return nil, fmt.Errorf("failed getting metadata from source 2: %w", targetMetaErr)
	}

	if !sourceTableInfo.TableExists || !targetTableInfo.TableExists {
		// Handle cases where table exists on one source but not the other
		if sourceTableInfo.TableExists != targetTableInfo.TableExists {
			comparisonResult.Match = false
			comparisonResult.RowCountMismatch = true // Consider existence difference as a row count mismatch
			// Provide more specific info about existence
			return comparisonResult, fmt.Errorf("table existence mismatch: source1 exists=%t, source2 exists=%t", sourceTableInfo.TableExists, targetTableInfo.TableExists)
		}
		// If table doesn't exist on either, perhaps return success or a specific status
		if !sourceTableInfo.TableExists {
			fmt.Printf("Table %s does not exist on either source. Skipping validation.\n", v.TableName)
			comparisonResult.Match = true // Or define a specific state for non-existent tables
			return comparisonResult, nil
		}
	}

	// Basic checks: Row counts and column definitions (optional but recommended)
	comparisonResult.SrcTotalRows = sourceTableInfo.RowCount
	comparisonResult.TargetTotalRows = targetTableInfo.RowCount
	// Optional: Compare column list/order
	if !equalStringSlices(sourceTableInfo.Columns, targetTableInfo.Columns) {
		return nil, fmt.Errorf("column definition mismatch between sources for table %s", v.TableName)
	}
	// Ensure a PK exists if partitioning is intended
	if v.NumPartitions > 1 && (sourceTableInfo.PrimaryKey == "" || targetTableInfo.PrimaryKey == "") {
		return nil, fmt.Errorf("cannot use partitioning for table %s as primary key is missing on one or both sources", v.TableName)
	}

	// 2. Calculate Partitions (based on Source 1 metadata, assuming structure compatibility)
	// Need a temporary DB connection for partition calculation
	partitionCalculator, err := partition.NewPartitionCalculator(sourceTableInfo)
	var partitions []metadata.Partition
	if partitionCalculator != nil { // Only calculate if calculator created and partitions requested
		partitions, err = partitionCalculator.CalculatePartitions()
		if err != nil {
			return nil, fmt.Errorf("failed to calculate partitions (source 1): %w", err)
		}
		fmt.Printf("Calculated %d partitions for table %s based on source 1.\n", len(partitions), v.TableName)
	}

	// 3. Calculate Checksums for Partitions on both sources concurrently
	var results1, results2 map[int]PartitionChecksumResult
	var checksumErr1, checksumErr2 error
	var wgChecksum sync.WaitGroup
	wgChecksum.Add(2)

	go func() {
		defer wgChecksum.Done()
		results1, checksumErr1 = v.CalculateChecksumInPartitions(srcDbConnProvider.GetDbConn(), sourceTableInfo, partitions)
	}()
	go func() {
		defer wgChecksum.Done()
		// Use the *same* partition definitions derived from source 1
		results2, checksumErr2 = v.CalculateChecksumInPartitions(targetDbConnProvider.GetDbConn(), targetTableInfo, partitions)
	}()
	wgChecksum.Wait()

	if checksumErr1 != nil {
		return nil, fmt.Errorf("error calculating checksums on source 1: %w", checksumErr1)
	}
	if checksumErr2 != nil {
		return nil, fmt.Errorf("error calculating checksums on source 2: %w", checksumErr2)
	}

	// Check if number of results matches expected number of partitions
	if len(results1) != len(partitions) || len(results2) != len(partitions) {
		// This might indicate partial failures or inconsistencies
		return nil, fmt.Errorf("partition result count mismatch: expected %d, source1 got %d, source2 got %d", len(partitions), len(results1), len(results2))
	}

	// 4. Compare Partition Checksums
	comparisonResult.Match = true                 // Assume match until proven otherwise
	mismatchedPartitionsMap := make(map[int]bool) // Track indices of mismatched partitions

	for i := 1; i <= len(partitions); i++ { // Iterate based on expected partition indices
		res1, ok1 := results1[i]
		res2, ok2 := results2[i]

		if !ok1 || !ok2 {
			// Should not happen if previous checks passed, but handle defensively
			fmt.Printf("Missing result for partition %d on source1 (%t) or source2 (%t)\n", i, ok1, ok2)
			comparisonResult.Match = false
			comparisonResult.MismatchPartitions = append(comparisonResult.MismatchPartitions, i)
			mismatchedPartitionsMap[i] = true
			continue
		}

		// Check for errors within partition results (though CalculateChecksumInPartitions should have returned error already)
		if res1.Error != nil || res2.Error != nil {
			fmt.Printf("Error encountered in partition %d: src1=%v, src2=%v\n", i, res1.Error, res2.Error)
			comparisonResult.Match = false // Mark as mismatch if any partition had errors
			comparisonResult.MismatchPartitions = append(comparisonResult.MismatchPartitions, i)
			mismatchedPartitionsMap[i] = true
			continue // Skip checksum comparison if error occurred
		}

		if res1.RowCount != res2.RowCount {
			fmt.Printf("Partition %d row count mismatch: Source1=%d, Source2=%d\n", i, res1.RowCount, res2.RowCount)
			comparisonResult.Match = false
			comparisonResult.RowCountMismatch = true // Mark overall row count mismatch as well
			comparisonResult.MismatchPartitions = append(comparisonResult.MismatchPartitions, i)
			mismatchedPartitionsMap[i] = true
		}
		if res1.Checksum != res2.Checksum {
			fmt.Printf("Partition %d checksum mismatch: Source1=%s, Source2=%s\n", i, res1.Checksum, res2.Checksum)
			comparisonResult.Match = false
			if !mismatchedPartitionsMap[i] { // Avoid duplicates if row count also mismatched
				comparisonResult.MismatchPartitions = append(comparisonResult.MismatchPartitions, i)
				mismatchedPartitionsMap[i] = true
			}
		}
	}

	// Also consider the initial row count check
	if comparisonResult.RowCountMismatch {
		comparisonResult.Match = false
	}

	// 5. If Mismatches Found, Perform Row-by-Row Verification for those partitions
	// TODO

	sort.Ints(comparisonResult.MismatchPartitions) // Sort for consistent output

	if comparisonResult.Match {
		fmt.Printf("Validation successful: Table %s checksums match.\n", v.TableName)
	} else {
		fmt.Printf("Validation failed for table %s.\n", v.TableName)
		if comparisonResult.RowCountMismatch {
			fmt.Printf("  Row count mismatch: Source1=%d, Source2=%d\n", comparisonResult.SrcTotalRows, comparisonResult.TargetTotalRows)
		}
		if len(comparisonResult.MismatchPartitions) > 0 {
			fmt.Printf("  Mismatched partitions: %v\n", comparisonResult.MismatchPartitions)
		}
		if comparisonResult.SrcError != nil || comparisonResult.TargetError != nil {
			fmt.Printf("  Errors occurred during validation: Source1=%v, Source2=%v\n", comparisonResult.SrcError, comparisonResult.TargetError)
		}
	}

	return comparisonResult, nil // Return comparison result, error is nil unless setup failed catastrophically
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
