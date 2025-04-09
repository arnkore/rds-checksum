package checksum

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

// Config holds the database connection configuration.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// ChecksumResult holds the result of a checksum calculation.
type ChecksumResult struct {
	Count    int64
	Checksum string
}

// TableInfo holds metadata about a table.
type TableInfo struct {
	Name        string
	Columns     []string
	PrimaryKey  string
	RowCount    int64
	Partitions  int
	TableExists bool
}

// TableMetaProvider fetches metadata about a database table.
type TableMetaProvider struct {
	db *sql.DB
}

// NewTableMetaProvider creates a new TableMetaProvider.
func NewTableMetaProvider(config Config) (*TableMetaProvider, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.User, config.Password, config.Host, config.Port, config.DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return &TableMetaProvider{db: db}, nil
}

// Close closes the database connection.
func (p *TableMetaProvider) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// GetTableInfo retrieves metadata for the specified table.
func (p *TableMetaProvider) GetTableInfo(tableName string) (*TableInfo, error) {
	exists, err := p.verifyTableExists(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to verify table existence for %s: %w", tableName, err)
	}
	if !exists {
		return &TableInfo{Name: tableName, TableExists: false}, nil
	}

	rowCount, err := p.getRowCount(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get row count for %s: %w", tableName, err)
	}

	columns, err := p.getColumnNames(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns for %s: %w", tableName, err)
	}

	primaryKey, err := p.getPrimaryKey(tableName)
	if err != nil {
		// Non-fatal, proceed without primary key if not found or error occurs
		fmt.Printf("Warning: Could not determine primary key for table %s: %v\n", tableName, err)
		primaryKey = "" // Set to empty if not found or error
	}

	// For simplicity, let's assume a fixed number of partitions or determine dynamically if needed.
	// Here, we'll just set a default or leave it 0 if not applicable.
	partitions := 0 // Example: Could calculate based on rowCount or primary key range if applicable

	return &TableInfo{
		Name:        tableName,
		Columns:     columns,
		PrimaryKey:  primaryKey,
		RowCount:    rowCount,
		Partitions:  partitions, // Adjust based on actual partitioning strategy if any
		TableExists: true,
	}, nil
}

// verifyTableExists checks if a table exists in the database.
func (p *TableMetaProvider) verifyTableExists(tableName string) (bool, error) {
	query := "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?"
	var count int
	err := p.db.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to query table existence: %w", err)
	}
	return count > 0, nil
}

// getRowCount retrieves the total number of rows in a table.
func (p *TableMetaProvider) getRowCount(tableName string) (int64, error) {
	var count int64
	// Using COUNT(*) can be slow on large InnoDB tables without a WHERE clause.
	// Consider alternative methods if performance is critical, like `information_schema.tables` (can be approximate)
	// or `EXPLAIN SELECT COUNT(*) FROM tableName` which might be faster.
	// For simplicity and accuracy, we use COUNT(*) here.
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)
	err := p.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to query row count: %w", err)
	}
	return count, nil
}

// getColumnNames retrieves the names of the columns in a table, ordered by ordinal position.
func (p *TableMetaProvider) getColumnNames(tableName string) ([]string, error) {
	query := `
		SELECT COLUMN_NAME
		FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ?
		ORDER BY ORDINAL_POSITION
	`
	rows, err := p.db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query column names: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %w", err)
		}
		columns = append(columns, columnName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over column names rows: %w", err)
	}
	if len(columns) == 0 {
        // This case should ideally not happen if verifyTableExists passed,
        // but good to handle defensively.
		return nil, fmt.Errorf("no columns found for table %s, or table does not exist", tableName)
	}
	return columns, nil
}

// getPrimaryKey retrieves the primary key column name(s) for a table.
// Returns the first column of the primary key if it exists, otherwise empty string.
func (p *TableMetaProvider) getPrimaryKey(tableName string) (string, error) {
	query := `
		SELECT COLUMN_NAME
		FROM information_schema.key_column_usage
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		  AND constraint_name = 'PRIMARY'
		ORDER BY ORDINAL_POSITION
		LIMIT 1
	`
	var primaryKeyColumn string
	err := p.db.QueryRow(query, tableName).Scan(&primaryKeyColumn)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil // No primary key found
		}
		return "", fmt.Errorf("failed to query primary key: %w", err)
	}
	return primaryKeyColumn, nil
}

// Partition represents a range of rows in a table, typically defined by PK range.
type Partition struct {
	Index    int
	StartPK  interface{} // Can be int, string, etc., depending on PK type
	EndPK    interface{}
	RowCount int64 // Estimated or actual row count in the partition
}

// PartitionCalculator determines how to divide a table into partitions.
type PartitionCalculator struct {
	db        *sql.DB
	tableName string
	pkColumn  string
	rowCount  int64
}

// NewPartitionCalculator creates a new PartitionCalculator.
func NewPartitionCalculator(db *sql.DB, tableInfo *TableInfo) (*PartitionCalculator, error) {
	if tableInfo.PrimaryKey == "" {
		return nil, fmt.Errorf("cannot partition table %s without a primary key", tableInfo.Name)
	}
	// Further checks could be added here, e.g., ensuring PK is numeric for range partitioning.
	return &PartitionCalculator{
		db:        db,
		tableName: tableInfo.Name,
		pkColumn:  tableInfo.PrimaryKey,
		rowCount:  tableInfo.RowCount,
	}, nil
}

// CalculatePartitions divides the table into partitions based on the primary key.
// This is a simplified example assuming a numeric, auto-incrementing PK.
// A more robust implementation would handle different PK types and distributions.
func (pc *PartitionCalculator) CalculatePartitions(numPartitions int) ([]Partition, error) {
	if numPartitions <= 0 {
		return nil, fmt.Errorf("number of partitions must be positive")
	}
	if pc.rowCount == 0 {
		return []Partition{}, nil // No rows, no partitions needed
	}

	var minPK, maxPK int64
	queryMinMax := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`", pc.pkColumn, pc.pkColumn, pc.tableName)
	err := pc.db.QueryRow(queryMinMax).Scan(&minPK, &maxPK)
	if err != nil {
		// Handle cases like empty table (though checked earlier) or non-numeric PK if assumption is wrong
		if err == sql.ErrNoRows {
			return []Partition{}, nil
		}
        // A simple scan might fail if MIN/MAX return NULL (empty table) or non-integer types.
        // Need more robust scanning if PK isn't guaranteed to be non-null integer.
		var minPKNullable, maxPKNullable sql.NullInt64
        errRetry := pc.db.QueryRow(queryMinMax).Scan(&minPKNullable, &maxPKNullable)
        if errRetry != nil{
             return nil, fmt.Errorf("failed to query min/max primary key for %s: %w", pc.tableName, errRetry)
        }
        if !minPKNullable.Valid || !maxPKNullable.Valid {
             // This implies the table is empty, contradicting rowCount > 0 potentially.
             // Or the PK is not suitable for MIN/MAX (e.g., all NULLs, though unlikely for PK).
             return []Partition{}, fmt.Errorf("primary key MIN/MAX returned NULL for table %s, check table state", pc.tableName)
        }
        minPK = minPKNullable.Int64
        maxPK = maxPKNullable.Int64
	}


	if maxPK < minPK {
         // Should not happen in a normal table with rows
         return []Partition{}, fmt.Errorf("max primary key (%d) is less than min primary key (%d) for table %s", maxPK, minPK, pc.tableName)
    }


	partitions := make([]Partition, 0, numPartitions)
	totalRange := maxPK - minPK + 1
    // Avoid division by zero if numPartitions is 1 and range is 0 (single row table)
    if totalRange == 0 && numPartitions > 0 { // Single distinct PK value
        partitions = append(partitions, Partition{Index: 1, StartPK: minPK, EndPK: maxPK})
        return partitions, nil
    }
    // Ensure partitionSize is at least 1
	partitionSize := totalRange / int64(numPartitions)
    if partitionSize == 0 && totalRange > 0 {
        partitionSize = 1 // Handle case where range < numPartitions
    }


	currentStart := minPK
	for i := 0; i < numPartitions; i++ {
		currentEnd := currentStart + partitionSize - 1
		if i == numPartitions-1 || currentEnd >= maxPK {
			currentEnd = maxPK // Ensure the last partition includes the max PK
		}

        // Add partition only if start <= end. This handles edge cases.
        if currentStart <= currentEnd {
		    partitions = append(partitions, Partition{
			    Index:   i + 1,
			    StartPK: currentStart,
			    EndPK:   currentEnd,
			    // RowCount estimation could be added here if needed
		    })
        }

		currentStart = currentEnd + 1
        if currentStart > maxPK {
            break // Stop if we've covered the entire range
        }
	}

	// Basic validation: Ensure we didn't create empty ranges or exceed maxPK inappropriately
    if len(partitions) > 0 {
        lastPartition := partitions[len(partitions)-1]
        if lastPK, ok := lastPartition.EndPK.(int64); ok && lastPK != maxPK {
           // This might indicate a logic error or edge case not handled
           fmt.Printf("Warning: Last partition end PK %d does not match max PK %d for table %s\n", lastPK, maxPK, pc.tableName)
           // Optionally adjust the last partition's EndPK here if necessary
           // partitions[len(partitions)-1].EndPK = maxPK
        }
    } else if pc.rowCount > 0 {
         // If we have rows but generated no partitions, something is wrong
         return nil, fmt.Errorf("failed to generate partitions for table %s despite having %d rows", pc.tableName, pc.rowCount)
    }


	return partitions, nil
}

// PartitionChecksumResult holds checksum results for a single partition.
type PartitionChecksumResult struct {
	Partition Partition
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

// ChecksumCalculator computes checksums for table data.
type ChecksumCalculator struct {
	config    Config
	tableInfo *TableInfo
	db        *sql.DB // Use a shared connection for metadata, manage connections per goroutine for data
}

// NewChecksumCalculator creates a new ChecksumCalculator.
func NewChecksumCalculator(config Config, tableInfo *TableInfo) (*ChecksumCalculator, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.User, config.Password, config.Host, config.Port, config.DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection for calculator: %w", err)
	}
	// Ping to ensure connection is valid, but don't keep it open long if not needed immediately
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database for calculator: %w", err)
	}
	// Close this initial connection; goroutines will open their own.
	// Alternatively, use a connection pool managed externally.
	db.Close()

	return &ChecksumCalculator{
		config:    config,
		tableInfo: tableInfo,
		// db: db, // Don't store db here if each goroutine manages its own
	}, nil
}

// calculateRowChecksum computes the SHA256 checksum for a single row.
// Input `values` should be in the order of `tableInfo.Columns`.
func (c *ChecksumCalculator) calculateRowChecksum(values []interface{}) (string, error) {
	hasher := sha256.New()
	for i, val := range values {
		colName := c.tableInfo.Columns[i] // Get column name for context
		var strVal string
		if val == nil {
			strVal = "NULL" // Consistent representation for NULL
		} else {
			// Convert value to string representation. Handle different types carefully.
			switch v := val.(type) {
			case []byte:
				strVal = string(v)
			case string:
				strVal = v
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				strVal = fmt.Sprintf("%d", v)
			case float32, float64:
				// Use a consistent float format, e.g., strconv.FormatFloat
				// Be mindful of precision issues with floats.
                strVal = strconv.FormatFloat(val.(float64), 'g', -1, 64)
			// Add cases for other types like time.Time, bool, etc. as needed
			// case time.Time:
			//     strVal = v.Format(time.RFC3339Nano) // Example format
            case bool:
                if v { strVal = "1" } else { strVal = "0" }
			default:
                // Fallback or error for unsupported types
				return "", fmt.Errorf("unsupported data type %T for column %s", v, colName)
			}
		}
		// Write "colName=value;" to the hasher for structure. Separator is important.
		hasher.Write([]byte(fmt.Sprintf("%s=%s;", colName, strVal)))
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}


// calculatePartitionChecksum calculates the checksum for a given partition.
func (c *ChecksumCalculator) calculatePartitionChecksum(partition Partition, columns []string) (string, int64, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.config.User, c.config.Password, c.config.Host, c.config.Port, c.config.DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", 0, fmt.Errorf("partition %d: failed to open DB connection: %w", partition.Index, err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return "", 0, fmt.Errorf("partition %d: failed to ping DB: %w", partition.Index, err)
	}

	colList := "`" + strings.Join(columns, "`,`") + "`"
	pkCol := "`" + c.tableInfo.PrimaryKey + "`"

	// Construct WHERE clause based on PK range
	// Ensure StartPK and EndPK are handled correctly based on their actual type (e.g., int64)
    whereClause := fmt.Sprintf("%s >= ? AND %s <= ?", pkCol, pkCol)
	query := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s ORDER BY %s",
        colList, c.tableInfo.Name, whereClause, pkCol) // ORDER BY PK is crucial


	rows, err := db.Query(query, partition.StartPK, partition.EndPK)
	if err != nil {
		return "", 0, fmt.Errorf("partition %d: failed to query rows (PK >= %v AND PK <= %v): %w",
            partition.Index, partition.StartPK, partition.EndPK, err)
	}
	defer rows.Close()

	partitionHasher := sha256.New()
	var rowCount int64 = 0
	rowData := make([]interface{}, len(columns))
	rowDataPtrs := make([]interface{}, len(columns))
	for i := range rowData {
		rowDataPtrs[i] = &rowData[i]
	}

	for rows.Next() {
		if err := rows.Scan(rowDataPtrs...); err != nil {
			return "", 0, fmt.Errorf("partition %d: failed to scan row: %w", partition.Index, err)
		}
		rowHash, err := c.calculateRowChecksum(rowData)
		if err != nil {
			return "", 0, fmt.Errorf("partition %d: failed to calculate checksum for row (data: %v): %w", partition.Index, rowData, err)
		}
		partitionHasher.Write([]byte(rowHash)) // Combine row hashes
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return "", 0, fmt.Errorf("partition %d: error iterating rows: %w", partition.Index, err)
	}

	// Final checksum for the partition is the hash of concatenated row hashes
	finalChecksum := hex.EncodeToString(partitionHasher.Sum(nil))
	return finalChecksum, rowCount, nil
}

// CalculateChecksumInPartitions calculates checksums for all partitions in parallel.
func (c *ChecksumCalculator) CalculateChecksumInPartitions(partitions []Partition) (map[int]PartitionChecksumResult, error) {
	if c.tableInfo == nil || !c.tableInfo.TableExists {
		return nil, fmt.Errorf("table %s does not exist or info not loaded", c.config.DBName)
	}
	if len(partitions) == 0 && c.tableInfo.RowCount > 0 {
        // Handle case where table has rows but no partitions were generated (e.g., PK issue)
        // A single "partition" covering the whole table might be needed, or error out.
         fmt.Printf("Warning: Calculating checksum for non-partitioned table %s with %d rows.\n", c.tableInfo.Name, c.tableInfo.RowCount)
         // For simplicity, treat the whole table as one partition if partitioning failed/not applicable
         // This might happen if table has no PK suitable for partitioning.

         // We need PK range even for a single partition conceptually. If PK is missing, error.
         if c.tableInfo.PrimaryKey == "" {
             return nil, fmt.Errorf("cannot calculate checksum for table %s without a primary key", c.tableInfo.Name)
         }
         // Create a single pseudo-partition covering everything (requires min/max PK)
         // This logic duplicates some from PartitionCalculator, refactor potential.
         var minPK, maxPK interface{} // Use interface{} for flexibility
         queryMinMax := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`", c.tableInfo.PrimaryKey, c.tableInfo.PrimaryKey, c.tableInfo.Name)
         // Need a temporary DB connection here if c.db is not stored
         dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.config.User, c.config.Password, c.config.Host, c.config.Port, c.config.DBName)
         tempDb, err := sql.Open("mysql", dsn)
         if err != nil { return nil, fmt.Errorf("failed to open temp db connection: %w", err)}
         defer tempDb.Close()
         err = tempDb.QueryRow(queryMinMax).Scan(&minPK, &maxPK)
         if err != nil {
             // Handle NULLs/errors similar to PartitionCalculator
             var minPKNullable, maxPKNullable sql.NullInt64 // Use sql.NullInt64 assuming PK is integer-like
             errRetry := tempDb.QueryRow(queryMinMax).Scan(&minPKNullable, &maxPKNullable)
             if errRetry != nil {
                 return nil, fmt.Errorf("failed query min/max PK for single partition: %w", errRetry)
             }
             if !minPKNullable.Valid || !maxPKNullable.Valid {
                 // Empty table case, should align with rowCount == 0
                 if c.tableInfo.RowCount == 0 {
                     return map[int]PartitionChecksumResult{}, nil // Empty result for empty table
                 }
                 return nil, fmt.Errorf("min/max PK NULL for non-empty table %s", c.tableInfo.Name)
             }
             minPK = minPKNullable // Assign actual value from NullValue
             maxPK = maxPKNullable
         }

         partitions = []Partition{{Index: 1, StartPK: minPK, EndPK: maxPK}}


	} else if len(partitions) == 0 && c.tableInfo.RowCount == 0 {
        // Empty table, return empty result immediately
        return map[int]PartitionChecksumResult{}, nil
    }


	resultsChan := make(chan PartitionChecksumResult, len(partitions))
	var eg errgroup.Group
	var mu sync.Mutex // Mutex to protect shared resources if any (e.g., logging)
    columns := c.tableInfo.Columns


	for _, p := range partitions {
		part := p // Capture loop variable for goroutine
		eg.Go(func() error {
            mu.Lock()
			fmt.Printf("Starting checksum for partition %d (PK %v to %v)...\n", part.Index, part.StartPK, part.EndPK)
            mu.Unlock()

			checksum, rowCount, err := c.calculatePartitionChecksum(part, columns)

            mu.Lock()
			if err != nil {
				fmt.Printf("Error in partition %d: %v\n", part.Index, err)
                // Don't return error immediately from goroutine, send it via channel
                // resultsChan <- PartitionChecksumResult{Partition: part, Error: err}
                // return err // Returning error here cancels other goroutines in errgroup
			} else {
				fmt.Printf("Finished partition %d: Count=%d, Checksum=%s\n", part.Index, rowCount, checksum)
			}
            mu.Unlock()

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
    var firstError error = waitErr // Initialize with potential error from Wait()


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


	// Verify total row count matches table info
	if totalRowCount != c.tableInfo.RowCount {
		// This indicates an issue, possibly with partitioning logic or concurrent updates
		return results, fmt.Errorf("mismatch in row count: calculated %d, expected %d for table %s",
			totalRowCount, c.tableInfo.RowCount, c.tableInfo.Name)
	}

	return results, nil
}


// fetchRowsForVerification fetches specific rows based on Primary Key for detailed comparison.
func (c *ChecksumCalculator) fetchRowsForVerification(pks []interface{}) (map[interface{}]string, error) {
	if len(pks) == 0 {
		return map[interface{}]string{}, nil
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.config.User, c.config.Password, c.config.Host, c.config.Port, c.config.DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open DB connection for row verification: %w", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping DB for row verification: %w", err)
	}


	colList := "`" + strings.Join(c.tableInfo.Columns, "`,`") + "`"
	pkCol := "`" + c.tableInfo.PrimaryKey + "`"
    placeholders := strings.Repeat("?,", len(pks)-1) + "?"
	query := fmt.Sprintf("SELECT %s, %s FROM `%s` WHERE %s IN (%s)", pkCol, colList, c.tableInfo.Name, pkCol, placeholders)

	rows, err := db.Query(query, pks...)
	if err != nil {
		return nil, fmt.Errorf("failed to query rows for verification (PKs: %v): %w", pks, err)
	}
	defer rows.Close()

	results := make(map[interface{}]string)
    numCols := len(c.tableInfo.Columns)
	rowData := make([]interface{}, numCols+1) // +1 for the PK column
	rowDataPtrs := make([]interface{}, numCols+1)
	for i := range rowData {
		rowDataPtrs[i] = &rowData[i]
	}

	for rows.Next() {
		if err := rows.Scan(rowDataPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row during verification: %w", err)
		}
        pkValue := rowData[0] // First scanned value is the PK
        rowValues := rowData[1:] // Remaining values are the data columns

		rowHash, err := c.calculateRowChecksum(rowValues) // Pass only data columns
		if err != nil {
			// Log or handle error for specific row checksum failure
            fmt.Printf("Error calculating checksum for row with PK %v: %v\n", pkValue, err)
            // Depending on requirements, either skip this row or return an error
			results[pkValue] = fmt.Sprintf("ERROR: %v", err) // Mark row as errored
            // continue // Or return nil, fmt.Errorf(...) if one error should fail all
		} else {
		    results[pkValue] = rowHash
        }
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating verification rows: %w", err)
	}

	// Check if all requested PKs were found
    if len(results) != len(pks) {
       // Some PKs might be missing if rows were deleted between steps.
       // Log this difference. Depending on strictness, could be an error.
       fmt.Printf("Warning: Requested %d PKs for verification, found %d rows for table %s.\n", len(pks), len(results), c.tableInfo.Name)
    }

	return results, nil
}


// CompareChecksumResults compares checksum results from two sources (e.g., master and replica).
type CompareChecksumResults struct {
	Match            bool
	MismatchPartitions []int // Indices of partitions that don't match
	RowCountMismatch bool
	TotalRowsSource1 int64
	TotalRowsSource2 int64
	FailedRowsSource1 map[interface{}]string // PK -> Hash/Error for rows that failed comparison on Source 1
	FailedRowsSource2 map[interface{}]string // PK -> Hash/Error for rows that failed comparison on Source 2
    Source1Error error // Errors during Source 1 processing (e.g., fetching rows)
    Source2Error error // Errors during Source 2 processing
}


// UnifiedChecksumValidator orchestrates the checksum validation process.
type UnifiedChecksumValidator struct {
	Config1 Config // Source 1 (e.g., Master)
	Config2 Config // Source 2 (e.g., Replica)
	TableName string
    NumPartitions int // Number of partitions to use
}

// NewUnifiedChecksumValidator creates a new validator.
func NewUnifiedChecksumValidator(cfg1, cfg2 Config, tableName string, numPartitions int) *UnifiedChecksumValidator {
    return &UnifiedChecksumValidator{
        Config1: cfg1,
        Config2: cfg2,
        TableName: tableName,
        NumPartitions: numPartitions,
    }
}


// RunValidation performs the checksum validation.
func (v *UnifiedChecksumValidator) RunValidation() (*CompareChecksumResults, error) {
	fmt.Printf("Starting unified checksum validation for table %s...\n", v.TableName)
    comparisonResult := &CompareChecksumResults{
        FailedRowsSource1: make(map[interface{}]string),
        FailedRowsSource2: make(map[interface{}]string),
    }

	// 1. Get Table Metadata for both sources concurrently
	var tableInfo1, tableInfo2 *TableInfo
	var metaErr1, metaErr2 error
	var wgMeta sync.WaitGroup
	wgMeta.Add(2)

	go func() {
		defer wgMeta.Done()
		provider1, err := NewTableMetaProvider(v.Config1)
		if err != nil { metaErr1 = fmt.Errorf("source1 meta provider: %w", err); return }
		defer provider1.Close()
		tableInfo1, metaErr1 = provider1.GetTableInfo(v.TableName)
	}()
	go func() {
		defer wgMeta.Done()
		provider2, err := NewTableMetaProvider(v.Config2)
		if err != nil { metaErr2 = fmt.Errorf("source2 meta provider: %w", err); return }
		defer provider2.Close()
		tableInfo2, metaErr2 = provider2.GetTableInfo(v.TableName)
	}()
	wgMeta.Wait()

	if metaErr1 != nil { return nil, fmt.Errorf("failed getting metadata from source 1: %w", metaErr1)}
	if metaErr2 != nil { return nil, fmt.Errorf("failed getting metadata from source 2: %w", metaErr2)}

    if !tableInfo1.TableExists || !tableInfo2.TableExists {
        // Handle cases where table exists on one source but not the other
        if tableInfo1.TableExists != tableInfo2.TableExists {
             comparisonResult.Match = false
             comparisonResult.RowCountMismatch = true // Consider existence difference as a row count mismatch
             // Provide more specific info about existence
             return comparisonResult, fmt.Errorf("table existence mismatch: source1 exists=%t, source2 exists=%t", tableInfo1.TableExists, tableInfo2.TableExists)
        }
        // If table doesn't exist on either, perhaps return success or a specific status
        if !tableInfo1.TableExists {
             fmt.Printf("Table %s does not exist on either source. Skipping validation.\n", v.TableName)
             comparisonResult.Match = true // Or define a specific state for non-existent tables
             return comparisonResult, nil
        }
    }


	// Basic checks: Row counts and column definitions (optional but recommended)
    comparisonResult.TotalRowsSource1 = tableInfo1.RowCount
    comparisonResult.TotalRowsSource2 = tableInfo2.RowCount
	if tableInfo1.RowCount != tableInfo2.RowCount {
        fmt.Printf("Row count mismatch: Source1=%d, Source2=%d\n", tableInfo1.RowCount, tableInfo2.RowCount)
		comparisonResult.RowCountMismatch = true
        // Decide whether to proceed if row counts differ. Often, checksums are invalid anyway.
        // For this example, we'll continue to partition checksums, but mark as mismatch.
        comparisonResult.Match = false
	}
    // Optional: Compare column list/order
    if !equalStringSlices(tableInfo1.Columns, tableInfo2.Columns) {
         return nil, fmt.Errorf("column definition mismatch between sources for table %s", v.TableName)
    }
    // Ensure a PK exists if partitioning is intended
    if v.NumPartitions > 1 && (tableInfo1.PrimaryKey == "" || tableInfo2.PrimaryKey == "") {
         return nil, fmt.Errorf("cannot use partitioning for table %s as primary key is missing on one or both sources", v.TableName)
    }
     // Use PK from source 1 for partitioning; assume they are compatible if columns match.
    effectivePK := tableInfo1.PrimaryKey


	// 2. Calculate Partitions (based on Source 1 metadata, assuming structure compatibility)
    // Need a temporary DB connection for partition calculation
    dsn1 := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", v.Config1.User, v.Config1.Password, v.Config1.Host, v.Config1.Port, v.Config1.DBName)
    db1Partition, err := sql.Open("mysql", dsn1)
    if err != nil { return nil, fmt.Errorf("failed to open db connection for partitioning (source 1): %w", err)}
    defer db1Partition.Close()

	partCalc, err := NewPartitionCalculator(db1Partition, tableInfo1)
	if err != nil {
        // Handle case where partitioning is not possible (e.g., no PK)
        // Fallback to full table checksum? For now, error out if partitioning requested.
        if v.NumPartitions > 1 {
		    return nil, fmt.Errorf("failed to initialize partition calculator (source 1): %w", err)
        }
        // If NumPartitions is 1 or less, maybe we don't need partitioning.
        fmt.Println("Partitioning not possible or not requested, proceeding with full table comparison if applicable.")
        // If row counts match and no partitioning, maybe skip partition checksums?
        // Or treat the whole table as one partition (handled within CalculateChecksumInPartitions)
	}

    var partitions []Partition
    if partCalc != nil && v.NumPartitions > 0 { // Only calculate if calculator created and partitions requested
	    partitions, err = partCalc.CalculatePartitions(v.NumPartitions)
	    if err != nil {
		    return nil, fmt.Errorf("failed to calculate partitions (source 1): %w", err)
	    }
        fmt.Printf("Calculated %d partitions for table %s based on source 1.\n", len(partitions), v.TableName)
    } else if tableInfo1.RowCount > 0 && v.NumPartitions != 0 {
        // If row count > 0 but partitioning failed/skipped, and partitions were expected (NumPartitions!=0)
         return nil, fmt.Errorf("partitioning requested but could not be performed for table %s", v.TableName)
    }
    // If tableInfo1.RowCount == 0, partitions will be empty, which is fine.


	// 3. Calculate Checksums for Partitions on both sources concurrently
	var results1, results2 map[int]PartitionChecksumResult
	var checksumErr1, checksumErr2 error
	var wgChecksum sync.WaitGroup
	wgChecksum.Add(2)

	go func() {
		defer wgChecksum.Done()
		calc1, err := NewChecksumCalculator(v.Config1, tableInfo1)
		if err != nil { checksumErr1 = fmt.Errorf("source1 checksum calc init: %w", err); return }
		results1, checksumErr1 = calc1.CalculateChecksumInPartitions(partitions)
	}()
	go func() {
		defer wgChecksum.Done()
		calc2, err := NewChecksumCalculator(v.Config2, tableInfo2)
		if err != nil { checksumErr2 = fmt.Errorf("source2 checksum calc init: %w", err); return }
        // Use the *same* partition definitions derived from source 1
		results2, checksumErr2 = calc2.CalculateChecksumInPartitions(partitions)
	}()
	wgChecksum.Wait()

	if checksumErr1 != nil { return nil, fmt.Errorf("error calculating checksums on source 1: %w", checksumErr1)}
	if checksumErr2 != nil { return nil, fmt.Errorf("error calculating checksums on source 2: %w", checksumErr2)}

    // Check if number of results matches expected number of partitions
    if len(results1) != len(partitions) || len(results2) != len(partitions) {
       // This might indicate partial failures or inconsistencies
       return nil, fmt.Errorf("partition result count mismatch: expected %d, source1 got %d, source2 got %d", len(partitions), len(results1), len(results2))
    }


	// 4. Compare Partition Checksums
	comparisonResult.Match = true // Assume match until proven otherwise
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
	if !comparisonResult.Match && len(comparisonResult.MismatchPartitions) > 0 && effectivePK != "" {
		fmt.Printf("Mismatch detected in partitions %v. Performing row-by-row check...\n", comparisonResult.MismatchPartitions)

        // Identify all PKs within the mismatched partitions. This requires querying PKs.
        // This can be inefficient. Alternative: Directly query rows in mismatched partitions again.
        pksToVerify := []interface{}{}
        for _, pIndex := range comparisonResult.MismatchPartitions {
             part := partitions[pIndex-1] // Get partition info (0-based index)
             // Need DB connection to fetch PKs for this partition range
             db1PKFetch, err := sql.Open("mysql", dsn1) // Use source 1 for PK list
             if err != nil { comparisonResult.Source1Error = err; continue } // Log error and potentially skip partition

             pkQuery := fmt.Sprintf("SELECT `%s` FROM `%s` WHERE `%s` >= ? AND `%s` <= ? ORDER BY `%s`",
                effectivePK, v.TableName, effectivePK, effectivePK, effectivePK)
             rows, err := db1PKFetch.Query(pkQuery, part.StartPK, part.EndPK)
             if err != nil {
                 fmt.Printf("Error fetching PKs for partition %d from source 1: %v\n", pIndex, err)
                 comparisonResult.Source1Error = fmt.Errorf("pk fetch partition %d: %w", pIndex, err)
                 db1PKFetch.Close()
                 continue // Skip this partition if PKs can't be fetched
             }

             for rows.Next() {
                 // Scan appropriately based on PK type. Assume int64 for simplicity here.
                 var pkVal int64
                 if err := rows.Scan(&pkVal); err != nil {
                     fmt.Printf("Error scanning PK in partition %d source 1: %v\n", pIndex, err)
                     // Decide how to handle scan error (log, skip row, fail partition?)
                 } else {
                     pksToVerify = append(pksToVerify, pkVal)
                 }
             }
             rows.Close() // Close rows iterator
             db1PKFetch.Close() // Close connection for this partition fetch
             if rows.Err() != nil {
                 fmt.Printf("Error iterating PKs for partition %d source 1: %v\n", pIndex, rows.Err())
                 comparisonResult.Source1Error = fmt.Errorf("pk iterate partition %d: %w", pIndex, rows.Err())
             }
        }


        if len(pksToVerify) > 0 {
		    var rows1, rows2 map[interface{}]string
		    var rowErr1, rowErr2 error
		    var wgRows sync.WaitGroup
		    wgRows.Add(2)

		    go func() {
			    defer wgRows.Done()
			    calc1, err := NewChecksumCalculator(v.Config1, tableInfo1) // Recreate or reuse
                if err != nil { rowErr1 = err; return }
			    rows1, rowErr1 = calc1.fetchRowsForVerification(pksToVerify)
		    }()
		    go func() {
			    defer wgRows.Done()
			    calc2, err := NewChecksumCalculator(v.Config2, tableInfo2) // Recreate or reuse
                if err != nil { rowErr2 = err; return }
			    rows2, rowErr2 = calc2.fetchRowsForVerification(pksToVerify)
		    }()
		    wgRows.Wait()

            comparisonResult.Source1Error = rowErr1 // Store errors from row fetching
            comparisonResult.Source2Error = rowErr2


            if rowErr1 != nil || rowErr2 != nil {
                fmt.Printf("Error during row verification: src1=%v, src2=%v\n", rowErr1, rowErr2)
                // Decide if this should completely fail validation or just report failed rows
            }


            // Compare row hashes
            allPKs := make(map[interface{}]bool)
            for pk := range rows1 { allPKs[pk] = true }
            for pk := range rows2 { allPKs[pk] = true }

            for pk := range allPKs {
                 hash1, ok1 := rows1[pk]
                 hash2, ok2 := rows2[pk]

                 // Handle rows existing on one side but not the other, or errors during hash calc
                 if !ok1 { // Exists in Source 2 only (or failed fetch in Source 1)
                    comparisonResult.FailedRowsSource1[pk] = "MISSING"
                    comparisonResult.FailedRowsSource2[pk] = hash2 // Store hash from source 2
                    comparisonResult.Match = false
                    continue
                 }
                 if !ok2 { // Exists in Source 1 only (or failed fetch in Source 2)
                    comparisonResult.FailedRowsSource1[pk] = hash1 // Store hash from source 1
                    comparisonResult.FailedRowsSource2[pk] = "MISSING"
                    comparisonResult.Match = false
                    continue
                 }

                 // Handle errors reported within the row map
                 isError1 := strings.HasPrefix(hash1, "ERROR:")
                 isError2 := strings.HasPrefix(hash2, "ERROR:")
                 if isError1 || isError2 {
                     comparisonResult.FailedRowsSource1[pk] = hash1 // Keep error message or hash
                     comparisonResult.FailedRowsSource2[pk] = hash2
                     comparisonResult.Match = false // Mark mismatch if error occurred
                     continue
                 }


                 if hash1 != hash2 {
                    comparisonResult.FailedRowsSource1[pk] = hash1
                    comparisonResult.FailedRowsSource2[pk] = hash2
                    comparisonResult.Match = false
                 }
            }
        } else if len(comparisonResult.MismatchPartitions) > 0 {
             // Mismatch detected, but couldn't get PKs to verify. Mark as inconclusive/failed?
             fmt.Println("Warning: Mismatched partitions found, but unable to fetch primary keys for row-by-row check.")
             // Keep Match as false, but FailedRows will be empty.
        }

	} // End row-by-row check

    sort.Ints(comparisonResult.MismatchPartitions) // Sort for consistent output

    if comparisonResult.Match {
        fmt.Printf("Validation successful: Table %s checksums match.\n", v.TableName)
    } else {
        fmt.Printf("Validation failed for table %s.\n", v.TableName)
        if comparisonResult.RowCountMismatch {
             fmt.Printf("  Row count mismatch: Source1=%d, Source2=%d\n", comparisonResult.TotalRowsSource1, comparisonResult.TotalRowsSource2)
        }
        if len(comparisonResult.MismatchPartitions) > 0 {
            fmt.Printf("  Mismatched partitions: %v\n", comparisonResult.MismatchPartitions)
        }
        if len(comparisonResult.FailedRowsSource1) > 0 {
             fmt.Printf("  Found %d differing or problematic rows.\n", len(comparisonResult.FailedRowsSource1))
             // Optionally print details of failed rows (can be verbose)
             // for pk, h1 := range comparisonResult.FailedRowsSource1 {
             //    h2 := comparisonResult.FailedRowsSource2[pk]
             //    fmt.Printf("    PK %v: Source1=%s, Source2=%s\n", pk, h1, h2)
             // }
        }
         if comparisonResult.Source1Error != nil || comparisonResult.Source2Error != nil {
            fmt.Printf("  Errors occurred during validation: Source1=%v, Source2=%v\n", comparisonResult.Source1Error, comparisonResult.Source2Error)
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

// --- Deprecated / Old Code (To be removed by file replacement) ---
// CalculateChecksum calculates the checksum for a given table.
// Deprecated: Use UnifiedChecksumValidator for new implementations.
func CalculateChecksum(config Config, tableName string) (string, error) {
	provider, err := NewTableMetaProvider(config)
	if err != nil {
		return "", fmt.Errorf("failed to create meta provider: %w", err)
	}
	defer provider.Close()

	tableInfo, err := provider.GetTableInfo(tableName)
	if err != nil {
		return "", fmt.Errorf("failed to get table info for %s: %w", tableName, err)
	}

	if !tableInfo.TableExists {
		return "", fmt.Errorf("table %s does not exist", tableName)
	}
    if tableInfo.RowCount == 0 {
        // Define checksum for empty table (e.g., hash of empty string or specific marker)
        emptyHash := hex.EncodeToString(sha256.New().Sum(nil))
        return fmt.Sprintf("0-%s", emptyHash), nil // Format: count-checksum
    }

	// Simplified checksum for the whole table (no partitioning)
    calculator, err := NewChecksumCalculator(config, tableInfo)
    if err != nil { return "", fmt.Errorf("failed to create calculator: %w", err)}

    // Need a DB connection to fetch all rows
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.User, config.Password, config.Host, config.Port, config.DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil { return "", fmt.Errorf("failed to open db connection: %w", err) }
	defer db.Close()
    if err := db.Ping(); err != nil { return "", fmt.Errorf("failed to ping db: %w", err) }

    colList := "`" + strings.Join(tableInfo.Columns, "`,`") + "`"
    pkCol := ""
    orderByClause := ""
    if tableInfo.PrimaryKey != "" {
        pkCol = "`" + tableInfo.PrimaryKey + "`"
        orderByClause = fmt.Sprintf("ORDER BY %s", pkCol)
    } else {
         // Warn if no PK for ordering - checksum might be inconsistent
         fmt.Printf("Warning: No primary key found for table %s. Checksum calculation might be inconsistent without ORDER BY.\n", tableName)
         // Consider ordering by all columns as a fallback, but can be slow/complex
         // orderByClause = "ORDER BY " + colList
    }

	query := fmt.Sprintf("SELECT %s FROM `%s` %s", colList, tableName, orderByClause)
	rows, err := db.Query(query)
	if err != nil { return "", fmt.Errorf("failed to query all rows: %w", err) }
	defer rows.Close()

	tableHasher := sha256.New()
	var rowCount int64 = 0
	rowData := make([]interface{}, len(tableInfo.Columns))
	rowDataPtrs := make([]interface{}, len(tableInfo.Columns))
	for i := range rowData {
		rowDataPtrs[i] = &rowData[i]
	}

	for rows.Next() {
		if err := rows.Scan(rowDataPtrs...); err != nil {
			return "", fmt.Errorf("failed to scan row: %w", err)
		}
		rowHash, err := calculator.calculateRowChecksum(rowData)
		if err != nil {
			return "", fmt.Errorf("failed to calculate checksum for row: %w", err)
		}
		tableHasher.Write([]byte(rowHash))
		rowCount++
	}
    if err := rows.Err(); err != nil { return "", fmt.Errorf("error iterating rows: %w", err) }

    if rowCount != tableInfo.RowCount {
        return "", fmt.Errorf("row count mismatch during full table scan: expected %d, got %d", tableInfo.RowCount, rowCount)
    }

	finalChecksum := hex.EncodeToString(tableHasher.Sum(nil))
	return fmt.Sprintf("%d-%s", rowCount, finalChecksum), nil
}

// CompareChecksums compares two checksum strings.
// Deprecated: Use UnifiedChecksumValidator for new implementations.
func CompareChecksums(checksum1, checksum2 string) (bool, error) {
	// Basic string comparison is sufficient if format is consistent
	if checksum1 == "" || checksum2 == "" {
		return false, fmt.Errorf("checksum strings cannot be empty")
	}
	return checksum1 == checksum2, nil
}