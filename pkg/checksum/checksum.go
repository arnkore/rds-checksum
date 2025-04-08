package checksum

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"crypto/md5"
)

// Config holds the MySQL connection configuration
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// batchResult holds the result of a single batch calculation
type batchResult struct {
	batchNum     int
	count        int
	checksum     string
	err          error
	FailedRowIDs []int64
}

// Mode defines the validation mode
type Mode int

const (
	// ModeOverall validates the entire table as a whole
	ModeOverall Mode = iota
	// ModeRowByRow validates each row individually
	ModeRowByRow
)

// ValidationResult contains the validation results
type ValidationResult struct {
	IsValid     bool
	TotalRows   int
	FailedRows  int
	FailedRowIDs []int64
	Error       error
}

// CalculateChecksum calculates the checksum for a given table in batches
func CalculateChecksum(config *Config, tableName string, mode Mode) (*ValidationResult, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.User, config.Password, config.Host, config.Port, config.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	defer db.Close()

	// Verify table exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", config.Database, tableName).Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("failed to check table existence: %v", err)
	}
	if count == 0 {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get total row count
	var totalRows int64
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&totalRows)
	if err != nil {
		return nil, fmt.Errorf("failed to get total row count: %v", err)
	}

	// Get all columns for the table
	rows, err := db.Query("SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position", config.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %v", err)
	}

	// Calculate number of batches
	batchSize := int64(10000)
	numBatches := (totalRows + batchSize - 1) / batchSize

	// Process batches
	results := make([]batchResult, numBatches)
	var wg sync.WaitGroup
	for i := int64(0); i < numBatches; i++ {
		wg.Add(1)
		go func(batchNum int64) {
			defer wg.Done()

			// Create a new connection for this batch
			batchDB, err := sql.Open("mysql", dsn)
			if err != nil {
				results[batchNum] = batchResult{
					batchNum: int(batchNum),
					err:      fmt.Errorf("failed to create database connection: %v", err),
				}
				return
			}
			defer batchDB.Close()

			offset := batchNum * batchSize
			query := fmt.Sprintf("SELECT id, %s FROM %s ORDER BY id LIMIT %d OFFSET %d",
				strings.Join(columns, ", "), tableName, batchSize, offset)

			rows, err := batchDB.Query(query)
			if err != nil {
				results[batchNum] = batchResult{
					batchNum: int(batchNum),
					err:      fmt.Errorf("failed to query rows: %v", err),
				}
				return
			}
			defer rows.Close()

			var batchCount int
			var batchChecksum string
			var failedRowIDs []int64

			for rows.Next() {
				// Create a slice to hold column values
				values := make([]interface{}, len(columns)+1) // +1 for id
				valuePtrs := make([]interface{}, len(columns)+1)
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					results[batchNum] = batchResult{
						batchNum: int(batchNum),
						err:      fmt.Errorf("failed to scan row: %v", err),
					}
					return
				}

				// Get row ID
				rowID, ok := values[0].(int64)
				if !ok {
					results[batchNum] = batchResult{
						batchNum: int(batchNum),
						err:      fmt.Errorf("invalid row ID type"),
					}
					return
				}

				// Calculate row checksum
				rowChecksum := calculateRowChecksum(values[1:])

				if mode == ModeRowByRow {
					// For row-by-row mode, we need to verify each row individually
					// This is a simplified example - you would need to implement actual row verification
					if !verifyRow(rowID, rowChecksum) {
						failedRowIDs = append(failedRowIDs, rowID)
					}
				}

				batchCount++
				if batchChecksum == "" {
					batchChecksum = rowChecksum
				} else {
					batchChecksum = calculateCombinedChecksum(batchChecksum, rowChecksum)
				}
			}

			results[batchNum] = batchResult{
				batchNum:     int(batchNum),
				count:        batchCount,
				checksum:     batchChecksum,
				err:          nil,
				FailedRowIDs: failedRowIDs,
			}
		}(i)
	}

	wg.Wait()

	// Collect results
	var totalCount int
	var totalChecksum string
	var allFailedRowIDs []int64 = make([]int64, 0)
	var validationError error

	for _, result := range results {
		if result.err != nil {
			validationError = result.err
			break
		}
		totalCount += result.count
		if totalChecksum == "" {
			totalChecksum = result.checksum
		} else {
			totalChecksum = calculateCombinedChecksum(totalChecksum, result.checksum)
		}
		allFailedRowIDs = append(allFailedRowIDs, result.FailedRowIDs...)
	}

	if validationError != nil {
		return &ValidationResult{
			IsValid:     false,
			TotalRows:   totalCount,
			FailedRows:  len(allFailedRowIDs),
			FailedRowIDs: allFailedRowIDs,
			Error:       validationError,
		}, validationError
	}

	return &ValidationResult{
		IsValid:     len(allFailedRowIDs) == 0,
		TotalRows:   totalCount,
		FailedRows:  len(allFailedRowIDs),
		FailedRowIDs: allFailedRowIDs,
	}, nil
}

// calculateRowChecksum calculates the checksum for a single row
func calculateRowChecksum(values []interface{}) string {
	var rowData strings.Builder
	for _, value := range values {
		if value == nil {
			rowData.WriteString("NULL")
		} else {
			rowData.WriteString(fmt.Sprintf("%v", value))
		}
		rowData.WriteString("|")
	}
	return fmt.Sprintf("%x", md5.Sum([]byte(rowData.String())))
}

// verifyRow verifies a single row's checksum
// This is a placeholder - implement actual verification logic
func verifyRow(rowID int64, checksum string) bool {
	// TODO: Implement actual row verification logic
	// This could involve comparing with a reference checksum
	return true
}

// calculateCombinedChecksum combines two checksums
func calculateCombinedChecksum(checksum1, checksum2 string) string {
	combined := checksum1 + "|" + checksum2
	return fmt.Sprintf("%x", md5.Sum([]byte(combined)))
}

// CompareChecksums compares two checksums and returns true if they match
func CompareChecksums(checksum1, checksum2 string) bool {
	return checksum1 == checksum2
} 