package checksum

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
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
	batchNum int
	count    int64
	checksum int64
	err      error
}

// CalculateChecksum calculates the checksum for a given table in batches
func CalculateChecksum(config *Config, table string) (string, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.User, config.Password, config.Host, config.Port, config.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", fmt.Errorf("failed to connect to database: %v", err)
	}
	defer db.Close()

	// Verify table exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", config.Database, table).Scan(&count)
	if err != nil {
		return "", fmt.Errorf("failed to check table existence: %v", err)
	}
	if count == 0 {
		return "", fmt.Errorf("table %s does not exist", table)
	}

	// Get total row count
	var totalRows int64
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&totalRows)
	if err != nil {
		return "", fmt.Errorf("failed to get total row count: %v", err)
	}

	// Get all columns for the table
	rows, err := db.Query("SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position", config.Database, table)
	if err != nil {
		return "", fmt.Errorf("failed to get columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return "", fmt.Errorf("failed to scan column: %v", err)
		}
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating columns: %v", err)
	}

	// Calculate number of batches
	batchSize := int64(10000)
	numBatches := (totalRows + batchSize - 1) / batchSize

	// Create a channel to receive batch results
	results := make(chan batchResult, numBatches)

	// Create a wait group to wait for all goroutines to complete
	var wg sync.WaitGroup

	// Process each batch in parallel
	for i := int64(0); i < numBatches; i++ {
		wg.Add(1)
		go func(batchNum int64) {
			defer wg.Done()

			// Create a new connection for this goroutine
			batchDB, err := sql.Open("mysql", dsn)
			if err != nil {
				results <- batchResult{batchNum: int(batchNum), err: err}
				return
			}
			defer batchDB.Close()

			// Calculate offset and limit for this batch
			offset := batchNum * batchSize
			limit := batchSize

			// Build the checksum query for this batch
			query := fmt.Sprintf("SELECT COUNT(*) as count, SUM(CRC32(CONCAT_WS('#', %s))) as checksum FROM %s LIMIT %d OFFSET %d",
				strings.Join(columns, ", "), table, limit, offset)

			var countRows int64
			var checksum int64
			err = batchDB.QueryRow(query).Scan(&countRows, &checksum)
			if err != nil {
				results <- batchResult{batchNum: int(batchNum), err: err}
				return
			}

			results <- batchResult{
				batchNum: int(batchNum),
				count:    countRows,
				checksum: checksum,
			}
		}(i)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var totalCount int64
	var totalChecksum int64
	for result := range results {
		if result.err != nil {
			return "", fmt.Errorf("batch %d failed: %v", result.batchNum, result.err)
		}
		totalCount += result.count
		totalChecksum += result.checksum
	}

	return fmt.Sprintf("%d-%d", totalCount, totalChecksum), nil
}

// CompareChecksums compares two checksums and returns true if they match
func CompareChecksums(checksum1, checksum2 string) bool {
	return checksum1 == checksum2
} 