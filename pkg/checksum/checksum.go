package checksum

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"crypto/md5"

	_ "github.com/go-sql-driver/mysql"
	"github.com/liuzonghao/mychecksum/pkg/metadata"
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

// dbConnector handles database connection operations
type dbConnector struct {
	config *Config
	dsn    string
}

// newDBConnector creates a new database connector
func newDBConnector(config *Config) *dbConnector {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.User, config.Password, config.Host, config.Port, config.Database)
	return &dbConnector{
		config: config,
		dsn:    dsn,
	}
}

// connect creates a new database connection
func (dc *dbConnector) connect() (*sql.DB, error) {
	db, err := sql.Open("mysql", dc.dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	return db, nil
}

// batchProcessor handles the processing of data in batches
type batchProcessor struct {
	connector *dbConnector
	info      *metadata.TableInfo
	mode      Mode
	batchSize int64
}

// newBatchProcessor creates a new batch processor
func newBatchProcessor(connector *dbConnector, info *metadata.TableInfo, mode Mode) *batchProcessor {
	return &batchProcessor{
		connector: connector,
		info:      info,
		mode:      mode,
		batchSize: 10000,
	}
}

// processBatch processes a single batch of data
func (bp *batchProcessor) processBatch(batchNum int64) batchResult {
	// Create a new connection for this batch
	batchDB, err := bp.connector.connect()
	if err != nil {
		return batchResult{
			batchNum: int(batchNum),
			err:      fmt.Errorf("failed to create database connection: %v", err),
		}
	}
	defer batchDB.Close()

	offset := batchNum * bp.batchSize
	query := fmt.Sprintf("SELECT id, %s FROM %s ORDER BY id LIMIT %d OFFSET %d",
		strings.Join(bp.info.Columns, ", "), bp.info.Name, bp.batchSize, offset)

	rows, err := batchDB.Query(query)
	if err != nil {
		return batchResult{
			batchNum: int(batchNum),
			err:      fmt.Errorf("failed to query rows: %v", err),
		}
	}
	defer rows.Close()

	return bp.processRows(rows, batchNum)
}

// processRows processes rows from a database query
func (bp *batchProcessor) processRows(rows *sql.Rows, batchNum int64) batchResult {
	var batchCount int
	var batchChecksum string
	var failedRowIDs []int64

	for rows.Next() {
		values := make([]interface{}, len(bp.info.Columns)+1) // +1 for id
		valuePtrs := make([]interface{}, len(bp.info.Columns)+1)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return batchResult{
				batchNum: int(batchNum),
				err:      fmt.Errorf("failed to scan row: %v", err),
			}
		}

		rowID, ok := values[0].(int64)
		if !ok {
			return batchResult{
				batchNum: int(batchNum),
				err:      fmt.Errorf("invalid row ID type"),
			}
		}

		rowChecksum := calculateRowChecksum(values[1:])

		if bp.mode == ModeRowByRow {
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

	return batchResult{
		batchNum:     int(batchNum),
		count:        batchCount,
		checksum:     batchChecksum,
		err:          nil,
		FailedRowIDs: make([]int64, 0),
	}
}

// CalculateChecksum calculates the checksum for a given table in batches
func CalculateChecksum(config *Config, tableName string, mode Mode) (*ValidationResult, error) {
	// Create database connector
	connector := newDBConnector(config)

	// Connect to database
	db, err := connector.connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Create TableMetaProvider
	metaProvider := metadata.NewTableMetaProvider(db, &metadata.Config{
		Host:     config.Host,
		Port:     config.Port,
		User:     config.User,
		Password: config.Password,
		Database: config.Database,
	})

	// Get table information
	info, err := metaProvider.GetTableInfo(tableName)
	if err != nil {
		return nil, err
	}

	// Create batch processor
	processor := newBatchProcessor(connector, info, mode)

	// Calculate number of batches
	numBatches := (info.RowCount + processor.batchSize - 1) / processor.batchSize

	// Process batches
	results := make([]batchResult, numBatches)
	var wg sync.WaitGroup

	for i := int64(0); i < numBatches; i++ {
		wg.Add(1)
		go func(batchNum int64) {
			defer wg.Done()
			results[batchNum] = processor.processBatch(batchNum)
		}(i)
	}

	wg.Wait()

	// Collect and return results
	return collectResults(results)
}

// collectResults collects and processes batch results
func collectResults(results []batchResult) (*ValidationResult, error) {
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