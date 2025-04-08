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
	db     *sql.DB
}

// newDBConnector creates a new database connector
func newDBConnector(config *Config) (*dbConnector, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.User, config.Password, config.Host, config.Port, config.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	return &dbConnector{
		config: config,
		dsn:    dsn,
		db:     db,
	}, nil
}

// connect creates a new database connection
func (dc *dbConnector) connect() (*sql.DB, error) {
	return dc.db, nil
}

// batchProcessor handles the processing of data in batches
type batchProcessor struct {
	connector *dbConnector
	info      *metadata.TableInfo
	mode      Mode
	batchSize int64
	pkRanges  []metadata.PKRange
}

// newBatchProcessor creates a new batch processor
func newBatchProcessor(connector *dbConnector, info *metadata.TableInfo, mode Mode) (*batchProcessor, error) {
	// Get primary key ranges
	pkRanges, err := metadata.NewTableMetaProvider(connector.db, &metadata.Config{
		Host:     connector.config.Host,
		Port:     connector.config.Port,
		User:     connector.config.User,
		Password: connector.config.Password,
		Database: connector.config.Database,
	}).GetPKRanges(info.Name, 10000)
	if err != nil {
		return nil, err
	}

	return &batchProcessor{
		connector: connector,
		info:      info,
		mode:      mode,
		batchSize: 10000,
		pkRanges:  pkRanges,
	}, nil
}

// processBatch processes a single batch of data
func (bp *batchProcessor) processBatch(batchNum int64) batchResult {
	if int(batchNum) >= len(bp.pkRanges) {
		return batchResult{
			batchNum: int(batchNum),
			err:      fmt.Errorf("batch number %d out of range", batchNum),
		}
	}

	// Create a new connection for this batch
	batchDB, err := bp.connector.connect()
	if err != nil {
		return batchResult{
			batchNum: int(batchNum),
			err:      fmt.Errorf("failed to create database connection: %v", err),
		}
	}
	defer batchDB.Close()

	pkRange := bp.pkRanges[batchNum]
	query := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s BETWEEN ? AND ? ORDER BY %s",
		bp.info.PKColumn,
		strings.Join(bp.info.Columns, ", "),
		bp.info.Name,
		bp.info.PKColumn,
		bp.info.PKColumn)

	rows, err := batchDB.Query(query, pkRange.StartPK, pkRange.EndPK)
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
		values := make([]interface{}, len(bp.info.Columns)+1) // +1 for primary key
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
	connector, err := newDBConnector(config)
	if err != nil {
		return nil, err
	}

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
	processor, err := newBatchProcessor(connector, info, mode)
	if err != nil {
		return nil, err
	}

	// Process batches
	results := make([]batchResult, len(processor.pkRanges))
	var wg sync.WaitGroup

	for i := 0; i < len(processor.pkRanges); i++ {
		wg.Add(1)
		go func(batchNum int) {
			defer wg.Done()
			results[batchNum] = processor.processBatch(int64(batchNum))
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