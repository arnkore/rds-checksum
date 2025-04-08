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

// tableInfo holds information about a database table
type tableInfo struct {
	name     string
	columns  []string
	rowCount int64
}

// TableMetaProvider handles fetching table metadata
type TableMetaProvider struct {
	db     *sql.DB
	config *Config
}

// newTableMetaProvider creates a new TableMetaProvider
func newTableMetaProvider(db *sql.DB, config *Config) *TableMetaProvider {
	return &TableMetaProvider{
		db:     db,
		config: config,
	}
}

// verifyTableExists checks if the specified table exists in the database
func (p *TableMetaProvider) verifyTableExists(tableName string) error {
	var count int
	err := p.db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		p.config.Database, tableName).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %v", err)
	}
	if count == 0 {
		return fmt.Errorf("table %s does not exist", tableName)
	}
	return nil
}

// getTableRowCount retrieves the total number of rows in the table
func (p *TableMetaProvider) getTableRowCount(tableName string) (int64, error) {
	var totalRows int64
	err := p.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&totalRows)
	if err != nil {
		return 0, fmt.Errorf("failed to get total row count: %v", err)
	}
	return totalRows, nil
}

// getTableColumns retrieves all column names for the table
func (p *TableMetaProvider) getTableColumns(tableName string) ([]string, error) {
	rows, err := p.db.Query(`
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = ? 
		AND table_name = ? 
		ORDER BY ordinal_position`,
		p.config.Database, tableName)
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

	return columns, nil
}

// getTableInfo retrieves table information by coordinating multiple operations
func (p *TableMetaProvider) getTableInfo(tableName string) (*tableInfo, error) {
	// First verify table exists
	if err := p.verifyTableExists(tableName); err != nil {
		return nil, err
	}

	// Get row count
	rowCount, err := p.getTableRowCount(tableName)
	if err != nil {
		return nil, err
	}

	// Get columns
	columns, err := p.getTableColumns(tableName)
	if err != nil {
		return nil, err
	}

	return &tableInfo{
		name:     tableName,
		columns:  columns,
		rowCount: rowCount,
	}, nil
}

// batchProcessor handles the processing of data in batches
type batchProcessor struct {
	connector *dbConnector
	info      *tableInfo
	mode      Mode
	batchSize int64
}

// newBatchProcessor creates a new batch processor
func newBatchProcessor(connector *dbConnector, info *tableInfo, mode Mode) *batchProcessor {
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
		strings.Join(bp.info.columns, ", "), bp.info.name, bp.batchSize, offset)

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
		values := make([]interface{}, len(bp.info.columns)+1) // +1 for id
		valuePtrs := make([]interface{}, len(bp.info.columns)+1)
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
		FailedRowIDs: failedRowIDs,
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
	metaProvider := newTableMetaProvider(db, config)

	// Get table information
	info, err := metaProvider.getTableInfo(tableName)
	if err != nil {
		return nil, err
	}

	// Create batch processor
	processor := newBatchProcessor(connector, info, mode)

	// Calculate number of batches
	numBatches := (info.rowCount + processor.batchSize - 1) / processor.batchSize

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