package metadata

import (
	"database/sql"
	"fmt"
)

// TableInfo holds information about a database table
type TableInfo struct {
	Name     string
	Columns  []string
	RowCount int64
	PKColumn string
	MinPK    int64
	MaxPK    int64
}

// TableMetaProvider handles fetching table metadata
type TableMetaProvider struct {
	db     *sql.DB
	config *Config
}

// Config holds the MySQL connection configuration
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// NewTableMetaProvider creates a new TableMetaProvider
func NewTableMetaProvider(db *sql.DB, config *Config) *TableMetaProvider {
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

// getPrimaryKeyInfo retrieves primary key column and its min/max values
func (p *TableMetaProvider) getPrimaryKeyInfo(tableName string) (string, int64, int64, error) {
	// Get primary key column name
	var pkColumn string
	err := p.db.QueryRow(`
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = ? 
		AND table_name = ? 
		AND column_key = 'PRI'`,
		p.config.Database, tableName).Scan(&pkColumn)
	if err != nil {
		return "", 0, 0, fmt.Errorf("failed to get primary key column: %v", err)
	}

	// Get min and max primary key values
	var minPK, maxPK int64
	err = p.db.QueryRow(fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", pkColumn, pkColumn, tableName)).
		Scan(&minPK, &maxPK)
	if err != nil {
		return "", 0, 0, fmt.Errorf("failed to get primary key range: %v", err)
	}

	return pkColumn, minPK, maxPK, nil
}

// GetTableInfo retrieves table information by coordinating multiple operations
func (p *TableMetaProvider) GetTableInfo(tableName string) (*TableInfo, error) {
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

	// Get primary key information
	pkColumn, minPK, maxPK, err := p.getPrimaryKeyInfo(tableName)
	if err != nil {
		return nil, err
	}

	return &TableInfo{
		Name:     tableName,
		Columns:  columns,
		RowCount: rowCount,
		PKColumn: pkColumn,
		MinPK:    minPK,
		MaxPK:    maxPK,
	}, nil
} 