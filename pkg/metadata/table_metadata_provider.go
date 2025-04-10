package metadata

import (
	"database/sql"
	"fmt"
)

// TableMetaProvider handles fetching table metadata
type TableMetaProvider struct {
	DbProvider     *DbConnProvider
	TableName      string
	TableInfoCache *TableInfo
}

// NewTableMetaProvider creates a new TableMetaProvider.
func NewTableMetaProvider(dbProvider *DbConnProvider, tableName string) *TableMetaProvider {
	return &TableMetaProvider{DbProvider: dbProvider, TableName: tableName}
}

// verifyTableExists checks if the specified table exists in the database
func (p *TableMetaProvider) verifyTableExists(tableName string) (bool, error) {
	var count int
	err := p.getDbConn().QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		p.GetDatabaseName(), tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %v", err)
	}
	if count == 0 {
		return false, fmt.Errorf("table %s does not exist", tableName)
	}
	return true, nil
}

// getTableRowCount retrieves the total number of rows in the table
func (p *TableMetaProvider) getTableRowCount(tableName string) (int64, error) {
	var totalRows int64
	err := p.getDbConn().QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&totalRows)
	if err != nil {
		return 0, fmt.Errorf("failed to get total row count: %v", err)
	}
	return totalRows, nil
}

// getTableColumns retrieves all column names for the table
func (p *TableMetaProvider) getTableColumns(tableName string) ([]string, error) {
	rows, err := p.getDbConn().Query(`
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = ? 
		AND table_name = ? 
		ORDER BY ordinal_position`,
		p.GetDatabaseName(), tableName)
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
func (p *TableMetaProvider) getPrimaryKeyInfo(tableName string) (string, error) {
	// Get primary key column name
	var pkColumn string
	err := p.getDbConn().QueryRow(`
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = ? 
		AND table_name = ? 
		AND column_key = 'PRI'`,
		p.GetDatabaseName(), tableName).Scan(&pkColumn)
	if err != nil {
		return "", fmt.Errorf("failed to get primary key column: %v", err)
	}

	return pkColumn, nil
}

func (p *TableMetaProvider) queryTablePKRange(primaryKey string, tableName string) (*PKRange, error) {
	var minPK, maxPK int64
	queryMinMax := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`", primaryKey, primaryKey, tableName)
	err := p.getDbConn().QueryRow(queryMinMax).Scan(&minPK, &maxPK)
	if err != nil {
		// Handle cases like empty table (though checked earlier) or non-numeric PK if assumption is wrong
		if err == sql.ErrNoRows {
			return EmptyPKRange, err
		}
		// A simple scan might fail if MIN/MAX return NULL (empty table) or non-integer types.
		// Need more robust scanning if PK isn't guaranteed to be non-null integer.
		var minPKNullable, maxPKNullable sql.NullInt64
		errRetry := p.getDbConn().QueryRow(queryMinMax).Scan(&minPKNullable, &maxPKNullable)
		if errRetry != nil {
			return EmptyPKRange, fmt.Errorf("failed to query min/max primary key for %s: %w", tableName, errRetry)
		}
		if !minPKNullable.Valid || !maxPKNullable.Valid {
			// This implies the table is empty, contradicting rowCount > 0 potentially.
			// Or the PK is not suitable for MIN/MAX (e.g., all NULLs, though unlikely for PK).
			return EmptyPKRange, fmt.Errorf("primary key MIN/MAX returned NULL for table %s, check table state", tableName)
		}
		minPK = minPKNullable.Int64
		maxPK = maxPKNullable.Int64
	}

	if maxPK < minPK {
		// Should not happen in a normal table with rows
		return EmptyPKRange, fmt.Errorf("max primary key (%d) is less than min primary key (%d) for table %s", maxPK, minPK, tableName)
	}
	return NewPKRange(minPK, maxPK), nil
}

// queryTableInfo retrieves table information by coordinating multiple operations
func (p *TableMetaProvider) queryTableInfo(tableName string) (*TableInfo, error) {
	// First verify table exists
	exists, err := p.verifyTableExists(tableName)
	if err != nil {
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
	pkColumn, err := p.getPrimaryKeyInfo(tableName)
	if err != nil {
		return nil, err
	}

	// Get table pk range info
	pkRange, err := p.queryTablePKRange(pkColumn, tableName)
	if err != nil {
		return nil, err
	}

	return &TableInfo{
		DatabaseName: p.GetDatabaseName(),
		TableName:    tableName,
		Columns:      columns,
		RowCount:     rowCount,
		PrimaryKey:   pkColumn,
		PKRange:      pkRange,
		TableExists:  exists,
	}, nil
}

// GetTableInfo retrieves table information by coordinating multiple operations
func (p *TableMetaProvider) GetTableInfo(tableName string) (*TableInfo, error) {
	if p.TableInfoCache != nil {
		return p.TableInfoCache, nil
	} else {
		return p.queryTableInfo(tableName)
	}
}

func (p *TableMetaProvider) getDbConn() *sql.DB {
	return p.DbProvider.GetDbConn()
}

func (p *TableMetaProvider) GetDatabaseName() string {
	return p.DbProvider.databaseName
}
