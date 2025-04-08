package checksum

import (
	"database/sql"
	"fmt"
	"strings"

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

// CalculateChecksum calculates the checksum for a given table
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

	// Build the checksum query
	query := fmt.Sprintf("SELECT COUNT(*) as count, SUM(CRC32(CONCAT_WS('#', %s))) as checksum FROM %s", strings.Join(columns, ", "), table)
	
	var countRows int64
	var checksum int64
	err = db.QueryRow(query).Scan(&countRows, &checksum)
	if err != nil {
		return "", fmt.Errorf("failed to calculate checksum: %v", err)
	}

	return fmt.Sprintf("%d-%d", countRows, checksum), nil
}

// CompareChecksums compares two checksums and returns true if they match
func CompareChecksums(checksum1, checksum2 string) bool {
	return checksum1 == checksum2
} 