package checksum

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"mychecksum/pkg/metadata"
	"strings"
)

type PartitionChecksumCalculator struct {
	Partition *metadata.Partition
}

func NewPartitionChecksumCalculator(partition *metadata.Partition) *PartitionChecksumCalculator {
	return &PartitionChecksumCalculator{Partition: partition}
}

// CalculateChecksum calculates the checksum for the partition.
func (p *PartitionChecksumCalculator) CalculateChecksum(dbConn *sql.DB, columns []string) (string, int64, error) {
	colList := "CONCAT_WS(', ', `" + strings.Join(columns, "`,`") + "`) as concatenated_columns"
	pkCol := "`" + p.Partition.TableInfo.PrimaryKey + "`"
	// Ensure StartPK and EndPK are handled correctly based on their actual type (e.g., int64)
	query := fmt.Sprintf("SELECT %s, %s FROM `%s` WHERE %s >= ? AND %s <= ? ORDER BY %s",
		pkCol, colList, p.Partition.TableInfo.TableName, pkCol, pkCol, pkCol) // ORDER BY PK is crucial
	rows, err := dbConn.Query(query, p.Partition.GetStart(), p.Partition.GetEnd())
	if err != nil {
		return "", 0, fmt.Errorf("partition %d: failed to query rows (PK >= %v AND PK <= %v): %w",
			p.Partition.Index, p.Partition.GetStart(), p.Partition.GetEnd(), err)
	}
	defer rows.Close()

	partitionHasher := sha256.New()
	var rowCount int64 = 0
	var concatenatedColumns string

	for rows.Next() {
		if err := rows.Scan(&concatenatedColumns); err != nil {
			return "", 0, fmt.Errorf("partition %d: failed to scan row: %w", p.Partition.Index, err)
		}
		partitionHasher.Write([]byte(concatenatedColumns)) // Combine row hashes
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return "", 0, fmt.Errorf("partition %d: error iterating rows: %w", p.Partition.Index, err)
	}

	// Final checksum for the partition is the hash of concatenated row hashes
	finalChecksum := hex.EncodeToString(partitionHasher.Sum(nil))
	return finalChecksum, rowCount, nil
}
