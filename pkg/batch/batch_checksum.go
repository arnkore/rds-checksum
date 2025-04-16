package batch

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"strings"
	"sync"
)

type BatchChecksumCalculator struct {
	Batch              *metadata.Batch
	SrcConnProvider    *metadata.DbConnProvider
	TargetConnProvider *metadata.DbConnProvider
}

func NewBatchChecksumCalculator(batch *metadata.Batch, srcConnProvider, targetConnProvider *metadata.DbConnProvider) *BatchChecksumCalculator {
	return &BatchChecksumCalculator{Batch: batch, SrcConnProvider: srcConnProvider, TargetConnProvider: targetConnProvider}
}

func (p *BatchChecksumCalculator) calculateSourceChecksum(columns []string) (string, int64, error) {
	return p.calculateChecksum(p.SrcConnProvider, columns)
}

func (p *BatchChecksumCalculator) calculateTargetChecksum(columns []string) (string, int64, error) {
	return p.calculateChecksum(p.TargetConnProvider, columns)
}

// calculateChecksum calculates the checksum for the batch.
func (p *BatchChecksumCalculator) calculateChecksum(connProvider *metadata.DbConnProvider, columns []string) (string, int64, error) {
	dbConn, err := connProvider.CreateDbConn()
	defer connProvider.Close(dbConn)
	colList := "CONCAT_WS(', ', `" + strings.Join(columns, "`,`") + "`) as concatenated_columns"
	pkCol := "`" + p.Batch.TableInfo.PrimaryKey + "`"
	// Ensure StartPK and EndPK are handled correctly based on their actual type (e.g., int64)
	query := fmt.Sprintf("SELECT %s, %s FROM `%s` WHERE %s >= ? AND %s <= ? ORDER BY %s",
		pkCol, colList, p.Batch.TableInfo.TableName, pkCol, pkCol, pkCol) // ORDER BY PK is crucial
	rows, err := dbConn.Query(query, p.Batch.GetStart(), p.Batch.GetEnd())
	if err != nil {
		return "", 0, fmt.Errorf("batch %d: failed to query rows (PK >= %v AND PK <= %v): %w",
			p.Batch.Index, p.Batch.GetStart(), p.Batch.GetEnd(), err)
	}
	defer rows.Close()

	batchHasher := sha256.New()
	var rowCount int64 = 0
	var pkVal int64 = 0
	var concatenatedColumns string

	for rows.Next() {
		if err := rows.Scan(&pkVal, &concatenatedColumns); err != nil {
			return "", 0, fmt.Errorf("batch %d: failed to scan row: %w", p.Batch.Index, err)
		}
		batchHasher.Write([]byte(concatenatedColumns)) // Combine row hashes
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return "", 0, fmt.Errorf("batch %d: error iterating rows: %w", p.Batch.Index, err)
	}

	// Final checksum for the batch is the hash of concatenated row hashes
	finalChecksum := hex.EncodeToString(batchHasher.Sum(nil))
	return finalChecksum, rowCount, nil
}

func (p *BatchChecksumCalculator) CalculateAndCompareChecksum(columns []string) *BatchChecksumInfo {
	var srcBatchChecksum, targetBatchChecksum string
	var srcBatchRowCount, targetBatchRowCount int64
	var srcErr, targetErr error
	var wgPart sync.WaitGroup
	wgPart.Add(2)

	go func() {
		defer wgPart.Done()
		srcBatchChecksum, srcBatchRowCount, srcErr = p.calculateSourceChecksum(columns)
	}()
	go func() {
		defer wgPart.Done()
		targetBatchChecksum, targetBatchRowCount, targetErr = p.calculateTargetChecksum(columns)
	}()
	wgPart.Wait()

	return &BatchChecksumInfo{
		Index:          p.Batch.Index,
		SrcRowCount:    srcBatchRowCount,
		TargetRowCount: targetBatchRowCount,
		SrcChecksum:    srcBatchChecksum,
		TargetChecksum: targetBatchChecksum,
		SrcErr:         srcErr,
		TargetErr:      targetErr,
	}
}

type BatchChecksumInfo struct {
	Index          int
	SrcChecksum    string
	TargetChecksum string
	SrcRowCount    int64
	TargetRowCount int64
	SrcErr         error
	TargetErr      error
}

func (p *BatchChecksumInfo) IsRowCountMatch() bool {
	return p.SrcRowCount == p.TargetRowCount
}

func (p *BatchChecksumInfo) IsChecksumMatch() bool {
	return p.SrcChecksum == p.SrcChecksum
}

func (p *BatchChecksumInfo) IsOverallMatch() bool {
	return p.IsRowCountMatch() && p.IsChecksumMatch()
}
