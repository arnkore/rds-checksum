package batch

import (
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/arnkore/rds-checksum/pkg/utils"
	"sync"
)

type BatchChecksumCalculator struct {
	Batch              *metadata.Batch
	SrcConnProvider    *metadata.DbConnProvider
	TargetConnProvider *metadata.DbConnProvider
	CalcCRC32InDB      bool
}

func NewBatchChecksumCalculator(batch *metadata.Batch, calcCrc32InDb bool, srcConnProvider, targetConnProvider *metadata.DbConnProvider) *BatchChecksumCalculator {
	return &BatchChecksumCalculator{Batch: batch, CalcCRC32InDB: calcCrc32InDb, SrcConnProvider: srcConnProvider, TargetConnProvider: targetConnProvider}
}

func (p *BatchChecksumCalculator) CalculateSourceChecksum(columns []string) (int, uint32, error) {
	return p.calculateChecksum(p.SrcConnProvider, columns)
}

func (p *BatchChecksumCalculator) CalculateTargetChecksum(columns []string) (int, uint32, error) {
	return p.calculateChecksum(p.TargetConnProvider, columns)
}

func (p *BatchChecksumCalculator) calculateChecksum(connProvider *metadata.DbConnProvider, columns []string) (int, uint32, error) {
	var rowCount int
	var bitXorOfChecksum uint32
	var err error

	if p.CalcCRC32InDB {
		rowCount, bitXorOfChecksum, _, err = p.calculateChecksumInDB(connProvider, columns)
	} else {
		rowCount, bitXorOfChecksum, _, err = p.calculateChecksumOutOfDB(connProvider, columns)
	}
	return rowCount, bitXorOfChecksum, err
}

// calculateChecksumOutOfDB calculates the checksum for the batch.
func (p *BatchChecksumCalculator) calculateChecksumOutOfDB(connProvider *metadata.DbConnProvider, columns []string) (int, uint32, map[int64]uint32, error) {
	dbConn, err := connProvider.CreateDbConn()
	if err != nil {
		return 0, 0, nil, fmt.Errorf("batch %d: failed to create DB connection: %w", p.Batch.Index, err)
	}
	defer connProvider.Close(dbConn)

	concateAllColumns := utils.ConcateQuotedColumns(columns)
	pkCol := "`" + p.Batch.TableInfo.PrimaryKey + "`"
	// Query to calculate pk and CONCAT_WS(all_columns) in the database
	queryTemplate := fmt.Sprintf("SELECT %s, CONCAT_WS(',', %s) FROM `%s` WHERE %s >= ? AND %s <= ?",
		pkCol, concateAllColumns, p.Batch.TableInfo.TableName, pkCol, pkCol)
	rows, err := dbConn.Query(queryTemplate, p.Batch.GetStart(), p.Batch.GetEnd())
	if err != nil {
		return 0, 0, nil, fmt.Errorf("batch %d: failed to query checksum (PK >= %v AND PK <= %v): %w",
			p.Batch.Index, p.Batch.GetStart(), p.Batch.GetEnd(), err)
	}

	var checksumMap = make(map[int64]uint32)
	var bitXorOfChecksum uint32 = 0
	for rows.Next() {
		var pkVal int64
		var concateAllColumnsVal string
		if err := rows.Scan(&pkVal, &concateAllColumnsVal); err != nil {
			return 0, 0, nil, fmt.Errorf("batch %d: failed to scan row: %w", p.Batch.Index, err)
		}
		rowChecksum := utils.CalculateCRC32(concateAllColumnsVal)
		checksumMap[pkVal] = rowChecksum
		bitXorOfChecksum ^= rowChecksum
	}
	return len(checksumMap), bitXorOfChecksum, checksumMap, nil
}

// calculateChecksumInDB calculates the checksum for the batch using database functions.
func (p *BatchChecksumCalculator) calculateChecksumInDB(connProvider *metadata.DbConnProvider, columns []string) (int, uint32, map[int64]uint32, error) {
	dbConn, err := connProvider.CreateDbConn()
	if err != nil {
		return 0, 0, nil, fmt.Errorf("batch %d: failed to create DB connection: %w", p.Batch.Index, err)
	}
	defer connProvider.Close(dbConn)

	concateAllColumns := utils.ConcateQuotedColumns(columns)
	pkCol := "`" + p.Batch.TableInfo.PrimaryKey + "`"
	// Query to calculate pk and CRC32(CONCAT_WS(all_columns)) in the database
	queryTemplate := fmt.Sprintf("SELECT %s, CRC32(CONCAT_WS(',', %s)) FROM `%s` WHERE %s >= ? AND %s <= ?",
		pkCol, concateAllColumns, p.Batch.TableInfo.TableName, pkCol, pkCol)
	rows, err := dbConn.Query(queryTemplate, p.Batch.GetStart(), p.Batch.GetEnd())
	if err != nil {
		return 0, 0, nil, fmt.Errorf("batch %d: failed to query checksum (PK >= %v AND PK <= %v): %w",
			p.Batch.Index, p.Batch.GetStart(), p.Batch.GetEnd(), err)
	}
	defer rows.Close()

	var checksumMap = make(map[int64]uint32)
	var bitXorOfChecksum uint32 = 0
	for rows.Next() {
		var pkVal int64
		var checksum uint32
		if err := rows.Scan(&pkVal, &checksum); err != nil {
			return 0, 0, nil, fmt.Errorf("batch %d: failed to scan row: %w", p.Batch.Index, err)
		}
		checksumMap[pkVal] = checksum
		bitXorOfChecksum ^= checksum
	}
	return len(checksumMap), bitXorOfChecksum, checksumMap, nil
}

func (p *BatchChecksumCalculator) CalculateAndCompareChecksum(columns []string) *BatchChecksumInfo {
	var srcBatchChecksum, targetBatchChecksum uint32
	var srcBatchRowCount, targetBatchRowCount int
	var srcErr, targetErr error
	var wgPart sync.WaitGroup
	wgPart.Add(2)

	go func() {
		defer wgPart.Done()
		srcBatchRowCount, srcBatchChecksum, srcErr = p.CalculateSourceChecksum(columns)
	}()
	go func() {
		defer wgPart.Done()
		targetBatchRowCount, targetBatchChecksum, targetErr = p.CalculateTargetChecksum(columns)
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
	Index             int
	SrcChecksum       uint32
	TargetChecksum    uint32
	SrcRowCount       int
	TargetRowCount    int
	SrcChecksumMap    map[int64]uint32
	TargetChecksumMap map[int64]uint32
	SrcErr            error
	TargetErr         error
}

func (p *BatchChecksumInfo) IsRowCountMatch() bool {
	return p.SrcRowCount == p.TargetRowCount
}

func (p *BatchChecksumInfo) IsChecksumMatch() bool {
	return p.SrcChecksum == p.TargetChecksum
}

func (p *BatchChecksumInfo) IsOverallMatch() bool {
	return p.IsRowCountMatch() && p.IsChecksumMatch()
}
