package checksum

import (
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	NilRetryMap map[int64]*RetryCheckRow
)

type RowChecksum struct {
	RetryRowMap        map[int64]*RetryCheckRow
	PrimaryKey         int64
	TableInfo          *metadata.TableInfo
	SrcConnProvider    *metadata.DbConnProvider
	TargetConnProvider *metadata.DbConnProvider
	CalcCRC32InDB      bool
}

type RetryCheckRow struct {
	PKey           int64
	RetryTimes     *atomic.Int32
	lastCheckTime  *time.Time
	firstCheckTime *time.Time
}

func NewRowChecksum(retryRowMap map[int64]*RetryCheckRow, tableInfo *metadata.TableInfo,
	srcConnProvider, targetConnProvider *metadata.DbConnProvider, calcCRC32InDB bool) *RowChecksum {
	return &RowChecksum{
		RetryRowMap:        retryRowMap,
		TableInfo:          tableInfo,
		SrcConnProvider:    srcConnProvider,
		TargetConnProvider: targetConnProvider,
		CalcCRC32InDB:      calcCRC32InDB,
	}
}

func (rc *RowChecksum) getInconsistentPKeys() []int64 {
	inconsistentPKeys := make([]int64, len(rc.RetryRowMap))
	for pKey := range rc.RetryRowMap {
		inconsistentPKeys = append(inconsistentPKeys, pKey)
	}
	return inconsistentPKeys
}

func (rc *RowChecksum) CalculateAndCompareChecksum(columns []string) (map[int64]*RetryCheckRow, error) {
	var srcChecksum, targetChecksum uint32
	var srcChecksumMap, targetChecksumMap map[int64]uint32
	var srcCheckTime time.Time
	var srcErr, targetErr error
	var wgPart sync.WaitGroup
	wgPart.Add(2)

	go func() {
		defer wgPart.Done()
		srcChecksum, srcChecksumMap, srcErr = rc.CalculateSourceChecksum(columns)
		srcCheckTime = time.Now()
	}()
	go func() {
		defer wgPart.Done()
		targetChecksum, targetChecksumMap, targetErr = rc.CalculateTargetChecksum(columns)
	}()
	wgPart.Wait()

	if srcErr != nil {
		return NilRetryMap, srcErr
	}
	if targetErr != nil {
		return NilRetryMap, targetErr
	}

	if srcChecksum == targetChecksum {
		return NilRetryMap, nil
	}
	inconsistentPKeys := CompareChecksumMap(srcChecksumMap, targetChecksumMap)
	retryRowsMap := make(map[int64]*RetryCheckRow, len(inconsistentPKeys))
	for _, pkey := range inconsistentPKeys {
		oldRetryRow := rc.RetryRowMap[pkey]
		oldRetryRow.RetryTimes.And(1)
		oldRetryRow.lastCheckTime = &srcCheckTime
		retryRowsMap[pkey] = oldRetryRow
	}
	return retryRowsMap, nil
}

func (rc *RowChecksum) CalculateSourceChecksum(columns []string) (uint32, map[int64]uint32, error) {
	return rc.calculateChecksum(rc.SrcConnProvider, columns)
}

func (rc *RowChecksum) CalculateTargetChecksum(columns []string) (uint32, map[int64]uint32, error) {
	return rc.calculateChecksum(rc.TargetConnProvider, columns)
}

func (rc *RowChecksum) calculateChecksum(connProvider *metadata.DbConnProvider, columns []string) (uint32, map[int64]uint32, error) {
	var bitXorOfChecksum uint32
	var checksumMap map[int64]uint32
	var err error

	if rc.CalcCRC32InDB {
		bitXorOfChecksum, checksumMap, err = rc.calculateChecksumInDB(connProvider, columns)
	} else {
		bitXorOfChecksum, checksumMap, err = rc.calculateChecksumOutOfDB(connProvider, columns)
	}
	return bitXorOfChecksum, checksumMap, err
}

// calculateChecksumInDB calculates the checksum for the batch using database functions.
func (rc *RowChecksum) calculateChecksumOutOfDB(connProvider *metadata.DbConnProvider, columns []string) (uint32, map[int64]uint32, error) {
	dbConn, err := connProvider.CreateDbConn()
	if err != nil {
		return 0, nil, fmt.Errorf("row checksum: failed to create DB connection: %w", err)
	}
	defer connProvider.Close(dbConn)

	concateAllColumns := ConcateQuotedColumns(columns)
	pkCol := "`" + rc.TableInfo.PrimaryKey + "`"
	pkValList := strings.Join(ConvertIntArrToStringArr(rc.getInconsistentPKeys()), ",")
	// Query to calculate pk and CRC32(CONCAT_WS(all_columns)) in the database
	queryTemplate := fmt.Sprintf("SELECT %s, CONCAT_WS(',', %s) FROM `%s` WHERE %s in (%s)",
		pkCol, concateAllColumns, rc.TableInfo.TableName, pkCol, pkValList)
	rows, err := dbConn.Query(queryTemplate)
	if err != nil {
		return 0, nil, fmt.Errorf("row checksum: failed to query checksum: %w", err)
	}
	defer rows.Close()

	var checksumMap = make(map[int64]uint32)
	var bitXorOfChecksum uint32 = 0
	for rows.Next() {
		var pkVal int64
		var concateAllColumnsVal string
		if err := rows.Scan(&pkVal, &concateAllColumnsVal); err != nil {
			return 0, nil, fmt.Errorf("row checksum: failed to scan row: %w", err)
		}
		rowChecksum := CalculateCRC32(concateAllColumnsVal)
		checksumMap[pkVal] = rowChecksum
		bitXorOfChecksum ^= rowChecksum
	}
	return bitXorOfChecksum, checksumMap, nil
}

// calculateChecksumInDB calculates the checksum for the batch using database functions.
func (rc *RowChecksum) calculateChecksumInDB(connProvider *metadata.DbConnProvider, columns []string) (uint32, map[int64]uint32, error) {
	dbConn, err := connProvider.CreateDbConn()
	if err != nil {
		return 0, nil, fmt.Errorf("row checksum: failed to create DB connection: %w", err)
	}
	defer connProvider.Close(dbConn)

	concateAllColumns := ConcateQuotedColumns(columns)
	pkCol := "`" + rc.TableInfo.PrimaryKey + "`"
	pkValList := strings.Join(ConvertIntArrToStringArr(rc.getInconsistentPKeys()), ",")
	// Query to calculate pk and CRC32(CONCAT_WS(all_columns)) in the database
	queryTemplate := fmt.Sprintf("SELECT %s, CRC32(CONCAT_WS(',', %s)) FROM `%s` WHERE %s in (%s)",
		pkCol, concateAllColumns, rc.TableInfo.TableName, pkCol, pkValList)
	rows, err := dbConn.Query(queryTemplate)
	if err != nil {
		return 0, nil, fmt.Errorf("row checksum: failed to query checksum: %w", err)
	}
	defer rows.Close()

	var checksumMap = make(map[int64]uint32)
	var bitXorOfChecksum uint32 = 0
	for rows.Next() {
		var pkVal int64
		var checksum uint32
		if err := rows.Scan(&pkVal, &checksum); err != nil {
			return 0, nil, fmt.Errorf("row checksum: failed to scan row: %w", err)
		}
		checksumMap[pkVal] = checksum
		bitXorOfChecksum ^= checksum
	}
	return bitXorOfChecksum, checksumMap, nil
}
