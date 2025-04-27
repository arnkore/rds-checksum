package batch

import (
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	log "github.com/sirupsen/logrus"
)

// BatchCalculator determines how to divide a table into batches.
type BatchCalculator struct {
	TableInfo    *metadata.TableInfo
	RowsPerBatch int
}

// NewBatchCalculator creates a new BatchCalculator.
func NewBatchCalculator(tableInfo *metadata.TableInfo, rowsPerBatch int) *BatchCalculator {
	return &BatchCalculator{
		TableInfo:    tableInfo,
		RowsPerBatch: rowsPerBatch,
	}
}

// CalculateBatches divides the table into batches based on the primary key.
// This is a simplified example assuming a numeric, auto-incrementing PK.
// A more robust implementation would handle different PK types and distributions.
func (pc *BatchCalculator) CalculateBatches() ([]metadata.Batch, error) {
	tablePKRange := pc.TableInfo.PKRange
	totalRange := tablePKRange.GetTotalRange()
	if totalRange == 0 || pc.TableInfo.RowCount == 0 {
		return []metadata.Batch{}, nil // No rows, no batches needed
	}

	var numBatches = int(totalRange / int64(pc.RowsPerBatch))
	batches := make([]metadata.Batch, 0, numBatches)
	currentStart := tablePKRange.GetStart()
	for i := 0; i < numBatches; i++ {
		currentEnd := currentStart + int64(pc.RowsPerBatch) - 1
		if i == numBatches-1 || currentEnd >= tablePKRange.GetEnd() {
			// Ensure the last batch includes the max PK
			currentEnd = tablePKRange.GetEnd()
		}

		// Add batch only if start <= end. This handles edge cases.
		if currentStart <= currentEnd {
			pkRange := &metadata.PKRange{currentStart, currentEnd}
			batches = append(batches, metadata.Batch{
				TableInfo: pc.TableInfo,
				Index:     i + 1,
				PKRange:   pkRange,
				// RowCount estimation could be added here if needed
			})
		}

		currentStart = currentEnd + 1
		if currentStart > tablePKRange.GetEnd() {
			break // Stop if we've covered the entire range
		}
	}

	// Basic validation: Ensure we didn't create empty ranges or exceed maxPK inappropriately
	if len(batches) > 0 {
		lastBatch := batches[len(batches)-1]
		if lastPK := lastBatch.PKRange.GetEnd(); lastPK != tablePKRange.GetEnd() {
			// This might indicate a logic error or edge case not handled
			log.WithFields(log.Fields{
				"last_batch_end_pk": lastPK,
				"table_max_pk":      tablePKRange.GetEnd(),
				"table_name":        pc.TableInfo.TableName,
			}).Warn("Last batch end PK does not match max PK for table")
		}
	} else if pc.TableInfo.RowCount > 0 {
		// If we have rows but generated no batches, something is wrong
		return nil, fmt.Errorf("failed to generate batches for table %s despite having %d rows", pc.TableInfo.TableName, pc.TableInfo.RowCount)
	}

	return batches, nil
}
