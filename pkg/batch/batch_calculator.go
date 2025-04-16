package batch

import (
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/metadata"
)

// BatchCalculator determines how to divide a table into batches.
type BatchCalculator struct {
	TableInfo    *metadata.TableInfo
	RowsPerBatch int
}

// NewBatchCalculator creates a new BatchCalculator.
func NewBatchCalculator(tableInfo *metadata.TableInfo, rowsPerBatch int) (*BatchCalculator, error) {
	if tableInfo.PrimaryKey == "" {
		return nil, fmt.Errorf("cannot batch table %s without a primary key", tableInfo.TableName)
	}
	// Further checks could be added here, e.g., ensuring PK is numeric for range partitioning.
	return &BatchCalculator{
		TableInfo:    tableInfo,
		RowsPerBatch: rowsPerBatch,
	}, nil
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
			fmt.Printf("Warning: Last batch end PK %d does not match max PK %d for table %s\n", lastPK, tablePKRange.GetEnd(), pc.TableInfo.TableName)
		}
	} else if pc.TableInfo.RowCount > 0 {
		// If we have rows but generated no batches, something is wrong
		return nil, fmt.Errorf("failed to generate batches for table %s despite having %d rows", pc.TableInfo.TableName, pc.TableInfo.RowCount)
	}

	return batches, nil
}
