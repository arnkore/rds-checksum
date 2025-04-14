package partition

import (
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/metadata"
)

// PartitionCalculator determines how to divide a table into partitions.
type PartitionCalculator struct {
	TableInfo *metadata.TableInfo
}

// NewPartitionCalculator creates a new PartitionCalculator.
func NewPartitionCalculator(tableInfo *metadata.TableInfo) (*PartitionCalculator, error) {
	if tableInfo.PrimaryKey == "" {
		return nil, fmt.Errorf("cannot partition table %s without a primary key", tableInfo.TableName)
	}
	// Further checks could be added here, e.g., ensuring PK is numeric for range partitioning.
	return &PartitionCalculator{
		TableInfo: tableInfo,
	}, nil
}

// CalculatePartitions divides the table into partitions based on the primary key.
// This is a simplified example assuming a numeric, auto-incrementing PK.
// A more robust implementation would handle different PK types and distributions.
func (pc *PartitionCalculator) CalculatePartitions() ([]metadata.Partition, error) {
	if pc.TableInfo.RowCount == 0 {
		return []metadata.Partition{}, nil // No rows, no partitions needed
	}

	tablePKRange := pc.TableInfo.PKRange
	var numPartitions int
	partitions := make([]metadata.Partition, 0, numPartitions)
	totalRange := tablePKRange.GetTotalRange()
	// Avoid division by zero if numPartitions is 1 and range is 0 (single row table)
	if totalRange == 0 && numPartitions > 0 { // Single distinct PK value
		partitions = append(partitions, metadata.Partition{Index: 1, PKRange: tablePKRange})
		return partitions, nil
	}
	// Ensure partitionSize is at least 1
	partitionSize := totalRange / int64(numPartitions)
	if partitionSize == 0 && totalRange > 0 {
		partitionSize = 1 // Handle case where range < numPartitions
	}

	currentStart := tablePKRange.GetStart()
	for i := 0; i < numPartitions; i++ {
		currentEnd := currentStart + partitionSize - 1
		if i == numPartitions-1 || currentEnd >= tablePKRange.GetEnd() {
			currentEnd = tablePKRange.GetEnd() // Ensure the last partition includes the max PK
		}

		// Add partition only if start <= end. This handles edge cases.
		if currentStart <= currentEnd {
			pkRange := &metadata.PKRange{currentStart, currentEnd}
			partitions = append(partitions, metadata.Partition{
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
	if len(partitions) > 0 {
		lastPartition := partitions[len(partitions)-1]
		if lastPK := lastPartition.PKRange.GetEnd(); lastPK != tablePKRange.GetEnd() {
			// This might indicate a logic error or edge case not handled
			fmt.Printf("Warning: Last partition end PK %d does not match max PK %d for table %s\n", lastPK, tablePKRange.GetEnd(), pc.TableInfo.TableName)
			// Optionally adjust the last partition's EndPK here if necessary
			// partitions[len(partitions)-1].EndPK = maxPK
		}
	} else if pc.TableInfo.RowCount > 0 {
		// If we have rows but generated no partitions, something is wrong
		return nil, fmt.Errorf("failed to generate partitions for table %s despite having %d rows", pc.TableInfo.TableName, pc.TableInfo.RowCount)
	}

	return partitions, nil
}
