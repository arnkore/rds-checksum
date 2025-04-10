package metadata

import "fmt"

// TableInfo holds information about a database table
type TableInfo struct {
	DatabaseName string
	TableName    string
	Columns      []string
	RowCount     int64
	PrimaryKey   string
	PKRange      *PKRange
	TableExists  bool
}

func (t *TableInfo) String() string {
	return t.DatabaseName + "." + t.TableName
}

// PKRange represents a range of primary keys
type PKRange struct {
	StartPK int64 // Can be int, string, etc., depending on PK type
	EndPK   int64 // Can be int, string, etc., depending on PK type
}

var (
	EmptyPKRange = &PKRange{-1, -1}
)

func NewPKRange(start, end int64) *PKRange {
	return &PKRange{start, end}
}

func (p *PKRange) GetStart() int64 {
	return p.StartPK
}

func (p *PKRange) GetEnd() int64 {
	return p.EndPK
}

func (p *PKRange) GetTotalRange() int64 {
	return p.EndPK - p.StartPK + 1
}

func (p *PKRange) String() string {
	return fmt.Sprintf("(%v, %v)", p.StartPK, p.EndPK)
}

// Partition represents a range of rows in a table, typically defined by PK range.
type Partition struct {
	TableInfo *TableInfo
	Index     int
	PKRange   *PKRange
	RowCount  int64 // Estimated or actual row count in the partition
}

func (p *Partition) GetStart() int64 {
	return p.PKRange.GetStart()
}

func (p *Partition) GetEnd() int64 {
	return p.PKRange.GetEnd()
}
