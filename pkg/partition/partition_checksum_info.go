package partition

type PartitionChecksumInfo struct {
	SrcChecksum    string
	TargetChecksum string
	SrcRowCount    int64
	TargetRowCount int64
	SrcErr         error
	TargetErr      error
}

func (p *PartitionChecksumInfo) IsRowCountMatch() bool {
	return p.SrcRowCount == p.TargetRowCount
}

func (p *PartitionChecksumInfo) IsChecksumMatch() bool {
	return p.SrcChecksum == p.SrcChecksum
}

func (p *PartitionChecksumInfo) IsOverallMatch() bool {
	return p.IsRowCountMatch() && p.IsChecksumMatch()
}
