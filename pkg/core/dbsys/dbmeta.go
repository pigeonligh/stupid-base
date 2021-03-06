package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

type RelInfo struct {
	RelName      string
	RecordSize   int
	AttrCount    int
	IndexCount   int // index constraint count
	PrimaryCount int // primary constraint count
	ForeignCount int // foreign constraint count
}

type TemporalTable struct {
	rels  []string
	attrs []string
	lens  []int
	offs  []int
	types []types.ValueType
	nils  []bool // nullAllowed
	rows  []*record.Record
}

func (tt *TemporalTable) Count() int {
	return len(tt.rows)
}
