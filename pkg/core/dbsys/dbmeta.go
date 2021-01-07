package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"unsafe"
)

// currently AttrInfo has been split into expr.AttrInfo & types.AttrInfo
//type AttrInfo struct {
//	AttrSize             int // used by expr::NodeAttr
//	AttrOffset           int // used by expr::NodeAttr
//	AttrType             ValueType
//	NullAllowed          bool // used by system manager
//}

var TableDescribeColumn = []string{
	"Field",
	"Type",
	"Size",
	"Offset",
	"IndexName",
	"Null",
	"IsPrimary",
	"HasForeignConstraint",
	"Default",
}

const AttrInfoSize = int(unsafe.Sizeof(parser.AttrInfo{}))
const RelInfoSize = int(unsafe.Sizeof(RelInfo{}))

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
