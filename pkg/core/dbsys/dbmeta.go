package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"unsafe"
)

// currently AttrInfo has been split into expr.AttrInfo & types.AttrInfo
//type AttrInfo struct {
//	types.AttrInfo
//	AttrName [types.MaxNameSize]byte
//	RelName  [types.MaxNameSize]byte // 24 * 2
//	Default  types.Value
//}

const offsetAttrName = int(unsafe.Sizeof(types.AttrInfo{}))
const offsetRelName = offsetAttrName + types.MaxNameSize
const offsetDefault = offsetRelName + types.MaxNameSize

const offsetAttrSize = int(0)
const offsetAttrOffset = offsetAttrSize + 8
const offsetAttrType = offsetAttrOffset + 8
const offsetIndexNo = offsetAttrType + 8
const offsetConstraint = offsetIndexNo + 8
const offsetNull = offsetConstraint + int(unsafe.Sizeof(types.RID{}))
const offsetPrimary = offsetNull + 1
const offsetFK = offsetNull + 1

var TableDescribeColumn = []string{
	"Field",
	"Type",
	"Size",
	"Offset",
	"IndexNo",
	"Null",
	"IsPrimary",
	"HasForeignConstraint",
	"Default",
}

const AttrInfoSize = int(unsafe.Sizeof(parser.AttrInfo{}))
const RelInfoSize = int(unsafe.Sizeof(RelInfo{}))

type RelInfo struct {
	relName      [types.MaxNameSize]byte
	recordSize   int
	attrCount    int
	nextIndexNo  int
	indexCount   int // index constraint count
	primaryCount int // primary constraint count
	foreignCount int // foreign constraint count
}

type IndexInfo struct {
	idxNo   int
	idxName [types.MaxNameSize]byte
	col     [types.MaxNameSize]byte
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
