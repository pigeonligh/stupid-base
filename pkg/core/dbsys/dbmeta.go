package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"unsafe"
)

const offsetAttrName = 0
const offsetRelName = offsetAttrName + types.MaxNameSize
const offsetAttrSize = offsetRelName + types.MaxNameSize
const offsetAttrOffset = offsetAttrSize + 8
const offsetAttrType = offsetAttrOffset + 8
const offsetIndexNo = offsetAttrType + 8
const offsetConstraint = offsetIndexNo + 8 // size of above equals 96 (including constraint RID)
const offsetNull = 96                      // these 3 bit takes up 8 bytes, it seems it's 8 byte alignment
const offsetPrimary = 97
const offsetFK = 98 // foreign key
const offsetDefault = 104

// defined in parser/expr
//type AttrInfo struct {
//	AttrName             [types.MaxNameSize]byte
//	RelName              [types.MaxNameSize]byte //24 * 2
//	AttrSize             int                     // used by expr::NodeAttr
//	AttrOffset           int                     // used by expr::NodeAttr
//	AttrType             types.ValueType
//	IndexNo              int       // used by system manager
//	ConstraintRID        types.RID // used by system manager, deprecated
//	NullAllowed          bool      // used by system manager
//	IsPrimary            bool      // used by system manager
//	HasForeignConstraint bool      // will be checked if necessary
//	Default              Value
//}
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
	indexNo   int
	indexName [types.MaxNameSize]byte
	column    [types.MaxNameSize]byte
}

// used for database query, since only some of the column are selected
type TemporalTable = []TableColumn
type TableColumn struct {
	relName     string
	attrName    string
	attrSize    int
	attrType    int
	nullAllowed bool
	valueList   []parser.Value
}
