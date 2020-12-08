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
const offsetConstraint = offsetIndexNo + 8	// size of above equals 96 (including constraint RID)
const offsetNull = 96	// these 3 bit takes up 8 bytes, it seems it's 8 byte alignment
const offsetPrimary = 97
const offsetAutoIncre = 98
const offsetDefault = 104


// defined in parser/expr
//type AttrInfo struct {
//	AttrName   		[types.MaxNameSize]byte
//	RelName  		[types.MaxNameSize]byte	//24 * 2
//	AttrSize		int			// used by expr::NodeAttr
//	AttrOffset 		int			// used by expr::NodeAttr
//	AttrType        types.ValueType
//	IndexNo			int			// used by system manager
//	ConstraintRID	types.RID	// used by system manager
//	NullAllowed 	bool 		// used by system manager
//	IsPrimary		bool		// used by system manager
//	AutoIncrement 	bool		// used for auto increasing
//	Default			Value
//}
var TableDescribeColumn = []string {
	"Field",
	"Type",
	"Size",
	"Offset",
	"IndexNo",
	"Null",
	"IsPrimary",
	"AutoIncrement",
	"Default",
}

const AttrInfoSize = int(unsafe.Sizeof(parser.AttrInfo{}))
const RelInfoSize = int(unsafe.Sizeof(RelInfo{}))

type RelInfo struct {
	relName    [types.MaxNameSize]byte
	recordSize int
	attrCount  int
	idxCount   int // idx count ?
	consCount  int // constraint count
}
