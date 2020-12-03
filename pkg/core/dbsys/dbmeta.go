package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"unsafe"
)

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
