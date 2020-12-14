package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"unsafe"
)

const ConstraintInfoSize = int(unsafe.Sizeof(ConstraintInfo{}))

type ConstraintInfo struct {
	attrSrc  [types.MaxNameSize]byte // attr in current table
	foreign  ConstraintForeignInfo
	check    ConstraintCheckInfo
	consType types.ConstraintType
}

type ConstraintForeignInfo struct {
	attrDst [types.MaxNameSize]byte // attr in foreign table (must be primary)
	relDst  [types.MaxNameSize]byte // foreign table(relation) name
}

type ConstraintCheckInfo struct {
	value  parser.Value
	compOp types.OpType // must be a comparison op for check in constraint
}
