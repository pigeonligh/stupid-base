package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"unsafe"
)

type ConstraintType int

const (
	ConstraintPrimary ConstraintType = iota
	ConstraintForeign
	ConstraintCheck
)

type ConstraintInfo struct {
	// a temporary placeholder
}

const ConstraintForeignInfoSize = int(unsafe.Sizeof(ConstraintForeignInfo{}))
const FkFileName = "FOREIGN_KEY_FILE"

// ADD CONSTRAINT fkName FOREIGN KEY (columnList) REFERENCES tableName(columnList)
type ConstraintForeignInfo struct {
	fkName  [types.MaxNameSize]byte // foreign key name, specified by user
	relSrc  [types.MaxNameSize]byte // src table (referencing)
	attrSrc [types.MaxNameSize]byte // attr in src table
	relDst  [types.MaxNameSize]byte // foreign table(relation) name
	attrDst [types.MaxNameSize]byte // attr in foreign table (must be primary)
}

const ConstraintPrimaryInfoSize = int(unsafe.Sizeof(ConstraintForeignInfo{}))
const PkFileName = "PRIMARY_KEY_FILE"

type ConstraintPrimaryInfo struct {
	relSrc  [types.MaxNameSize]byte // src table (referencing)
	attrSrc [types.MaxNameSize]byte // attr in current table
}

type ConstraintCheckInfo struct {
	value  parser.Value
	compOp types.OpType // must be a comparison op for check in constraint
}
