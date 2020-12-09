package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

type TableUpdateType int
const (
	TableAddField TableUpdateType = iota
	TableUpdateField
	TableDropField
	TableRenameField
	TableRenameSelf
	TableAddConstraint	// include foreign & primary & check constraint
	TableDropConstraint
)

type AttrInfoUpdate struct {
	AttrName		string
	ValueType 		types.ValueType
	DefaultValue	parser.Value
	NullAllowed		parser.Value
}

type RelUpdate struct {
	UpdateType TableUpdateType
	AttrName string		// used for remove column
	RelName string

	// used for field updating
	FieldUpdate 	parser.AttrInfo

	// used for constraint updating
	ConstraintUpdate ConstraintInfo
}






