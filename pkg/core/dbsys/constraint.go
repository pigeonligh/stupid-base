package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"unsafe"
)

//type ConstraintType int
//const (
//	ConstraintPrimary ConstraintType = iota
//	ConstraintForeign
//	ConstraintCheck
//)

type ConstraintInfo struct {
	// a temporary placeholder
}

const ConstraintForeignInfoSize = int(unsafe.Sizeof(ConstraintForeignInfo{}))
const GlbFkFileName = "GLOBAL_FOREIGN_KEY_FILE"

// ADD CONSTRAINT fkName FOREIGN KEY (columnList) REFERENCES tableName(columnList)
type ConstraintForeignInfo struct {
	fkName  [types.MaxNameSize]byte // foreign key name, specified by user or random generated
	relSrc  [types.MaxNameSize]byte // src table (referencing)
	attrSrc [types.MaxNameSize]byte // attrName in src table
	relDst  [types.MaxNameSize]byte // foreign table(relation) name
	attrDst [types.MaxNameSize]byte // attrName in foreign table (must be primary)
}

//primary key will not be recorded use a file
//const ConstraintPrimaryInfoSize = int(unsafe.Sizeof(ConstraintForeignInfo{}))
//const PkFileName = "PRIMARY_KEY_FILE"
//
//type ConstraintPrimaryInfo struct {
//	relSrc  [types.MaxNameSize]byte // src table (referencing)
//	attrSrc [types.MaxNameSize]byte // attrName in current table
//}

// may be further implemented
//type ConstraintCheckInfo struct {
//	value  types.Value
//	compOp types.OpType // must be a comparison op for check in constraint
//}

func (m *Manager) checkDBTableAndAttrExistence(relName string, attrNameList []string) error {
	if len(m.dbSelected) == 0 {
		return errorutil.ErrorDBSysDBNotSelected
	}
	if _, found := m.rels[relName]; !found {
		return errorutil.ErrorDBSysRelationNotExisted
	}

	if attrNameList != nil {
		attrInfoMap := m.getAttrInfoMapViaCacheOrReload(relName, nil)
		for _, attr := range attrNameList {
			if _, found := attrInfoMap[attr]; !found {
				return errorutil.ErrorDBSysAttrNotExisted
			}
		}
	}
	return nil
}
