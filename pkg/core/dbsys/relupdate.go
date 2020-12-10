package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
)

type TableUpdateType int

const (
	TableAddField TableUpdateType = iota
	TableUpdateField
	TableDropField
	TableRenameField
	TableRenameSelf
	TableAddConstraint // include foreign & primary & check constraint
	TableDropConstraint
)

func (m *Manager) CreateIndex(relName string, attrList []string) {
	//TODO
}

func (m *Manager) DropIndex(relName string, attrList []string) {
	// TODO
}

func (m *Manager) AddPrimaryKey(relName string, attrList []string) {
	// TODO
}

func (m *Manager) DropPrimaryKey(relName string) {
	// TODO
}

func (m *Manager) AddForeignKey(fkName string, srcRel string, srcAttrList []string, dstRel string, dstAttrList []string) error {
	if len(m.dbSelected) == 0 {
		return errorutil.ErrorDbSysDbNotSelected
	}
	if len(srcAttrList) != len(dstAttrList) {
		return errorutil.ErrorDbSysForeignKeyLenNotMatch
	}

	fkFile, err := m.relManager.OpenFile(FkFileName)
	defer m.relManager.CloseFile(fkFile.Filename)
	if err != nil {
		panic(0)
	}

	// check foreign key
	rawFKList := fkFile.GetRecList()
	filterCond := record.FilterCond{
		AttrSize:   types.MaxNameSize,
		AttrOffset: 0,
		CompOp:     types.OpCompEQ,
		Value: parser.Value{
			ValueType: types.STRING,
		},
	}
	filterCond.Value.FromStr(fkName)
	if len(record.FilterOnRecList(rawFKList, []record.FilterCond{filterCond})) == 0 {
		return errorutil.ErrorDbSysForeignKeyExists
	}

	for i := 0; i < len(srcAttrList); i++ {
		if !m.CheckTableAndAttrExistence(srcRel, srcAttrList[i]) {
			return errorutil.ErrorDbSysRelationOrAttrNotExists
		}
		if !m.CheckTableAndAttrExistence(dstRel, dstAttrList[i]) {
			return errorutil.ErrorDbSysRelationOrAttrNotExists
		}
	}
	// todo check value boundary, index query is a must

	// write back
	for i := 0; i < len(srcAttrList); i++ {
		rec := make([]byte, ConstraintForeignInfoSize)
		constraint := (*ConstraintForeignInfo)(types.ByteSliceToPointer(rec))
		constraint.fkName = strTo24ByteArray(fkName)
		constraint.attrDst = strTo24ByteArray(dstAttrList[i])
		constraint.relDst = strTo24ByteArray(dstRel)
		constraint.attrSrc = strTo24ByteArray(srcAttrList[i])
		constraint.relSrc = strTo24ByteArray(srcRel)
		_, _ = fkFile.InsertRec(rec)
	}
	return nil
}

func (m *Manager) DropForeignKey(fkName string) {
}

func (m *Manager) AddColumn(fkName string) {

}

func (m *Manager) DropColumn(relName string, attrName string) {
	// TODO
	// check foreign constraint
}

func (m *Manager) CheckTableAndAttrExistence(relName string, attrName string) bool {
	if len(m.dbSelected) == 0 {
		panic(0)
	}
	if _, found := m.rels[relName]; !found {
		return false
	}
	attrInfoMap := m.GetAttrInfoMap(relName)
	if _, found := attrInfoMap[attrName]; !found {
		return false
	}
	return true
}
