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

func getIndexFilePrefix(relName string, attrList []string) string {
	filename := relName
	for _, attr := range attrList {
		filename += "." + attr
	}
	return filename
}

func getIndexFileName(prefix string) string {
	return prefix + ".index"
}

func (m *Manager) CreateIndex(idxName string, relName string, attrList []string) error {
	// todo check duplicated for primary key
	if err := m.checkDbTableAndAttrExistence(relName, attrList); err != nil {
		return err
	}
	if len(idxName) > 24 {
		return errorutil.ErrorDbSysInvalidIndexName
	}

	idxInfoCollection := m.getIdxDetailedInfoCollection(relName)
	if _, found := idxInfoCollection.name2cols[idxName]; found {
		return errorutil.ErrorDbSysIndexNameAlreadyExisted
	}

	attrInfoCollection := m.getAttrInfoDetailedCollection(relName)
	for _, attr := range attrList {
		if attrInfoCollection.infoMap[attr].IndexNo <= 0 {
			return errorutil.ErrorDbSysColIndexAlreadyExisted
		}
	}

	// insert each record rid to index file
	dataFile, _ := m.relManager.OpenFile(getTableDataFileName(relName))
	defer m.relManager.CloseFile(getTableDataFileName(relName))

	_ = m.idxManager.CreateIndex(getIndexFileName(idxName))
	idxFile, _ := m.idxManager.OpenIndex(getIndexFileName(idxName))
	defer m.idxManager.CloseIndex(getIndexFileName(idxName))

	for _, rec := range dataFile.GetRecList() {
		_ = idxFile.InsertEntry(rec.Rid)
	}

	// update each attr and index file
	relInfoMap, relInfoRidMap := m.getRelInfoMapWithRid()
	relInfo := relInfoMap[relName]
	relInfoRid := relInfoRidMap[relName]

	for _, attr := range attrList {
		attrInfo := attrInfoCollection.infoMap[attr]
		attrRid := attrInfoCollection.ridMap[attr]
		attrInfo.IndexNo = relInfo.nextIndexNo
		m.updateAttrInfo(relName, attrRid, attrInfo, false)
		m.insertOrRemoveIndexInfo(relName, &IndexInfo{
			idxNo:   relInfo.nextIndexNo,
			idxName: strTo24ByteArray(idxName),
			col:     strTo24ByteArray(attr),
		}, true, nil)
	}

	relInfo.indexCount += 1
	relInfo.nextIndexNo += 1
	m.updateRelInfo(relName, relInfoRid, relInfo, false)
	m.getAttrInfoMapViaCache(relName, true, attrInfoCollection.infoMap)
	return nil
}

func (m *Manager) DropIndex(relName string, idxName string) error {
	// 1. update relation info idxcount
	// 2. update each attr info
	// 3. remove from index file
	if !m.DbSelected() {
		return errorutil.ErrorDbSysDbNotSelected
	}

	// 3
	idxDetailedInfoCollection := m.getIdxDetailedInfoCollection(relName)
	if rids, found := idxDetailedInfoCollection.name2rids[idxName]; !found {
		// check existence
		return errorutil.ErrorDbSysIndexNameNotExisted
	} else {
		idxfh, err := m.relManager.OpenFile(getTableIdxFileName(relName))
		if err != nil {
			panic(0)
		}
		defer m.relManager.CloseFile(idxfh.Filename)
		idxfh.DeleteRecByBatch(rids)
	}

	// 1
	relInfoMap, relInfoRidMap := m.getRelInfoMapWithRid()
	relInfo := relInfoMap[relName]
	relInfo.indexCount -= 1
	m.updateRelInfo(relName, relInfoRidMap[relName], relInfo, false)

	// 2
	attrInfoDetailedCollection := m.getAttrInfoDetailedCollection(relName)
	for _, attr := range idxDetailedInfoCollection.name2cols[idxName] {
		attrInfo := attrInfoDetailedCollection.infoMap[attr]
		attrRid := attrInfoDetailedCollection.ridMap[attr]
		attrInfo.IndexNo = -1
		m.updateAttrInfo(relName, attrRid, attrInfo, false)
	}

	return nil
}

func (m *Manager) AddPrimaryKey(relName string, attrList []string) error {

	if !m.DbSelected() {
		return errorutil.ErrorDbSysDbNotSelected
	}
	if err := m.checkDbTableAndAttrExistence(relName, attrList); err != nil {
		return err
	}
	relInfoMap, relInfoRidMap := m.getRelInfoMapWithRid()
	if relInfoMap[relName].primaryCount >= 1 {
		return errorutil.ErrorDbSysPrimaryKeyCntExceed
	}
	if rec, err := m.dbMeta.GetRec(relInfoRidMap[relName]); err != nil {
		panic(0)
	} else {
		// index may already exists, won't create
		if err := m.CreateIndex(PrimaryKeyIndexName, relName, attrList); err != nil {
			return err
		}
		// otherwise update relation info
		rel := (*RelInfo)(types.ByteSliceToPointer(rec.Data))
		rel.primaryCount += len(attrList)
		m.dbMeta.ForcePage(relInfoRidMap[relName].Page)
	}
	return nil
}

func (m *Manager) DropPrimaryKey(relName string) {
	// TODO
}

func (m *Manager) AddForeignKey(fkName string, srcRel string, srcAttrList []string, dstRel string, dstAttrList []string) error {
	if len(srcAttrList) != len(dstAttrList) {
		return errorutil.ErrorDbSysForeignKeyLenNotMatch
	}
	if err := m.checkDbTableAndAttrExistence(srcRel, srcAttrList); err != nil {
		return err
	}
	if err := m.checkDbTableAndAttrExistence(dstRel, dstAttrList); err != nil {
		return err
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
