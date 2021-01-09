package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
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

func (m *Manager) CreateIndex(idxName string, relName string, attrList []string, duplicatedAllowed bool) error {
	if err := m.checkDBTableAndAttrExistence(relName, attrList); err != nil {
		return err
	}
	if len(idxName) > 24 {
		return errorutil.ErrorDBSysInvalidIndexName
	}

	attrInfoCollection := m.GetAttrInfoCollection(relName)
	if _, found := attrInfoCollection.IdxMap[idxName]; found {
		return errorutil.ErrorDBSysIndexNameAlreadyExisted
	}

	for _, attr := range attrList {
		if len(attrInfoCollection.InfoMap[attr].IndexName) > 0 {
			return errorutil.ErrorDBSysColIndexAlreadyExisted
		}
	}

	// insert each record rid to index file
	dataFile, _ := m.relManager.OpenFile(getTableDataFileName(relName))
	defer m.relManager.CloseFile(getTableDataFileName(relName))

	idxAttrSet := types.AttrSet{}
	for _, attr := range attrList {
		idxAttrSet.AddSingleAttr(attrInfoCollection.InfoMap[attr].AttrInfo)
	}

	_ = m.idxManager.CreateIndex(getTableIdxDataFileName(relName, idxName), idxAttrSet)
	idxFile, err := m.idxManager.OpenIndex(getTableIdxDataFileName(relName, idxName), dataFile)
	if err != nil {
		panic(err)
	}
	insertRidList := make([]types.RID, 0)
	for _, rec := range dataFile.GetRecList() {
		err := idxFile.InsertEntry(rec.Rid)
		if err != nil {
			panic(err)
		}
		insertRidList = append(insertRidList, rec.Rid)
		if !duplicatedAllowed {
			// it must have unique or primary constraint, check duplicate
			primaryByte := idxAttrSet.DataToAttrs(rec.Rid, rec.Data)
			if len(idxFile.GetRidList(types.OpCompEQ, primaryByte)) > 1 {
				// just drop the index is ojbk
				//if err := idxFile.DeleteEntryByBatch(insertRidList); err != nil {
				//	panic(err)
				//	// rolling back
				//}
				_ = m.idxManager.CloseIndex(getTableIdxDataFileName(relName, idxName))
				_ = m.idxManager.DestroyIndex(getTableIdxDataFileName(relName, idxName))
				return errorutil.ErrorDBSysDuplicatedKeysFound
			}
		}
	}
	_ = m.idxManager.CloseIndex(getTableIdxDataFileName(relName, idxName))

	// update each attr and index file
	relInfo := m.GetDBRelInfoMap()[relName]
	relInfo.IndexCount++
	//idxInfoCollection.Name2Cols[idxName] = attrList

	for _, attr := range attrList {
		attrInfo := attrInfoCollection.InfoMap[attr]
		attrInfo.IndexName = idxName
		attrInfoCollection.InfoMap[attr] = attrInfo
		//idxInfoCollection.Col2Name[attr] = idxName
	}

	m.SetAttrInfoListByCollection(relName, attrInfoCollection)
	m.SetRelInfo(relInfo)
	//m.SetIdxInfoCollection(relName, idxInfoCollection)
	return nil
}

func (m *Manager) DropIndex(relName string, idxName string) error {
	// 1. update relation info idxCount
	// 2. update each attr info
	// 3. remove from index file
	// 4. remove index file
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}

	// 3
	attrInfoCollection := m.GetAttrInfoCollection(relName)
	if _, found := attrInfoCollection.IdxMap[idxName]; !found {
		// check existence
		return errorutil.ErrorDBSysIndexNameNotExisted
	}

	// 1
	relInfo := m.GetDBRelInfoMap()[relName]
	relInfo.IndexCount--
	m.SetRelInfo(relInfo)

	// 2

	for _, attr := range attrInfoCollection.IdxMap[idxName] {
		attrInfo := attrInfoCollection.InfoMap[attr]
		attrInfo.IndexName = ""
		attrInfoCollection.InfoMap[attr] = attrInfo
	}
	m.SetAttrInfoListByCollection(relName, attrInfoCollection)

	// 4
	_ = m.idxManager.DestroyIndex(getTableIdxDataFileName(relName, idxName))

	return nil
}

func (m *Manager) AddPrimaryKey(relName string, attrList []string) error {
	// 0. check primary exists
	// 1. add primary count to dbMeta
	// 2. update each attr info

	// 0
	if err := m.checkDBTableAndAttrExistence(relName, attrList); err != nil {
		return err
	}
	relInfo := m.GetDBRelInfoMap()[relName]
	if relInfo.PrimaryCount >= 1 {
		return errorutil.ErrorDBSysPrimaryKeyCntExceed
	}

	// 1 add primary count to dbMeta
	if err := m.CreateIndex(PrimaryKeyIdxName, relName, attrList, false); err != nil {
		return err
	}
	relInfo.PrimaryCount += len(attrList)
	m.SetRelInfo(relInfo)

	// 2
	attrInfoDetailedCollection := m.GetAttrInfoCollection(relName)
	for _, attr := range attrList {
		attrInfo := attrInfoDetailedCollection.InfoMap[attr]
		attrInfo.IsPrimary = true
		attrInfoDetailedCollection.InfoMap[attr] = attrInfo
	}
	m.SetAttrInfoListByCollection(relName, attrInfoDetailedCollection)
	return nil
}

func (m *Manager) DropPrimaryKey(relName string) error {
	// 0. check primary exists
	// 1. set primary count to 0
	// 2. update each attr info
	// 3. remove index file

	// 0
	if err := m.checkDBTableAndAttrExistence(relName, nil); err != nil {
		return err
	}
	relInfo := m.GetDBRelInfoMap()[relName]
	if relInfo.PrimaryCount == 0 {
		return errorutil.ErrorDBSysPrimaryKeyDoNotExist
	}

	// 1
	if err := m.DropIndex(relName, PrimaryKeyIdxName); err != nil {
		return err
	}
	relInfo.PrimaryCount = 0
	m.SetRelInfo(relInfo)

	// 2
	attrInfoDetailedCollection := m.GetAttrInfoCollection(relName)
	for _, key := range attrInfoDetailedCollection.PkList {
		attrInfo := attrInfoDetailedCollection.InfoMap[key]
		attrInfo.IsPrimary = false
		attrInfoDetailedCollection.InfoMap[key] = attrInfo
	}
	m.SetAttrInfoListByCollection(relName, attrInfoDetailedCollection)
	return nil
}

func (m *Manager) AddForeignKey(fkName string, srcRel string, srcAttrList []string, dstRel string, dstAttrList []string) error {
	// 1. check table&attr existence
	// 2. check fk constraint existence
	// 3. check is primary key
	// 4. check value boundary
	// 5. check is types match
	// 6. update each attr
	// 7. update relation
	// 8. write back to origin fk file

	//1.
	if len(srcAttrList) != len(dstAttrList) {
		return errorutil.ErrorDBSysForeignKeyLenNotMatch
	}
	if srcRel == dstRel {
		return errorutil.ErrorDBSysForeignKeyRefSelf
	}
	if err := m.checkDBTableAndAttrExistence(srcRel, srcAttrList); err != nil {
		return err
	}
	if err := m.checkDBTableAndAttrExistence(dstRel, dstAttrList); err != nil {
		return err
	}

	// 2.
	fkMap := m.GetFkInfoMap()
	if _, found := fkMap[fkName]; found {
		return errorutil.ErrorDBSysForeignKeyExists
	}

	// 3. check is primary key
	dstAttrInfoCollection := m.GetAttrInfoCollection(dstRel)
	srcAttrInfoCollection := m.GetAttrInfoCollection(srcRel)

	if len(dstAttrList) != len(dstAttrInfoCollection.PkList) {
		return errorutil.ErrorDBSysIsNotPrimaryKeys
	}
	for _, attr := range dstAttrList {
		if attrInfo := dstAttrInfoCollection.InfoMap[attr]; !attrInfo.IsPrimary {
			return errorutil.ErrorDBSysIsNotPrimaryKeys
		}
	}

	// 5. check if type is match
	for i := range srcAttrList {
		if srcAttrInfoCollection.InfoMap[srcAttrList[i]].AttrType != dstAttrInfoCollection.InfoMap[dstAttrList[i]].AttrType {
			return errorutil.ErrorDBSysFkTypeNotMatchPk
		}
	}

	// 4. check value boundary
	srcFile, _ := m.relManager.OpenFile(getTableDataFileName(srcRel))
	dstFile, _ := m.relManager.OpenFile(getTableDataFileName(dstRel))
	dstIdxFile, _ := m.idxManager.OpenIndex(getTableIdxDataFileName(dstRel, PrimaryKeyIdxName), dstFile)
	defer m.relManager.CloseFile(getTableDataFileName(srcRel))
	defer m.relManager.CloseFile(getTableDataFileName(dstRel))
	defer m.idxManager.CloseIndex(getTableIdxDataFileName(dstRel, PrimaryKeyIdxName))

	srcAttrSet := m.GetAttrSetFromAttrs(srcRel, srcAttrList)
	for _, rec := range srcFile.GetRecList() {
		compData := srcAttrSet.DataToAttrs(rec.Rid, rec.Data)
		length := len(dstIdxFile.GetRidList(types.OpCompEQ, compData))
		if length == 0 {
			return errorutil.ErrorDBSysFkValueNotInPk
		}
		if length > 1 {
			panic(errorutil.ErrorDBSysDuplicatedKeysFound)
		}
	}

	// update attr info & check primary constraint
	{
		// dst update
		for _, attr := range dstAttrList {
			attrInfo := dstAttrInfoCollection.InfoMap[attr]
			attrInfo.FkName = fkName
			dstAttrInfoCollection.InfoMap[attr] = attrInfo
		}
		m.SetAttrInfoListByCollection(dstRel, dstAttrInfoCollection)
	}
	{
		// src update
		for _, attr := range srcAttrList {
			attrInfo := srcAttrInfoCollection.InfoMap[attr]
			attrInfo.FkName = fkName
			srcAttrInfoCollection.InfoMap[attr] = attrInfo
		}
		m.SetAttrInfoListByCollection(srcRel, srcAttrInfoCollection)
	}

	// update original relation
	relInfoMap := m.GetDBRelInfoMap()
	srcRelInfo := relInfoMap[srcRel]
	srcRelInfo.ForeignCount++
	dstRelInfo := relInfoMap[dstRel]
	dstRelInfo.ForeignCount++
	m.SetRelInfo(srcRelInfo)
	m.SetRelInfo(dstRelInfo)

	// write back to fkFile
	fkMap[fkName] = FkConstraint{
		FkName:  fkName,
		SrcRel:  srcRel,
		DstRel:  dstRel,
		SrcAttr: srcAttrList,
		DstAttr: dstAttrList,
	}
	m.SetFkInfoMap(fkMap)
	return nil
}

func (m *Manager) DropForeignKey(fkName string) error {
	// 1. check fk exists
	// 2. remove from attr info
	// 3. remove from foreign key file

	fkMap := m.GetFkInfoMap()
	if _, found := fkMap[fkName]; !found {
		return errorutil.ErrorDBSysForeignKeyNotExists
	}
	fkInfo := fkMap[fkName]

	// update relation info
	relInfoMap := m.GetDBRelInfoMap()
	srcRelInfo := relInfoMap[fkInfo.SrcRel]
	srcRelInfo.ForeignCount--
	dstRelInfo := relInfoMap[fkInfo.DstRel]
	dstRelInfo.ForeignCount--
	m.SetRelInfo(srcRelInfo)
	m.SetRelInfo(dstRelInfo)

	// remove from attr info
	dstAttrInfoCollection := m.GetAttrInfoCollection(fkInfo.DstRel)
	srcAttrInfoCollection := m.GetAttrInfoCollection(fkInfo.SrcRel)
	for _, attr := range fkInfo.SrcAttr {
		attrInfo := srcAttrInfoCollection.InfoMap[attr]
		attrInfo.IndexName = ""
		srcAttrInfoCollection.InfoMap[attr] = attrInfo
	}
	for _, attr := range fkInfo.DstAttr {
		attrInfo := dstAttrInfoCollection.InfoMap[attr]
		attrInfo.IndexName = ""
		dstAttrInfoCollection.InfoMap[attr] = attrInfo
	}
	m.SetAttrInfoListByCollection(fkInfo.SrcRel, srcAttrInfoCollection)
	m.SetAttrInfoListByCollection(fkInfo.DstRel, dstAttrInfoCollection)

	delete(fkMap, fkName)

	return nil
}

func (m *Manager) AddColumn(relName string, attrName string, info parser.AttrInfo) {
	// todo
	// 1. check name exists
	// 2. check info valid
	// 3. check attrInfo valid (foreign key, primary key)
}

func (m *Manager) DropColumn(relName string, attrName string) {
	// TODO
	// check foreign constraint, if has foreign constraint -> drop
}

func (m *Manager) RenameTable() {
	// TODO
}

func (m *Manager) ChangeColumn() {
	// TODO
}
