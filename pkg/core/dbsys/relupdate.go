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
		if len(attrInfoCollection.InfoMap[attr].IndexName) >= 0 {
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
		_ = idxFile.InsertEntry(rec.Rid)
		insertRidList = append(insertRidList, rec.Rid)
		if !duplicatedAllowed {
			// it must have unique or primary constraint, check duplicate
			primaryByte := idxAttrSet.DataToAttrs(rec.Rid, rec.Data)
			if len(idxFile.GetRidList(types.OpCompEQ, primaryByte)) > 1 {
				if err := idxFile.DeleteEntryByBatch(insertRidList); err != nil {
					panic(err)
					// rolling back
				}
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

	// 1.
	//if len(srcAttrList) != len(dstAttrList) {
	//	return errorutil.ErrorDBSysForeignKeyLenNotMatch
	//}
	//if srcRel == dstRel {
	//	return errorutil.ErrorDBSysForeignKeyRefSelf
	//}
	//if err := m.checkDBTableAndAttrExistence(srcRel, srcAttrList); err != nil {
	//	return err
	//}
	//if err := m.checkDBTableAndAttrExistence(dstRel, dstAttrList); err != nil {
	//	return err
	//}
	//
	//// 2.
	//fkFile, err := m.relManager.OpenFile(GlbFkFileName)
	//defer m.relManager.CloseFile(fkFile.Filename)
	//if err != nil {
	//	panic(0)
	//}
	//
	//tmpList, _ := record.FilterOnRecList(fkFile.GetRecList(), parser.NewExprCompQuickAttrCompValue(types.MaxNameSize, 0, types.OpCompEQ, types.NewValueFromStr(fkName)))
	//if len(tmpList) != 0 {
	//	return errorutil.ErrorDBSysForeignKeyExists
	//}
	//
	//// 3. check is primary key
	//dstAttrInfoDetailedCollection := m.getAttrInfoDetailedCollection(dstRel)
	//srcAttrInfoDetailedCollection := m.getAttrInfoDetailedCollection(srcRel)
	//if len(dstAttrList) != len(dstAttrInfoDetailedCollection.pkMap) {
	//	return errorutil.ErrorDBSysIsNotPrimaryKeys
	//}
	//for _, attr := range dstAttrList {
	//	if _, found := dstAttrInfoDetailedCollection.pkMap[attr]; !found {
	//		return errorutil.ErrorDBSysIsNotPrimaryKeys
	//	}
	//}
	//
	//// 5. check if type is match
	//for i := range srcAttrList {
	//	if srcAttrInfoDetailedCollection.infoMap[srcAttrList[i]].AttrType != dstAttrInfoDetailedCollection.infoMap[dstAttrList[i]].AttrType {
	//		return errorutil.ErrorDBSysFkTypeNotMatchPk
	//	}
	//}
	//
	//// 4. check value boundary
	//srcFile, _ := m.relManager.OpenFile(getTableDataFileName(srcRel))
	//dstFile, _ := m.relManager.OpenFile(getTableDataFileName(dstRel))
	//dstIdxFile, _ := m.idxManager.OpenIndex(getTableIdxDataFileName(dstRel, PrimaryKeyIdxName), dstFile)
	//defer m.relManager.CloseFile(getTableDataFileName(srcRel))
	//defer m.relManager.CloseFile(getTableDataFileName(dstRel))
	//defer m.idxManager.CloseIndex(getTableIdxDataFileName(dstRel, PrimaryKeyIdxName))
	//
	//srcAttrSet := types.AttrSet{}
	//for _, attr := range srcAttrList {
	//	srcAttrSet.AddSingleAttr(srcAttrInfoDetailedCollection.infoMap[attr].AttrInfo)
	//}
	//
	//for _, rec := range srcFile.GetRecList() {
	//	compData := srcAttrSet.DataToAttrs(rec.Rid, rec.Data)
	//	length := len(dstIdxFile.GetRidList(types.OpCompEQ, compData))
	//	if length == 0 {
	//		return errorutil.ErrorDBSysFkNotRefPk
	//	}
	//	if length > 1 {
	//		panic(errorutil.ErrorDBSysDuplicatedKeysFound)
	//	}
	//}
	//
	//// update attr info & check primary constraint
	//{
	//	// dst update
	//	for _, attr := range dstAttrList {
	//		attrInfo := dstAttrInfoDetailedCollection.infoMap[attr]
	//		attrRid := dstAttrInfoDetailedCollection.ridMap[attr]
	//		attrInfo.HasForeignConstraint = true
	//		if !attrInfo.IsPrimary {
	//			return errorutil.ErrorDBSysFkNotRefPk
	//		}
	//		m.updateAttrInfo(dstRel, attrRid, attrInfo, false)
	//	}
	//	m.getAttrInfoMapViaCacheOrReload(dstRel, dstAttrInfoDetailedCollection.infoMap)
	//}
	//{
	//	// src update
	//	for _, attr := range srcAttrList {
	//		attrInfo := srcAttrInfoDetailedCollection.infoMap[attr]
	//		attrRid := srcAttrInfoDetailedCollection.ridMap[attr]
	//		attrInfo.HasForeignConstraint = true
	//		m.updateAttrInfo(srcRel, attrRid, attrInfo, false)
	//	}
	//	m.getAttrInfoMapViaCacheOrReload(srcRel, srcAttrInfoDetailedCollection.infoMap)
	//}
	//
	//// update original relation
	//relInfoMap, relInfoRidMap := m.getRelInfoMapWithRid()
	//{
	//	// src relation
	//	srcRelInfo := relInfoMap[srcRel]
	//	srcRelInfoRid := relInfoRidMap[srcRel]
	//	srcRelInfo.foreignCount++
	//	m.updateRelInfo(srcRel, srcRelInfoRid, srcRelInfo, false)
	//}
	//{
	//	// dst relation
	//	dstRelInfo := relInfoMap[srcRel]
	//	dstRelInfoRid := relInfoRidMap[srcRel]
	//	dstRelInfo.foreignCount++
	//	m.updateRelInfo(dstRel, dstRelInfoRid, dstRelInfo, false)
	//}
	//
	//// write back to fkFile
	//for i := 0; i < len(srcAttrList); i++ {
	//	rec := make([]byte, ConstraintForeignInfoSize)
	//	constraint := (*ConstraintForeignInfo)(types.ByteSliceToPointer(rec))
	//	constraint.fkName = strTo24ByteArray(fkName)
	//	constraint.attrDst = strTo24ByteArray(dstAttrList[i])
	//	constraint.relDst = strTo24ByteArray(dstRel)
	//	constraint.attrSrc = strTo24ByteArray(srcAttrList[i])
	//	constraint.relSrc = strTo24ByteArray(srcRel)
	//	_, _ = fkFile.InsertRec(rec)
	//}

	return nil
}

func (m *Manager) DropForeignKey(fkName string) error {
	// 1. check fk exists
	// 2. remove from attr info
	// 3. remove from foreign key file

	//fkFile, err := m.relManager.OpenFile(GlbFkFileName)
	//defer m.relManager.CloseFile(fkFile.Filename)
	//if err != nil {
	//	panic(0)
	//}
	//
	//fkRecList, _ := record.FilterOnRecList(fkFile.GetRecList(), parser.NewExprCompQuickAttrCompValue(types.MaxNameSize, 0, types.OpCompEQ, types.NewValueFromStr(fkName)))
	//if len(fkRecList) == 0 {
	//	return errorutil.ErrorDBSysForeignKeyNotExists
	//}
	//
	//// 2. update attr info
	//srcRel := ""
	//dstRel := ""
	//srcAttrList := make([]string, 0)
	//dstAttrList := make([]string, 0)
	//for _, fk := range fkRecList {
	//	fk := (*ConstraintForeignInfo)(types.ByteSliceToPointer(fk.Data))
	//	srcRel = ByteArray24tostr(fk.relSrc)
	//	dstRel = ByteArray24tostr(fk.relDst)
	//	srcAttrList = append(srcAttrList, ByteArray24tostr(fk.attrSrc))
	//	dstAttrList = append(dstAttrList, ByteArray24tostr(fk.attrDst))
	//}
	//dstAttrInfoDetailedCollection := m.getAttrInfoDetailedCollection(dstRel)
	//srcAttrInfoDetailedCollection := m.getAttrInfoDetailedCollection(srcRel)
	//{
	//	// dst update
	//	for _, attr := range dstAttrList {
	//		attrInfo := dstAttrInfoDetailedCollection.infoMap[attr]
	//		attrRid := dstAttrInfoDetailedCollection.ridMap[attr]
	//		attrInfo.HasForeignConstraint = false
	//		m.updateAttrInfo(dstRel, attrRid, attrInfo, false)
	//	}
	//	m.getAttrInfoMapViaCacheOrReload(dstRel, dstAttrInfoDetailedCollection.infoMap)
	//}
	//{
	//	// src update
	//	for _, attr := range srcAttrList {
	//		attrInfo := srcAttrInfoDetailedCollection.infoMap[attr]
	//		attrRid := srcAttrInfoDetailedCollection.ridMap[attr]
	//		attrInfo.HasForeignConstraint = false
	//		m.updateAttrInfo(srcRel, attrRid, attrInfo, false)
	//	}
	//	m.getAttrInfoMapViaCacheOrReload(srcRel, srcAttrInfoDetailedCollection.infoMap)
	//}
	//
	//// update original relation
	//relInfoMap, relInfoRidMap := m.getRelInfoMapWithRid()
	//{
	//	// src relation
	//	srcRelInfo := relInfoMap[srcRel]
	//	srcRelInfoRid := relInfoRidMap[srcRel]
	//	srcRelInfo.foreignCount--
	//	m.updateRelInfo(srcRel, srcRelInfoRid, srcRelInfo, false)
	//}
	//{
	//	// dst relation
	//	dstRelInfo := relInfoMap[srcRel]
	//	dstRelInfoRid := relInfoRidMap[srcRel]
	//	dstRelInfo.foreignCount--
	//	m.updateRelInfo(dstRel, dstRelInfoRid, dstRelInfo, false)
	//}
	//
	//// 3.
	//fkFile.DeleteRecByBatch(record.GetRidListFromRecList(fkRecList))
	//return nil
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
