package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/env"
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"os"
	"unsafe"
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
	m.SetRelInfo(relInfo)
	//idxInfoCollection.Name2Cols[idxName] = attrList

	for _, attr := range attrList {
		attrInfo := attrInfoCollection.InfoMap[attr]
		attrInfo.IndexName = idxName
		attrInfoCollection.InfoMap[attr] = attrInfo
		//idxInfoCollection.Col2Name[attr] = idxName
	}

	m.SetAttrInfoListByCollection(relName, attrInfoCollection)
	m.SetRelInfo(relInfo)
	log.V(log.DBSysLevel).Infof("Create index succeed %v : %v, %v", relName, attrList, idxName)
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
	if err := m.checkDBTableAndAttrExistence(relName, nil); err != nil {
		return err
	}
	// 3
	attrInfoCollection := m.GetAttrInfoCollection(relName)
	if _, found := attrInfoCollection.IdxMap[idxName]; !found {
		// check existence
		return errorutil.ErrorDBSysIndexNameNotExisted
	}

	// 1
	relInfo := m.GetDBRelInfoMap()[relName]
	relInfo.IndexCount = relInfo.IndexCount - 1
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
	log.Debugf("Drop index success %v : %v", relName, idxName)
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

	if err := m.CreateIndex(PrimaryKeyIdxName, relName, attrList, false); err != nil {
		return err
	}

	// 1 add primary count to dbMeta
	relInfo := m.GetDBRelInfoMap()[relName]
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
	log.V(log.DBSysLevel).Infof("Add primary key succeed %v : %v", relName, attrList)

	return nil
}

func (m *Manager) DropPrimaryKey(relName string) error {
	// 00. check foreign key exists
	// 0. check primary exists
	// 1. set primary count to 0
	// 2. update each attr info
	// 3. remove index file
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	// 0
	if err := m.checkDBTableAndAttrExistence(relName, nil); err != nil {
		return err
	}

	// 00
	attrInfoCollection := m.GetAttrInfoCollection(relName)
	for _, pk := range attrInfoCollection.PkList {
		if len(attrInfoCollection.InfoMap[pk].FkName) > 0 {
			return errorutil.ErrorDBSysForeignKeyExists
		}
	}

	// 1
	if err := m.DropIndex(relName, PrimaryKeyIdxName); err != nil {
		return err
	}

	relInfo := m.GetDBRelInfoMap()[relName]
	relInfo.PrimaryCount = 0
	m.SetRelInfo(relInfo)

	// 2
	for _, key := range attrInfoCollection.PkList {
		attrInfo := attrInfoCollection.InfoMap[key]
		attrInfo.IsPrimary = false
		attrInfo.IndexName = ""
		attrInfoCollection.InfoMap[key] = attrInfo
	}
	m.SetAttrInfoListByCollection(relName, attrInfoCollection)
	log.V(log.DBSysLevel).Infof("Drop primary key succeed %v", relName)
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
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
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
	log.V(log.DBSysLevel).Infof("Create foreign key succeed %v : %v -> %v  | (%v) - > (%v)", fkName, srcRel, dstRel, srcAttrList, dstAttrList)
	return nil
}

func (m *Manager) DropForeignKey(fkName string) error {
	// 1. check fk exists
	// 2. remove from attr info
	// 3. remove from foreign key file
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
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
		attrInfo.FkName = ""
		srcAttrInfoCollection.InfoMap[attr] = attrInfo
	}
	for _, attr := range fkInfo.DstAttr {
		attrInfo := dstAttrInfoCollection.InfoMap[attr]
		attrInfo.FkName = ""
		dstAttrInfoCollection.InfoMap[attr] = attrInfo
	}
	m.SetAttrInfoListByCollection(fkInfo.SrcRel, srcAttrInfoCollection)
	m.SetAttrInfoListByCollection(fkInfo.DstRel, dstAttrInfoCollection)

	delete(fkMap, fkName)
	m.SetFkInfoMap(fkMap)
	log.V(log.DBSysLevel).Infof("Drop foreign key succeed %v", fkName)

	return nil
}

func (m *Manager) AddColumn(relName string, attrName string, info parser.AttrInfo) error {
	// todo
	// 1. check name exists
	// 2. check info valid
	// 3. check attrInfo valid (foreign key, primary key)
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	if err := m.checkDBTableAndAttrExistence(relName, []string{attrName}); err == nil {
		return errorutil.ErrorDBSysAttrExisted
	}
	relInfo := m.GetDBRelInfoMap()[relName]
	if info.IsPrimary || len(info.IndexName) > 0 || len(info.FkName) > 0 {
		return errorutil.ErrorDBSysAddComplicateColumnNotSupported
	}
	if relInfo.AttrCount+1 >= types.MaxAttrNums {
		return errorutil.ErrorDBSysMaxAttrExceeded
	}
	if relInfo.RecordSize+info.AttrSize+1 >= types.PageSize-int(unsafe.Sizeof(types.RecordHeaderPage{})) {
		return errorutil.ErrorDBSysBigRecordNotSupported
	}

	info.AttrOffset = relInfo.RecordSize
	info.RelName = relName
	relInfo.RecordSize = relInfo.RecordSize + info.AttrSize + 1
	relInfo.AttrCount++
	m.SetRelInfo(relInfo)

	_ = m.relManager.CreateFile("tmp", relInfo.RecordSize)
	tmpFH, _ := m.relManager.OpenFile("tmp")
	srcFH, _ := m.relManager.OpenFile(getTableDataFileName(relName))
	for _, rec := range srcFH.GetRecList() {
		tmpData := make([]byte, relInfo.RecordSize)
		copy(tmpData, rec.Data)
		copy(tmpData[info.AttrOffset:], info.Default.Value[0:info.AttrSize])
		_, _ = tmpFH.InsertRec(tmpData)
	}

	_ = m.relManager.CloseFile(srcFH.Filename)
	_ = m.relManager.CloseFile(tmpFH.Filename)
	_ = m.relManager.DestroyFile(srcFH.Filename)
	_ = os.Rename(env.WorkDir+"/"+tmpFH.Filename, env.WorkDir+"/"+srcFH.Filename)

	attrInfoList := m.GetAttrInfoList(relName)
	attrInfoList = append(attrInfoList, info)
	m.SetAttrInfoList(relName, attrInfoList)
	log.V(log.DBSysLevel).Infof("Add column succeed %v : %v %v", relName, attrName, info)
	return nil
}

func (m *Manager) DropColumn(relName string, attrName string) error {
	// TODO
	// check foreign constraint, if has foreign constraint -> drop
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	if err := m.checkDBTableAndAttrExistence(relName, []string{attrName}); err != nil {
		return err
	}
	relInfo := m.GetDBRelInfoMap()[relName]

	attrInfoCollection := m.GetAttrInfoCollection(relName)

	if relInfo.AttrCount == 1 {
		return errorutil.ErrorDBSysCannotRemoveLastColumn
	}
	if attrInfoCollection.InfoMap[attrName].IsPrimary {
		if relInfo.PrimaryCount == 1 {
			if err := m.DropPrimaryKey(relName); err != nil {
				return err
			}
		} else {
			return errorutil.ErrorDBSysCannotRemovePrimaryColumn
		}
	}
	if len(attrInfoCollection.InfoMap[attrName].FkName) != 0 {
		return errorutil.ErrorDBSysCannotRemoveForeignKeyCol
	}

	size := attrInfoCollection.InfoMap[attrName].AttrSize
	off := attrInfoCollection.InfoMap[attrName].AttrOffset

	_ = m.relManager.CreateFile("tmp", relInfo.RecordSize-size-1)
	tmpFH, _ := m.relManager.OpenFile("tmp")
	srcFH, _ := m.relManager.OpenFile(getTableDataFileName(relName))

	for _, rec := range srcFH.GetRecList() {
		tmpData := make([]byte, relInfo.RecordSize-size-1)
		copy(tmpData[0:off], rec.Data[0:off])
		copy(tmpData[off:], rec.Data[off+size+1:])
		_, _ = tmpFH.InsertRec(tmpData)
	}

	_ = m.relManager.CloseFile(srcFH.Filename)
	_ = m.relManager.CloseFile(tmpFH.Filename)
	_ = m.relManager.DestroyFile(srcFH.Filename)
	_ = os.Rename(env.WorkDir+"/"+tmpFH.Filename, env.WorkDir+"/"+srcFH.Filename)

	attrInfoList := m.GetAttrInfoList(relName)
	i := 0
	for {
		if attrInfoList[i].AttrName == attrName {
			break
		}
		i++
	}

	idxMap := m.GetAttrInfoCollection(relName).IdxMap
	if item, found := idxMap[attrInfoList[i].IndexName]; found && len(item) == 1 {
		_ = m.DropIndex(relName, attrName)
	}

	for j := i; j < len(attrInfoList); j++ {
		attrInfoList[j].AttrOffset = attrInfoList[j].AttrOffset - (size + 1)
	}
	attrInfoList = append(attrInfoList[0:i], attrInfoList[i+1:]...)
	m.SetAttrInfoList(relName, attrInfoList)
	relInfo.RecordSize = relInfo.RecordSize - size - 1
	relInfo.AttrCount = relInfo.AttrCount - 1
	m.SetRelInfo(relInfo)
	log.V(log.DBSysLevel).Infof("Drop column succeed %v : %v", relName, attrName)
	return nil
}

func (m *Manager) RenameTable(srcName, dstName string) error {
	if err := m.checkDBTableAndAttrExistence(srcName, nil); err != nil {
		return err
	}

	attrInfoCollection := m.GetAttrInfoCollection(srcName)
	for attr, attrInfo := range attrInfoCollection.InfoMap {
		attrInfo.RelName = dstName
		attrInfoCollection.InfoMap[attr] = attrInfo
	}
	m.SetAttrInfoListByCollection(srcName, attrInfoCollection)

	relInfoMap := m.GetDBRelInfoMap()
	if _, found := relInfoMap[dstName]; found {
		return errorutil.ErrorDBSysRelationExisted
	} else {
		relInfo := relInfoMap[srcName]
		relInfo.RelName = dstName
		relInfoMap[dstName] = relInfo
		delete(relInfoMap, srcName)
	}
	m.SetDBRelInfoMap(relInfoMap)

	// rename index file
	for idx := range attrInfoCollection.IdxMap {
		_ = os.Rename(env.WorkDir+"/"+getTableIdxDataFileName(srcName, idx), env.WorkDir+"/"+getTableIdxDataFileName(dstName, idx))
	}

	// rename data file
	_ = os.Rename(env.WorkDir+"/"+getTableDataFileName(srcName), env.WorkDir+"/"+getTableDataFileName(dstName))
	_ = os.Rename(env.WorkDir+"/"+getTableMetaFileName(srcName), env.WorkDir+"/"+getTableMetaFileName(dstName))

	// rename fk related
	fkMap := m.GetFkInfoMap()
	for key, val := range fkMap {
		if val.SrcRel == srcName {
			val.SrcRel = dstName
		}
		if val.DstRel == srcName {
			val.DstRel = dstName
		}
		fkMap[key] = val
	}
	m.SetFkInfoMap(fkMap)
	return nil
}

// ChangeColumn
// support incremental update
// change ValueType
// Null
// Default
const (
	ChangeValueType = 1 << iota
	ChangeNull
	ChangeDefault
)

func (m *Manager) ChangeColumn(relName, attrName string, info *parser.AttrInfo, changeField int) error {
	// support incremental value
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	if err := m.checkDBTableAndAttrExistence(relName, []string{attrName}); err != nil {
		return err
	}

	// check value type
	attrInfoCollection := m.GetAttrInfoCollection(relName)
	infoMap := attrInfoCollection.InfoMap
	if infoMap[attrName].IsPrimary || len(infoMap[attrName].FkName) != 0 {
		return errorutil.ErrorDBSysCannotChangePkFkColumn
	}

	if changeField&ChangeValueType != 0 {
		val := types.NewValueFromEmpty()
		val.ValueType = infoMap[attrName].AttrType
		val.AdaptToType(info.AttrType)
		if val.ValueType == types.NO_ATTR {
			return errorutil.ErrorDBSysUpdateValueTypeNotMatch
		}
	}

	off := infoMap[attrName].AttrOffset
	size := infoMap[attrName].AttrSize
	typ := infoMap[attrName].AttrType

	fh, _ := m.relManager.OpenFile(getTableDataFileName(relName))
	lastPage := 0
	for _, rec := range fh.GetRecList() {
		if changeField&ChangeValueType != 0 {
			val := types.NewValueFromByteSlice(rec.Data[off:off+size], typ)
			val.AdaptToType(info.AttrType)
			copy(rec.Data[off:off+size], val.Value[0:size])
		}
		if rec.Rid.Page != lastPage {
			fh.ForcePage(lastPage)
			lastPage = rec.Rid.Page
		}
	}
	fh.ForcePage(lastPage)

	attrInfo := infoMap[attrName]
	if changeField&ChangeNull != 0 {
		attrInfo.NullAllowed = info.NullAllowed
	}
	if changeField&ChangeDefault != 0 {
		val, err := types.String2Value(info.Default.ToStr(), typ, size)
		if err != nil {
			return err
		}
		attrInfo.Default = val
	}
	infoMap[attrName] = attrInfo
	attrInfoCollection.InfoMap = infoMap
	m.SetAttrInfoListByCollection(relName, attrInfoCollection)
	return nil
}
