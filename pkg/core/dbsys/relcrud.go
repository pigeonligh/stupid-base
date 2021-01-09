package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/index"
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
)

// SELECT <selector> FROM <tableList> WHERE <whereClause>
// current support simple select functions
func (m *Manager) SelectSingleTableByExpr(relName string, attrNameList []string, expr *parser.Expr) (*TemporalTable, error) {
	if err := m.checkDBTableAndAttrExistence(relName, attrNameList); err != nil {
		return nil, err
	}
	attrInfoMap := m.GetAttrInfoCollection(relName).InfoMap
	datafile, _ := m.relManager.OpenFile(getTableDataFileName(relName))
	defer m.relManager.CloseFile(datafile.Filename)

	recList, err := datafile.GetFilteredRecList(expr)
	if err != nil {
		return nil, err
	}

	rels := make([]string, 0)
	attrs := make([]string, 0)
	offs := make([]int, 0)
	newOffs := make([]int, 0)
	lens := make([]int, 0)
	valTypes := make([]types.ValueType, 0)
	nils := make([]bool, 0)

	totLen := 0
	for _, attr := range attrNameList {
		rels = append(rels, relName)
		attrs = append(attrs, attr)
		offs = append(offs, attrInfoMap[attr].AttrOffset)
		lens = append(lens, attrInfoMap[attr].AttrSize)
		valTypes = append(valTypes, attrInfoMap[attr].AttrType)
		nils = append(nils, attrInfoMap[attr].NullAllowed)
		newOffs = append(newOffs, totLen)
		totLen += attrInfoMap[attr].AttrSize + 1
	}

	rows := make([]*record.Record, 0)
	for _, rec := range recList {
		tmpRec := record.Record{
			Rid:  types.RID{},
			Data: make([]byte, totLen),
		}
		for i := range offs {
			copy(tmpRec.Data[newOffs[i]:newOffs[i]+lens[i]], rec.Data[offs[i]:offs[i]+lens[i]])
		}
		rows = append(rows, &tmpRec)
	}
	return &TemporalTable{
		rels:  rels,
		attrs: attrs,
		lens:  lens,
		offs:  newOffs,
		types: valTypes,
		nils:  nils,
		rows:  rows,
	}, nil
}

// DELETE FROM <tbName> WHERE <whereClause>
func (m *Manager) DeleteRows(relName string, expr *parser.Expr) error {
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	if _, found := m.rels[relName]; !found {
		return errorutil.ErrorDBSysRelationNotExisted
	}
	relInfo := m.GetDBRelInfoMap()[relName]
	attrInfoCollection := m.GetAttrInfoCollection(relName)

	fh, _ := m.relManager.OpenFile(getTableDataFileName(relName))
	defer m.relManager.CloseFile(fh.Filename)

	delRecList, err := fh.GetFilteredRecList(expr)
	if err != nil {
		return err
	}

	// if it is primary key and get referenced, check
	if relInfo.ForeignCount > 0 && relInfo.PrimaryCount > 0 {
		fkAttrs := attrInfoCollection.PkList // got referenced must be primary key
		fkAttrSet := m.GetAttrSetFromAttrs(relName, fkAttrs)
		fkInfoMap := m.GetFkInfoMap()
		for fkName := range attrInfoCollection.FkMap {
			if fkInfoMap[fkName].DstRel == relName {
				srcAttrSet := m.GetAttrSetFromAttrs(fkInfoMap[fkName].SrcRel, fkInfoMap[fkName].SrcAttr)
				srcFH, _ := m.relManager.OpenFile(getTableDataFileName(fkInfoMap[fkName].SrcRel))
				defer m.relManager.CloseFile(srcFH.Filename)

				// N * N, find if there's a match between src all records and delete records
				// todo there probably a chance to check index file
				// for simplicity we just ignore the case here
				for _, rec := range srcFH.GetRecList() {
					srcData := srcAttrSet.DataToAttrs(rec.Rid, rec.Data)
					for _, rec := range delRecList {
						if compareBytes(srcData, fkAttrSet.DataToAttrs(rec.Rid, rec.Data)) == 0 { // todo, there one case that remove primary key
							return errorutil.ErrorDBSysForeignKeyConstraintNotMatch
						}
					}
				}
			}
		}
	}

	if relInfo.IndexCount > 0 {
		// contains the upper case, delete the index key rids
		for idxName := range attrInfoCollection.IdxMap {
			idxFH, _ := m.idxManager.OpenIndex(getTableIdxDataFileName(relName, idxName), fh)
			defer m.idxManager.CloseIndex(getTableIdxDataFileName(relName, idxName))
			err := idxFH.DeleteEntryByBatch(record.GetRidListFromRecList(delRecList))
			if err != nil {
				panic(err)
			}
		}
	}
	fh.DeleteRecByBatch(record.GetRidListFromRecList(delRecList))
	return nil
}

// UPDATE <tbName> SET <setClause> WHERE <whereClause>
func (m *Manager) UpdateRows(relName string, attrNameList []string, valueList []types.Value, expr *parser.Expr) error {
	// check if it's primary key and referenced by others
	// check if it's referencing other primary keys
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	if err := m.checkDBTableAndAttrExistence(relName, attrNameList); err != nil {
		return err
	}
	attrInfoCollection := m.GetAttrInfoCollection(relName)
	infoMap := attrInfoCollection.InfoMap
	// check value compatible
	for i := range attrNameList {
		if !(valueList[i].ValueType == types.NO_ATTR && infoMap[attrNameList[i]].NullAllowed) {
			return errorutil.ErrorDBSysNullConstraintViolated
		}
		valueList[i].AdaptToType(infoMap[attrNameList[i]].AttrType)
		if valueList[i].ValueType == types.NO_ATTR {
			return errorutil.ErrorDBSysUpdateValueTypeNotMatch
		}

	}

	tmpTable, err := m.SelectSingleTableByExpr(relName, attrInfoCollection.NameList, expr)
	if err != nil {
		return err
	}
	if len(tmpTable.rows) == 0 {
		return nil
	}
	fh, err := m.relManager.OpenFile(getTableDataFileName(relName))
	defer m.relManager.CloseFile(fh.Filename)

	name2Val := make(map[string]types.Value)
	for i, attr := range attrNameList {
		name2Val[attr] = valueList[i]
	}

	// check if primary keys are contained in attrName list
	checkPrimary := checkIfaLEb(attrInfoCollection.PkList, attrNameList)

	// update temporal table (not write yet)

	// previous records
	prevData := make([][]byte, 0)

	for _, rec := range tmpTable.rows {
		tmpData := make([]byte, len(rec.Data))
		copy(tmpData, rec.Data)
		prevData = append(prevData, tmpData)
		for attr, val := range name2Val {
			off := infoMap[attr].AttrOffset
			size := infoMap[attr].AttrSize
			copy(rec.Data[off:off+size], val.Value[0:size])
		}
	}

	if checkPrimary {
		if len(tmpTable.rows) > 1 {
			return errorutil.ErrorDBSysDuplicatedKeysFound // must duplicate
		} else {
			attrSet := m.GetAttrSetFromAttrs(relName, attrInfoCollection.PkList)
			idxFile, _ := m.idxManager.OpenIndex(getTableIdxDataFileName(relName, PrimaryKeyIdxName), fh)
			defer m.idxManager.CloseIndex(getTableIdxDataFileName(relName, PrimaryKeyIdxName))
			compData := attrSet.DataToAttrs(types.RID{}, tmpTable.rows[0].Data)
			length := len(idxFile.GetRidList(types.OpCompEQ, compData))
			if length != 0 {
				return errorutil.ErrorDBSysDuplicatedKeysFound
			}
		}
	}

	// check foreign constraint
	fkInfoMap := m.GetFkInfoMap()
	for fkName, fkAttrs := range attrInfoCollection.FkMap {
		if checkIfaLEb(fkAttrs, attrNameList) {
			// should check this constraint
			fkInfo := fkInfoMap[fkName]
			if fkInfo.SrcRel == relName {
				// check if the altered value will satisfied
				attrSet := m.GetAttrSetFromAttrs(fkInfo.SrcRel, fkInfo.SrcAttr)
				datafile, _ := m.relManager.OpenFile(getTableDataFileName(getTableDataFileName(fkInfo.DstRel)))
				idxFile, _ := m.idxManager.OpenIndex(getTableIdxDataFileName(fkInfo.DstRel, PrimaryKeyIdxName), datafile)
				defer m.relManager.CloseFile(datafile.Filename)
				defer m.idxManager.CloseIndex(getTableIdxDataFileName(fkInfo.DstRel, PrimaryKeyIdxName))
				compData := attrSet.DataToAttrs(types.RID{}, tmpTable.rows[0].Data)
				length := len(idxFile.GetRidList(types.OpCompEQ, compData))
				if length == 0 {
					return errorutil.ErrorDBSysForeignKeyConstraintNotMatch
				}
			}
			if fkInfo.DstRel == relName {
				// referenced by others
				// must be primary key
				// must only has one record to be altered
				dstAttrSet := m.GetAttrSetFromAttrs(fkInfo.DstRel, fkInfo.DstAttr)
				if len(prevData[0]) != 1 {
					panic(0)
				}
				if compareBytes(dstAttrSet.DataToAttrs(types.RID{}, prevData[0]), dstAttrSet.DataToAttrs(types.RID{}, tmpTable.rows[0].Data)) == 0 {
					// nothing changed for foreign keys
					continue
				}

				srcAttrSet := m.GetAttrSetFromAttrs(fkInfoMap[fkName].SrcRel, fkInfoMap[fkName].SrcAttr)
				srcFH, _ := m.relManager.OpenFile(getTableDataFileName(fkInfoMap[fkName].SrcRel))
				defer m.relManager.CloseFile(srcFH.Filename)
				for _, rec := range srcFH.GetRecList() {
					srcData := srcAttrSet.DataToAttrs(rec.Rid, rec.Data)
					if compareBytes(srcData, dstAttrSet.DataToAttrs(types.RID{}, prevData[0])) == 0 {
						return errorutil.ErrorDBSysForeignKeyConstraintNotMatch
					}
				}
			}
		}
	}

	insRids := make([]types.RID, 0)
	delRids := record.GetRidListFromRecList(tmpTable.rows)
	for _, rec := range tmpTable.rows {
		rid, err := fh.InsertRec(rec.Data)
		if err != nil {
			panic(err)
		}
		insRids = append(insRids, rid)
	}

	// insert into index file
	for idxName, idxAttrs := range attrInfoCollection.IdxMap {
		if checkIfaLEb(attrNameList, idxAttrs) {
			idxFile, _ := m.idxManager.OpenIndex(getTableIdxDataFileName(relName, idxName), fh)
			defer m.idxManager.CloseIndex(getTableIdxDataFileName(relName, idxName))
			if err := idxFile.DeleteEntryByBatch(delRids); err != nil {
				panic(err)
			}
			for _, rid := range insRids {
				_ = idxFile.InsertEntry(rid)
			}
		}
	}
	fh.DeleteRecByBatch(delRids)
	return nil
}

// INSERT INTO <tbName> VALUES <valueLists>
func (m *Manager) InsertRow(relName string, valueList []types.Value) error {
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	if _, found := m.rels[relName]; !found {
		return errorutil.ErrorDBSysRelationNotExisted
	}

	relInfoMap := m.GetDBRelInfoMap()
	attrInfoCollection := m.GetAttrInfoCollection(relName)
	insData := make([]byte, relInfoMap[relName].RecordSize)

	// open file
	fh, err := m.relManager.OpenFile(getTableDataFileName(relName))
	defer m.relManager.CloseFile(fh.Filename)
	if err != nil {
		panic(0)
	}

	for i, attrName := range attrInfoCollection.NameList {
		// check basic value type match
		attrInfo := attrInfoCollection.InfoMap[attrName]
		valueList[i].AdaptToType(attrInfo.AttrType)
		if valueList[i].ValueType == types.NO_ATTR {
			return errorutil.ErrorDBSysInsertValueTypeNotMatch
		}

		// concat into a row
		off := attrInfo.AttrOffset
		size := attrInfo.AttrSize
		copy(insData[off:off+size], valueList[i].Value[0:size])
	}

	{
		// check foreign key constraint
		// insert we only need to check when RelName is fk's src (referencing other tables' primary key)

		fkInfo := m.GetFkInfoMap()
		for fk, cons := range fkInfo {
			if cons.SrcRel == relName {
				attrSet := m.GetAttrSetFromAttrs(relName, cons.SrcAttr)
				compData := attrSet.DataToAttrs(types.RID{}, insData)

				if fk != PrimaryKeyIdxName {
					panic(0) // must reference other table's primary key
				}
				dataFH, _ := m.relManager.OpenFile(getTableDataFileName(cons.DstRel))
				idxFH, _ := m.idxManager.OpenIndex(getTableIdxDataFileName(cons.DstRel, fk), dataFH)
				defer m.relManager.CloseFile(getTableDataFileName(cons.DstRel))
				defer m.idxManager.CloseIndex(getTableIdxDataFileName(cons.DstRel, fk))

				if len(idxFH.GetRidList(types.OpCompEQ, compData)) == 0 {
					return errorutil.ErrorDBSysFkValueNotInPk
				}
			}
		}
	}

	{
		// check primary
		var idxFile *index.FileHandle
		if len(attrInfoCollection.PkList) != 0 {
			// has primary key constraint
			attrSet := m.GetAttrSetFromAttrs(relName, attrInfoCollection.PkList)
			idxFile, _ = m.idxManager.OpenIndex(getTableIdxDataFileName(relName, PrimaryKeyIdxName), fh)
			defer m.idxManager.CloseIndex(getTableIdxDataFileName(relName, PrimaryKeyIdxName))
			compData := attrSet.DataToAttrs(types.RID{}, insData)
			length := len(idxFile.GetRidList(types.OpCompEQ, compData))
			if length != 0 {
				return errorutil.ErrorDBSysDuplicatedKeysFound
			}
		}
	}

	{
		// insert and insert into all index files
		rid, err := fh.InsertRec(insData)
		if err != nil {
			panic(err)
		}
		for idxName := range attrInfoCollection.IdxMap {
			idxFile, _ := m.idxManager.OpenIndex(getTableIdxDataFileName(relName, idxName), fh)
			_ = idxFile.InsertEntry(rid)
			defer m.idxManager.CloseIndex(getTableIdxDataFileName(relName, idxName))

		}
	}
	return nil
}

// used for database query, since only some of the col are selected
//type TemporalTable = []TableColumn
//
//type TableColumn struct {
//	RelName     string
//	attrName    string
//	attrSize    int
//	attrType    int
//	nullAllowed bool
//	valueList   []types.Value
//}

// maybe it can be used for select & join
//func (m *Manager) GetTemporalTableByAttrs(RelName string, attrNameList []string, expr *parser.Expr) TemporalTable {
//	retTempTable := make(TemporalTable, 0)
//
//	attrInfoMap := m.getAttrInfoMapViaCacheOrReload(RelName, nil)
//
//	datafile, err := m.relManager.OpenFile(getTableDataFileName(RelName))
//	if err != nil {
//		log.V(log.DBSysLevel).Error(errorutil.ErrorDBSysRelationNotExisted)
//		return nil
//	}
//	defer m.relManager.CloseFile(datafile.Filename)
//
//	recordList, _ := record.FilterOnRecList(datafile.GetRecList(), expr)
//	for _, attr := range attrNameList {
//		col := TableColumn{
//			RelName:   RelName,
//			attrName:  attr,
//			valueList: make([]types.Value, 0),
//		}
//		offset := attrInfoMap[attr].AttrOffset
//		length := attrInfoMap[attr].AttrSize
//		attrType := attrInfoMap[attr].AttrType
//		for _, rec := range recordList {
//			if rec.Data[offset+length] == 1 {
//				attrType = types.NO_ATTR // mark null here
//			}
//			col.valueList = append(col.valueList, types.NewValueFromByteSlice(rec.Data[offset:offset+length], attrType))
//		}
//		col.attrSize = length
//		col.attrType = attrType
//		col.nullAllowed = attrInfoMap[attr].NullAllowed
//		retTempTable = append(retTempTable, col)
//	}
//	return retTempTable
//}

//func (m *Manager) PrintTableByTmpColumns(table TemporalTable) {
//	printInfo := &TablePrintInfo{
//		TableHeaderList: make([]string, 0),
//		OffsetList:      make([]int, 0),
//		SizeList:        make([]int, 0),
//		TypeList:        make([]int, 0),
//		NullList:        make([]bool, 0),
//		ColWidMap:       make(map[string]int),
//		ShowingMeta:     false,
//	}
//	// construct a record list
//	recordNums := len(table[0].valueList)
//	RecordSize := 0
//	for _, col := range table {
//		if len(col.valueList) != recordNums {
//			panic(0)
//		}
//		printInfo.ColWidMap[col.attrName] = len(col.attrName)
//		printInfo.TableHeaderList = append(printInfo.TableHeaderList, col.attrName)
//		printInfo.OffsetList = append(printInfo.OffsetList, RecordSize)
//		printInfo.SizeList = append(printInfo.SizeList, col.attrSize)
//		printInfo.TypeList = append(printInfo.TypeList, col.attrType)
//		printInfo.NullList = append(printInfo.NullList, col.nullAllowed)
//
//		RecordSize += col.attrSize + 1
//	}
//	recList := make([]*record.Record, 0)
//
//	for i := 0; i < recordNums; i++ {
//		rec := record.Record{
//			Rid:  types.RID{},
//			Data: make([]byte, RecordSize),
//		}
//		for j := 0; j < len(table); j++ {
//			copy(rec.Data[printInfo.OffsetList[j]:printInfo.OffsetList[j]+printInfo.SizeList[j]], table[i].valueList[i].Value[0:printInfo.SizeList[j]])
//			if len(table[i].valueList[i].Format2String()) > printInfo.ColWidMap[table[i].attrName] {
//				printInfo.ColWidMap[table[i].attrName] = len(table[i].valueList[i].Format2String())
//			}
//		}
//		recList = append(recList, &rec)
//	}
//	m.PrintTableByInfo(recList, printInfo)
//}
