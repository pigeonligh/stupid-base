package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

func (m *Manager) GetTemporalTable(relName string, attrNameList []string, expr *parser.Expr) (*TemporalTable, error) {
	if err := m.checkDBTableAndAttrExistence(relName, attrNameList); err != nil {
		return nil, err
	}
	attrInfoMap := m.getAttrInfoMapViaCacheOrReload(relName, nil)
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

// used for database query, since only some of the col are selected
//type TemporalTable = []TableColumn
//
//type TableColumn struct {
//	relName     string
//	attrName    string
//	attrSize    int
//	attrType    int
//	nullAllowed bool
//	valueList   []types.Value
//}

// maybe it can be used for select & join
//func (m *Manager) GetTemporalTableByAttrs(relName string, attrNameList []string, expr *parser.Expr) TemporalTable {
//	retTempTable := make(TemporalTable, 0)
//
//	attrInfoMap := m.getAttrInfoMapViaCacheOrReload(relName, nil)
//
//	datafile, err := m.relManager.OpenFile(getTableDataFileName(relName))
//	if err != nil {
//		log.V(log.DBSysLevel).Error(errorutil.ErrorDBSysRelationNotExisted)
//		return nil
//	}
//	defer m.relManager.CloseFile(datafile.Filename)
//
//	recordList, _ := record.FilterOnRecList(datafile.GetRecList(), expr)
//	for _, attr := range attrNameList {
//		col := TableColumn{
//			relName:   relName,
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
//	recordSize := 0
//	for _, col := range table {
//		if len(col.valueList) != recordNums {
//			panic(0)
//		}
//		printInfo.ColWidMap[col.attrName] = len(col.attrName)
//		printInfo.TableHeaderList = append(printInfo.TableHeaderList, col.attrName)
//		printInfo.OffsetList = append(printInfo.OffsetList, recordSize)
//		printInfo.SizeList = append(printInfo.SizeList, col.attrSize)
//		printInfo.TypeList = append(printInfo.TypeList, col.attrType)
//		printInfo.NullList = append(printInfo.NullList, col.nullAllowed)
//
//		recordSize += col.attrSize + 1
//	}
//	recList := make([]*record.Record, 0)
//
//	for i := 0; i < recordNums; i++ {
//		rec := record.Record{
//			Rid:  types.RID{},
//			Data: make([]byte, recordSize),
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
