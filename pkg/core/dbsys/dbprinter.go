package dbsys

import (
	"fmt"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"io/ioutil"
	"strconv"
	"strings"
)

func (m *Manager) PrintDatabases() {
	rootdir := "./"
	if m.DBSelected() {
		rootdir = "../"
	}
	files, _ := ioutil.ReadDir(rootdir)
	names := make([]string, 0, len(files))
	maxLen := len("Database")
	for _, f := range files {
		if f.IsDir() {
			names = append(names, f.Name())
			if len(f.Name()) > maxLen {
				maxLen = len(f.Name())
			}
		}
	}

	println("+" + strings.Repeat("-", maxLen+2) + "+")
	println("| " + "Database" + strings.Repeat(" ", maxLen-len("Database")) + " |")
	println("+" + strings.Repeat("-", maxLen+2) + "+")
	for _, f := range names {
		println("| " + f + strings.Repeat(" ", maxLen-len(f)) + " |")
	}
	println("+" + strings.Repeat("-", maxLen+2) + "+")
}

func (m *Manager) PrintTables() {
	if !m.DBSelected() {
		m.PrintEmptySet()
	} else {
		maxLen := len(m.dbSelected)
		for i := range m.rels {
			if len(i) > maxLen {
				maxLen = len(i)
			}
		}
		println("+" + strings.Repeat("-", maxLen+2) + "+")
		println("| " + m.dbSelected + strings.Repeat(" ", maxLen-len("Database")) + " |")
		println("+" + strings.Repeat("-", maxLen+2) + "+")
		for rel := range m.rels {
			println("| " + rel + strings.Repeat(" ", maxLen-len(rel)) + " |")
		}
		println("+" + strings.Repeat("-", maxLen+2) + "+")

	}
}

func (m *Manager) PrintDBForeignInfos() {
	fkInfoMap := m.GetFkInfoMap()
	tableHeaders := []string{"FkName", "SrcRel", "DstRel"}
	maxAttrCnt := 0

	col2Wid := map[string]int{
		"FkName": 6,
		"SrcRel": 6,
		"DstRel": 6,
	}

	for _, cons := range fkInfoMap {
		if len(cons.SrcAttr) > maxAttrCnt {
			maxAttrCnt = len(cons.SrcAttr)
		}
	}
	for i := 0; i < maxAttrCnt; i++ {
		header := "src" + strconv.Itoa(i)
		tableHeaders = append(tableHeaders, header)
		col2Wid[header] = len(header)
	}
	for i := 0; i < maxAttrCnt; i++ {
		header := "dst" + strconv.Itoa(i)
		tableHeaders = append(tableHeaders, header)
		col2Wid[header] = len(header)
	}

	for _, cons := range fkInfoMap {
		for i, attr := range cons.SrcAttr {
			header := "src" + strconv.Itoa(i)
			if len(attr) > col2Wid[header] {
				col2Wid[header] = len(attr)
			}
		}
		for i, attr := range cons.DstAttr {
			header := "dst" + strconv.Itoa(i)
			if len(attr) > col2Wid[header] {
				col2Wid[header] = len(attr)
			}
		}
	}

	for _, header := range tableHeaders {
		switch header {
		case "FkName":
			for _, cons := range fkInfoMap {
				if len(cons.FkName) > col2Wid[header] {
					col2Wid[header] = len(cons.FkName)
				}
			}
		case "SrcRel":
			for _, cons := range fkInfoMap {
				if len(cons.SrcRel) > col2Wid[header] {
					col2Wid[header] = len(cons.SrcRel)
				}
			}
		case "DstRel":
			for _, cons := range fkInfoMap {
				if len(cons.DstRel) > col2Wid[header] {
					col2Wid[header] = len(cons.DstRel)
				}
			}
		}
	}
	maxLen := 0
	for _, wid := range col2Wid {
		maxLen += wid + 2
	}
	maxLen += maxAttrCnt*2 + 3 - 1

	for _, header := range tableHeaders {
		print("+" + strings.Repeat("-", col2Wid[header]+2))
	}
	println("+")

	for _, header := range tableHeaders {
		print("| " + header + strings.Repeat(" ", col2Wid[header]-len(header)+1))
	}
	println("|")

	for _, header := range tableHeaders {
		print("+" + strings.Repeat("-", col2Wid[header]+2))
	}
	println("+")
	for fk, cons := range fkInfoMap {
		print("| " + fk + strings.Repeat(" ", col2Wid["FkName"]-len(fk)+1))
		print("| " + cons.SrcRel + strings.Repeat(" ", col2Wid["SrcRel"]-len(cons.SrcRel)+1))
		print("| " + cons.DstRel + strings.Repeat(" ", col2Wid["DstRel"]-len(cons.DstRel)+1))
		for i, attr := range cons.SrcAttr {
			header := "src" + strconv.Itoa(i)
			print("| " + attr + strings.Repeat(" ", col2Wid[header]-len(attr)+1))
		}
		for i := len(cons.SrcAttr); i < maxAttrCnt; i++ {
			header := "src" + strconv.Itoa(i)
			print("| " + strings.Repeat(" ", col2Wid[header]+1))
		}
		for i, attr := range cons.DstAttr {
			header := "dst" + strconv.Itoa(i)
			print("| " + attr + strings.Repeat(" ", col2Wid[header]-len(attr)+1))
		}
		for i := len(cons.DstAttr); i < maxAttrCnt; i++ {
			header := "dst" + strconv.Itoa(i)
			print("| " + strings.Repeat(" ", col2Wid[header]+1))
		}
	}
	if len(fkInfoMap) != 0 {
		println("|")
	}
	for _, header := range tableHeaders {
		print("+" + strings.Repeat("-", col2Wid[header]+2))
	}
	println("+")
	//maxLen += len(tableHeaders) - 1

}

func (m *Manager) PrintTablesWithDetails() {
	if !m.DBSelected() {
		m.PrintEmptySet()
	} else {
		relInfoMap := m.GetDBRelInfoMap()
		tableHeaders := []string{"RelationName", "RecordSize", "AttrCnt", "IndexCnt", "PrimaryCnt", "ForeignCnt"}
		col2Wid := make(map[string]int)
		maxLen := 0
		for _, header := range tableHeaders {
			col2Wid[header] = len(header)
			maxLen += len(header) + 2
		}
		maxLen += 5

		//println("+" + strings.Repeat("-", maxLen+2) + "+")
		//println("| " + m.dbSelected + strings.Repeat(" ", maxLen-len("Database")) + " |")
		for _, header := range tableHeaders {
			print("+" + strings.Repeat("-", len(header)+2))
		}
		println("+")
		for _, header := range tableHeaders {
			print("| " + header + " ")
		}
		println("|")
		for _, header := range tableHeaders {
			print("+" + strings.Repeat("-", len(header)+2))
		}
		println("+")
		for _, rel := range relInfoMap {
			print("| " + rel.RelName + strings.Repeat(" ", col2Wid["RelationName"]-len(rel.RelName)+1))
			print("| " + strconv.Itoa(rel.RecordSize) + strings.Repeat(" ", col2Wid["RecordSize"]-len(strconv.Itoa(rel.RecordSize))+1))
			print("| " + strconv.Itoa(rel.AttrCount) + strings.Repeat(" ", col2Wid["AttrCnt"]-len(strconv.Itoa(rel.AttrCount))+1))
			print("| " + strconv.Itoa(rel.IndexCount) + strings.Repeat(" ", col2Wid["IndexCnt"]-len(strconv.Itoa(rel.IndexCount))+1))
			print("| " + strconv.Itoa(rel.PrimaryCount) + strings.Repeat(" ", col2Wid["PrimaryCnt"]-len(strconv.Itoa(rel.PrimaryCount))+1))
			print("| " + strconv.Itoa(rel.ForeignCount) + strings.Repeat(" ", col2Wid["ForeignCnt"]-len(strconv.Itoa(rel.ForeignCount))+1))
			println("|")
		}
		for _, header := range tableHeaders {
			print("+" + strings.Repeat("-", len(header)+2))
		}
		println("+")
	}
}

func (m *Manager) PrintTableMeta(relName string) {
	relInfoMap := m.GetDBRelInfoMap()
	if _, found := relInfoMap[relName]; !found {
		m.PrintEmptySet()
	} else {
		attrInfoList := m.GetAttrInfoList(relName)
		tableHeaders := []string{
			"Field", "Type", "Size", "Offset", "IndexName", "NullAllowed", "IsPrimary", "FkName", "Default",
		}
		col2Wid := make(map[string]int)
		maxLen := 0
		for _, header := range tableHeaders {
			col2Wid[header] = len(header)
			switch header {
			case "Default":
				for _, attr := range attrInfoList {
					if len(attr.Default.Format2String()) > col2Wid[header] {
						col2Wid[header] = len(attr.Default.Format2String())
					}
				}
			case "Field":
				for _, attr := range attrInfoList {
					if len(attr.AttrName) > col2Wid[header] {
						col2Wid[header] = len(attr.AttrName)
					}
				}
			case "Type":
				for _, attr := range attrInfoList {
					if len(types.ValueTypeStringMap[attr.AttrType]) > col2Wid[header] {
						col2Wid[header] = len(types.ValueTypeStringMap[attr.AttrType])
					}
				}
			case "IndexName":
				for _, attr := range attrInfoList {
					if len(attr.IndexName) > col2Wid[header] {
						col2Wid[header] = len(attr.IndexName)
					}
				}
			case "FkName":
				for _, attr := range attrInfoList {
					if len(attr.FkName) > col2Wid[header] {
						col2Wid[header] = len(attr.FkName)
					}
				}
			}

			maxLen += col2Wid[header] + 2
		}
		maxLen += len(tableHeaders) - 1

		for _, header := range tableHeaders {
			print("+" + strings.Repeat("-", col2Wid[header]+2))
		}
		println("+")

		for _, header := range tableHeaders {
			print("| " + header + strings.Repeat(" ", col2Wid[header]-len(header)+1))
		}
		println("|")

		for _, header := range tableHeaders {
			print("+" + strings.Repeat("-", col2Wid[header]+2))
		}
		println("+")

		for _, attr := range attrInfoList {
			print("| " + attr.AttrName + strings.Repeat(" ", col2Wid["Field"]-len(attr.AttrName)+1))
			print("| " + types.ValueTypeStringMap[attr.AttrType] + strings.Repeat(" ", col2Wid["Type"]-len(types.ValueTypeStringMap[attr.AttrType])+1))
			print("| " + strconv.Itoa(attr.AttrSize) + strings.Repeat(" ", col2Wid["Size"]-len(strconv.Itoa(attr.AttrSize))+1))
			print("| " + strconv.Itoa(attr.AttrOffset) + strings.Repeat(" ", col2Wid["Offset"]-len(strconv.Itoa(attr.AttrOffset))+1))
			print("| " + attr.IndexName + strings.Repeat(" ", col2Wid["IndexName"]-len(attr.IndexName)+1))
			print("| " + strconv.FormatBool(attr.NullAllowed) + strings.Repeat(" ", col2Wid["NullAllowed"]-len(strconv.FormatBool(attr.NullAllowed))+1))
			print("| " + strconv.FormatBool(attr.IsPrimary) + strings.Repeat(" ", col2Wid["IsPrimary"]-len(strconv.FormatBool(attr.IsPrimary))+1))
			print("| " + attr.FkName + strings.Repeat(" ", col2Wid["FkName"]-len(attr.FkName)+1))
			print("| " + attr.Default.Format2String() + strings.Repeat(" ", col2Wid["Default"]-len(attr.Default.Format2String())+1))
			println("|")
		}
		for _, header := range tableHeaders {
			print("+" + strings.Repeat("-", col2Wid[header]+2))
		}
		println("+")

	}

}

//func (m *Manager) PrintTablesWithDetails() {
//	if !m.DBSelected() {
//		PrintEmptySet()
//	} else {
//		tableShowingDescribedInfo, _ := m.getRelMetaPrintInfo()
//		m.PrintTableByInfo(m.dbMeta.GetRecList(), tableShowingDescribedInfo)
//	}
//}

func (m *Manager) PrintEmptySet() {
	println("+---------------+")
	println("|     empty     |")
	println("+---------------+")
}

func (m *Manager) GetTableShowingInfo(relName string) (*TablePrintInfo, error) {
	if !m.DBSelected() {
		return nil, errorutil.ErrorDBSysDBNotSelected
	}
	if _, found := m.rels[relName]; !found {
		return nil, errorutil.ErrorDBSysRelationNotExisted
	}

	tableHeaderList := make([]string, 0, types.MaxAttrNums)
	offsetList := make([]int, 0, types.MaxAttrNums)
	sizeList := make([]int, 0, types.MaxAttrNums)
	typeList := make([]types.ValueType, 0, types.MaxAttrNums)
	nullList := make([]bool, 0, types.MaxAttrNums)

	attrInfoList := m.GetAttrInfoList(relName)
	for _, attr := range attrInfoList {
		tableHeaderList = append(tableHeaderList, attr.AttrName)
		offsetList = append(offsetList, attr.AttrOffset)
		sizeList = append(sizeList, attr.AttrSize)
		typeList = append(typeList, attr.AttrType)
		nullList = append(nullList, attr.NullAllowed)
	}

	// compute the base length info for each item
	colWidMap := make(map[string]int)
	for _, head := range tableHeaderList {
		colWidMap[head] = len(head)
	}

	// null takes up at least 4, it's useless for metadata showing
	for j := 0; j < len(tableHeaderList); j++ {
		if nullList[j] {
			colWidMap[tableHeaderList[j]] = 4
		}
	}

	// changing showing list for table content itself, naming here might be confusing
	fileHandle, err := m.relManager.OpenFile(getTableDataFileName(relName))
	if err != nil {
		log.V(log.DBSysLevel).Error(err)
		return nil, err
	}
	defer m.relManager.CloseFile(fileHandle.Filename)
	rawRecordList := fileHandle.GetRecList()

	// compute the appropriate length for each component after necessary scanning of each item
	for _, rec := range rawRecordList {
		for j := 0; j < len(tableHeaderList); j++ {
			if length := len(data2StringByTypes(rec.Data[offsetList[j]:offsetList[j]+sizeList[j]], typeList[j])); length > colWidMap[tableHeaderList[j]] {
				colWidMap[tableHeaderList[j]] = length
			}

		}
	}
	return &TablePrintInfo{
		TableHeaderList: tableHeaderList,
		OffsetList:      offsetList,
		SizeList:        sizeList,
		TypeList:        typeList,
		NullList:        nullList,
		ColWidMap:       colWidMap,
	}, nil
}

func (m *Manager) PrintTemporalTable(table *TemporalTable) {
	printInfo := m.GetTemporalTableShowingInfo(table)
	m.PrintTableByInfo(table.rows, printInfo)
}

func (m *Manager) GetTemporalTableShowingInfo(table *TemporalTable) *TablePrintInfo {
	// calculate widths
	colWidMap := make(map[string]int)
	for i, attr := range table.attrs {
		colWidMap[attr] = len(attr)
		if table.nils[i] && colWidMap[attr] < 4 {
			colWidMap[attr] = 4
		}
	}
	for _, rec := range table.rows {
		for i := 0; i < len(table.attrs); i++ {
			off := table.offs[i]
			size := table.lens[i]
			if length := len(data2StringByTypes(rec.Data[off:off+size], table.types[i])); length > colWidMap[table.attrs[i]] {
				colWidMap[table.attrs[i]] = length
			}
		}
	}
	return &TablePrintInfo{
		TableHeaderList: table.attrs,
		OffsetList:      table.offs,
		SizeList:        table.lens,
		TypeList:        table.types,
		NullList:        table.nils,
		ColWidMap:       colWidMap,
	}
}

type TablePrintInfo struct {
	TableHeaderList []string
	OffsetList      []int
	SizeList        []int
	TypeList        []types.ValueType
	NullList        []bool
	ColWidMap       map[string]int // col width is computed from every item in the table
}

func (m *Manager) PrintTableByInfo(recordList []*record.Record, info *TablePrintInfo) {

	// print header
	for i := 0; i < len(info.TableHeaderList); i++ {
		print("+" + strings.Repeat("-", info.ColWidMap[info.TableHeaderList[i]]+2))
	}
	println("+")
	for i := 0; i < len(info.TableHeaderList); i++ {
		print("| " + info.TableHeaderList[i] + strings.Repeat(" ", info.ColWidMap[info.TableHeaderList[i]]-len(info.TableHeaderList[i])) + " ")
	}
	println("|")
	for i := 0; i < len(info.TableHeaderList); i++ {
		print("+" + strings.Repeat("-", info.ColWidMap[info.TableHeaderList[i]]+2))
	}
	println("+")

	// print content by iterating each row & col

	for _, rec := range recordList {
		str := ""
		for j := 0; j < len(info.TableHeaderList); j++ {
			// different print case are handled here
			byteSlice := rec.Data[info.OffsetList[j] : info.OffsetList[j]+info.SizeList[j]]
			if info.NullList[j] {
				if rec.Data[info.OffsetList[j]+info.SizeList[j]] == 1 {
					// a single col always takes up (size + 1 bit)
					str = "NULL"
				} else {
					str = data2StringByTypes(byteSlice, info.TypeList[j])
				}
			} else {
				str = data2StringByTypes(byteSlice, info.TypeList[j])
			}

			print("| " + str + strings.Repeat(" ", info.ColWidMap[info.TableHeaderList[j]]-len(str)+1))

		}
		print("|\n")
	}

	// print tails
	for i := 0; i < len(info.TableHeaderList); i++ {
		print("+" + strings.Repeat("-", info.ColWidMap[info.TableHeaderList[i]]+2))
	}
	println("+")
	fmt.Printf("%v in set\n", len(recordList))
}

// PrintTableMeta is implemented since GetRecordShould be wrapped up
// since GetTableShowingInfo & PrintTableByInfo provide a unified access for table printing
//func (m *Manager) PrintTableMeta(relName string) error {
//	tableShowingDescribedInfo, err := m.GetTableShowingInfo(relName, true)
//	if err != nil {
//		return err
//	}
//	// these must not have error since file get opened before
//	fileHandle, _ := m.relManager.OpenFile(getTableMetaFileName(relName))
//	defer m.relManager.CloseFile(fileHandle.Filename)
//	recList := fileHandle.GetRecList()
//	m.PrintTableByInfo(recList, tableShowingDescribedInfo)
//	return nil
//}

func (m *Manager) PrintTableData(relName string) error {
	tableShowingDescribedInfo, err := m.GetTableShowingInfo(relName)
	if err != nil {
		return err
	}
	// these must not have error since file get opened before
	fileHandle, _ := m.relManager.OpenFile(getTableDataFileName(relName))
	defer m.relManager.CloseFile(fileHandle.Filename)
	recList := fileHandle.GetRecList()
	m.PrintTableByInfo(recList, tableShowingDescribedInfo)
	return nil
}

//func (m *Manager) PrintTableIndex(relName string) error {
//	tableShowingDescribedInfo, err := m.getIndexMetaPrintInfo(relName)
//	if err != nil {
//		return err
//	}
//	// these must not have error since file get opened before
//	fileHandle, _ := m.relManager.OpenFile(getTableIdxMetaFileName(relName))
//	defer m.relManager.CloseFile(fileHandle.Filename)
//	recList := fileHandle.GetRecList()
//	m.PrintTableByInfo(recList, tableShowingDescribedInfo)
//	return nil
//}

// some other utils
//type IndexInfo struct {
//	idxNo   int
//	idxName [types.MaxNameSize]byte
//	col     [types.MaxNameSize]byte
//}
//func (m *Manager) getIndexMetaPrintInfo(relName string) (*TablePrintInfo, error) {
//	if !m.DBSelected() {
//		return nil, errorutil.ErrorDBSysDBNotSelected
//	}
//	if _, found := m.rels[relName]; !found {
//		return nil, errorutil.ErrorDBSysRelationNotExisted
//	}
//	tableHeaderList := []string{"idxNo", "idxName", "column"}
//	offsetList := []int{0, 8, 32}
//	sizeList := []int{8, 24, 24}
//	typeList := []types.ValueType{types.INT, types.VARCHAR, types.VARCHAR}
//	colWidMap := map[string]int{"idxNo": 5, "idxName": 7, "column": 6}
//
//	// compute the appropriate length for each component after necessary scanning of each item
//	fh, err := m.relManager.OpenFile(getTableIdxMetaFileName(relName))
//	if err != nil {
//		return nil, err
//	}
//	defer m.relManager.CloseFile(getTableIdxMetaFileName(relName))
//
//	recCnt := 0
//	for _, rec := range fh.GetRecList() {
//		recCnt += 1
//		for j := 0; j < len(tableHeaderList); j++ {
//			if length := len(data2StringByTypes(rec.Data[offsetList[j]:offsetList[j]+sizeList[j]], typeList[j])); length > colWidMap[tableHeaderList[j]] {
//				colWidMap[tableHeaderList[j]] = length
//			}
//		}
//	}
//
//	return &TablePrintInfo{
//		TableHeaderList: tableHeaderList,
//		OffsetList:      offsetList,
//		SizeList:        sizeList,
//		TypeList:        typeList,
//		NullList:        nil,
//		ColWidMap:       colWidMap,
//	}, nil
//}

//type RelInfo struct {
//	RelName      [types.MaxNameSize]byte
//	RecordSize   int
//	AttrCount    int
//	nextIndexNo  int
//	IndexCount   int // index constraint count
//	PrimaryCount int // primary constraint count
//	ForeignCount int // foreign constraint count
//}
//func (m *Manager) getRelMetaPrintInfo() (*TablePrintInfo, error) {
//	if !m.DBSelected() {
//		return nil, errorutil.ErrorDBSysDBNotSelected
//	}
//	tableHeaderList := []string{"RelName", "RecordSize", "AttrCount", "nextIndexNo", "IndexCount", "PrimaryCount", "ForeignCount"}
//	offsetList := []int{0, 24, 32, 40, 48, 56, 64}
//	sizeList := []int{24, 8, 8, 8, 8, 8, 8}
//	typeList := []types.ValueType{types.VARCHAR, types.INT, types.INT, types.INT, types.INT, types.INT, types.INT}
//	colWidMap := make(map[string]int)
//	for _, name := range tableHeaderList {
//		colWidMap[name] = len(name)
//	}
//
//	recCnt := 0
//	for _, rec := range m.dbMeta.GetRecList() {
//		recCnt += 1
//		for j := 0; j < len(tableHeaderList); j++ {
//			if length := len(data2StringByTypes(rec.Data[offsetList[j]:offsetList[j]+sizeList[j]], typeList[j])); length > colWidMap[tableHeaderList[j]] {
//				colWidMap[tableHeaderList[j]] = length
//			}
//		}
//	}
//	return &TablePrintInfo{
//		TableHeaderList: tableHeaderList,
//		OffsetList:      offsetList,
//		SizeList:        sizeList,
//		TypeList:        typeList,
//		NullList:        nil,
//		ColWidMap:       colWidMap,
//		VariantTypeList: make([]types.ValueType, recCnt), // this field will be of no use when not print table meta default value for each record
//		ShowingMeta:     true,
//	}, nil
//}
