package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"io/ioutil"
	"strings"
)

func (m *Manager) ShowDatabases() {
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

func (m *Manager) ShowTables() {
	if !m.DBSelected() {
		PrintEmptySet()
	} else {
		maxLen := len(m.dbSelected)
		for i := range m.rels {
			if len(i) > maxLen {
				maxLen = len(i)
			}
		}
		println("+" + strings.Repeat("-", maxLen+2) + "+")
		println("| " + m.dbSelected + strings.Repeat(" ", maxLen-len("Database")) + " |")
		for rel := range m.rels {
			println("| " + rel + strings.Repeat(" ", maxLen-len(rel)) + " |")
		}
		println("+" + strings.Repeat("-", maxLen+2) + "+")

	}
}

//func (m *Manager) ShowTablesWithDetails() {
//	if !m.DBSelected() {
//		PrintEmptySet()
//	} else {
//		tableShowingDescribedInfo, _ := m.getRelMetaPrintInfo()
//		m.PrintTableByInfo(m.dbMeta.GetRecList(), tableShowingDescribedInfo)
//	}
//}

func PrintEmptySet() {
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
	println("+\n")
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

func (m *Manager) PrintTableIndex(relName string) error {
	tableShowingDescribedInfo, err := m.getIndexMetaPrintInfo(relName)
	if err != nil {
		return err
	}
	// these must not have error since file get opened before
	fileHandle, _ := m.relManager.OpenFile(getTableIdxMetaFileName(relName))
	defer m.relManager.CloseFile(fileHandle.Filename)
	recList := fileHandle.GetRecList()
	m.PrintTableByInfo(recList, tableShowingDescribedInfo)
	return nil
}

// some other utils
//type IndexInfo struct {
//	idxNo   int
//	idxName [types.MaxNameSize]byte
//	col     [types.MaxNameSize]byte
//}
func (m *Manager) getIndexMetaPrintInfo(relName string) (*TablePrintInfo, error) {
	if !m.DBSelected() {
		return nil, errorutil.ErrorDBSysDBNotSelected
	}
	if _, found := m.rels[relName]; !found {
		return nil, errorutil.ErrorDBSysRelationNotExisted
	}
	tableHeaderList := []string{"idxNo", "idxName", "column"}
	offsetList := []int{0, 8, 32}
	sizeList := []int{8, 24, 24}
	typeList := []types.ValueType{types.INT, types.VARCHAR, types.VARCHAR}
	colWidMap := map[string]int{"idxNo": 5, "idxName": 7, "column": 6}

	// compute the appropriate length for each component after necessary scanning of each item
	fh, err := m.relManager.OpenFile(getTableIdxMetaFileName(relName))
	if err != nil {
		return nil, err
	}
	defer m.relManager.CloseFile(getTableIdxMetaFileName(relName))

	recCnt := 0
	for _, rec := range fh.GetRecList() {
		recCnt += 1
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
		NullList:        nil,
		ColWidMap:       colWidMap,
	}, nil
}

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
