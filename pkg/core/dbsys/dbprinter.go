package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"io/ioutil"
	"strings"
	"unsafe"
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

func (m *Manager) ShowTablesWithDetails() error {
	if !m.DBSelected() {
		PrintEmptySet()
	} else {
		tableShowingDescribedInfo, err := m.getRelMetaPrintInfo()
		if err != nil {
			return err
		}
		m.PrintTableByInfo(m.dbMeta.GetRecList(), tableShowingDescribedInfo)
	}
	return nil
}

func PrintEmptySet() {
	println("+---------------+")
	println("|     empty     |")
	println("+---------------+")
}

func (m *Manager) GetTableShowingInfo(relName string, showingMeta bool) (*TablePrintInfo, error) {
	if !m.DBSelected() {
		return nil, errorutil.ErrorDBSysDBNotSelected
	}
	if _, found := m.rels[relName]; !found {
		return nil, errorutil.ErrorDBSysRelationNotExisted
	}

	// get attrName then header
	fileHandle, err := m.relManager.OpenFile(getTableMetaFileName(relName))
	if err != nil {
		log.V(log.DBSysLevel).Error(err)
		return nil, err
	}
	defer m.relManager.CloseFile(fileHandle.Filename)

	tableHeaderList := make([]string, 0, types.MaxAttrNums)
	offsetList := make([]int, 0, types.MaxAttrNums)
	sizeList := make([]int, 0, types.MaxAttrNums)
	typeList := make([]types.ValueType, 0, types.MaxAttrNums)
	variantTypeList := make([]types.ValueType, 0, types.MaxAttrNums)
	nullList := make([]bool, 0, types.MaxAttrNums)

	var rawAttrList = fileHandle.GetRecList()

	if !showingMeta {
		for _, rawAttr := range rawAttrList {
			attr := (*parser.AttrInfo)(types.ByteSliceToPointer(rawAttr.Data))
			tableHeaderList = append(tableHeaderList, record.RecordData2TrimmedStringWithOffset(attr.AttrName[:], 0))
			offsetList = append(offsetList, attr.AttrOffset)
			sizeList = append(sizeList, attr.AttrSize)
			typeList = append(typeList, attr.AttrType)
			nullList = append(nullList, false)
		}
	} else {
		tableHeaderList = TableDescribeColumn
		offsetList = []int{offsetAttrName, offsetAttrType, offsetAttrSize, offsetAttrOffset, offsetIndexNo, offsetNull, offsetPrimary, offsetFK, offsetDefault}
		sizeList = []int{types.MaxNameSize, 8, 8, 8, 8, 1, 1, 1, int(unsafe.Sizeof(types.Value{}))}
		typeList = []int{types.VARCHAR, types.INT, types.INT, types.INT, types.INT, types.BOOL, types.BOOL, types.BOOL, types.NO_ATTR} // since the default value type is different, just assigned a NO_ATTR
		for _, rawAttr := range rawAttrList {
			rawTypeData := *(*types.ValueType)(types.ByteSliceToPointer(rawAttr.Data[offsetAttrType : offsetAttrType+8]))
			variantTypeList = append(variantTypeList, rawTypeData)
			nullData := *(*bool)(types.ByteSliceToPointer(rawAttr.Data[offsetNull : offsetNull+1]))
			nullList = append(nullList, nullData)
		}
	}

	// compute the base length info for each item
	colWidMap := make(map[string]int)
	for _, head := range tableHeaderList {
		colWidMap[head] = len(head)
	}
	if showingMeta {
		colWidMap["Type"] = 7 // since here type always converted to string
	}

	if !showingMeta {
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
		rawAttrList = fileHandle.GetRecList()
	}

	// compute the appropriate length for each component after necessary scanning of each item
	for _, rec := range rawAttrList {
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
		VariantTypeList: variantTypeList,
		ShowingMeta:     showingMeta,
	}, nil
}

type TablePrintInfo struct {
	TableHeaderList []string
	OffsetList      []int
	SizeList        []int
	TypeList        []types.ValueType
	NullList        []bool
	ColWidMap       map[string]int    // col width is computed from every item in the table
	VariantTypeList []types.ValueType // since table meta "Default" can be variant-type, so this field is needed
	ShowingMeta     bool
}

func (m *Manager) PrintTableByInfo(recordList []*record.Record, info *TablePrintInfo) {
	if info.ShowingMeta && len(info.VariantTypeList) != len(recordList) {
		log.Error("variant type list length doesn't match record length")
	}
	if !info.ShowingMeta && len(info.NullList) != len(info.TableHeaderList) {
		log.Error("length is not matched")
	}

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

	for i, rec := range recordList {
		str := ""
		for j := 0; j < len(info.TableHeaderList); j++ {
			// different print case are handled here
			if info.ShowingMeta {
				byteSlice := rec.Data[info.OffsetList[j] : info.OffsetList[j]+info.SizeList[j]]
				switch {
				case info.TypeList[j] == types.NO_ATTR:
					// this is the Default col, which has no attribute
					str = data2StringByTypes(byteSlice, info.VariantTypeList[i])
				case info.TableHeaderList[j] == "Type":
					// handle special cases, convert from types.ValueType to string
					str = types.ValueTypeStringMap[*(*int)(types.ByteSliceToPointer(byteSlice))]
				default:
					str = data2StringByTypes(byteSlice, info.TypeList[j])
				}

			} else {
				byteSlice := rec.Data[info.OffsetList[j] : info.OffsetList[j]+info.SizeList[j]]
				if info.NullList[j] {
					if rec.Data[info.OffsetList[j]+info.SizeList[j]] == 1 {
						// a single col always takes up (size + 1 bit)
						str = "NULL"
					} else {
						str = data2StringByTypes(byteSlice, info.TypeList[j])
					}
				}else {
					str = data2StringByTypes(byteSlice, info.TypeList[j])
				}
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
func (m *Manager) PrintTableMeta(relName string) error {
	tableShowingDescribedInfo, err := m.GetTableShowingInfo(relName, true)
	if err != nil {
		return err
	}
	// these must not have error since file get opened before
	fileHandle, _ := m.relManager.OpenFile(getTableMetaFileName(relName))
	defer m.relManager.CloseFile(fileHandle.Filename)
	recList := fileHandle.GetRecList()
	m.PrintTableByInfo(recList, tableShowingDescribedInfo)
	return nil
}

func (m *Manager) PrintTableData(relName string) error {
	tableShowingDescribedInfo, err := m.GetTableShowingInfo(relName, false)
	log.V(log.DBSysLevel).Debug(tableShowingDescribedInfo)
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

func (m *Manager) PrintTableByTmpColumns(table TemporalTable) {
	printInfo := &TablePrintInfo{
		TableHeaderList: make([]string, 0),
		OffsetList:      make([]int, 0),
		SizeList:        make([]int, 0),
		TypeList:        make([]int, 0),
		NullList:        make([]bool, 0),
		ColWidMap:       make(map[string]int),
		ShowingMeta:     false,
	}
	// construct a record list
	recordNums := len(table[0].valueList)
	recordSize := 0
	for _, col := range table {
		if len(col.valueList) != recordNums {
			panic(0)
		}
		printInfo.ColWidMap[col.attrName] = len(col.attrName)
		printInfo.TableHeaderList = append(printInfo.TableHeaderList, col.attrName)
		printInfo.OffsetList = append(printInfo.OffsetList, recordSize)
		printInfo.SizeList = append(printInfo.SizeList, col.attrSize)
		printInfo.TypeList = append(printInfo.TypeList, col.attrType)
		printInfo.NullList = append(printInfo.NullList, col.nullAllowed)

		recordSize += col.attrSize + 1
	}
	recList := make([]*record.Record, 0)

	for i := 0; i < recordNums; i++ {
		rec := record.Record{
			Rid:  types.RID{},
			Data: make([]byte, recordSize),
		}
		for j := 0; j < len(table); j++ {
			copy(rec.Data[printInfo.OffsetList[j]:printInfo.OffsetList[j]+printInfo.SizeList[j]], table[i].valueList[i].Value[0:printInfo.SizeList[j]])
			if len(table[i].valueList[i].Format2String()) > printInfo.ColWidMap[table[i].attrName] {
				printInfo.ColWidMap[table[i].attrName] = len(table[i].valueList[i].Format2String())
			}
		}
		recList = append(recList, &rec)
	}
	m.PrintTableByInfo(recList, printInfo)
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
		VariantTypeList: make([]types.ValueType, recCnt), // this field will be of no use when not print table meta default value for each record
		ShowingMeta:     true,
	}, nil
}

//type RelInfo struct {
//	relName      [types.MaxNameSize]byte
//	recordSize   int
//	attrCount    int
//	nextIndexNo  int
//	indexCount   int // index constraint count
//	primaryCount int // primary constraint count
//	foreignCount int // foreign constraint count
//}
func (m *Manager) getRelMetaPrintInfo() (*TablePrintInfo, error) {
	if !m.DBSelected() {
		return nil, errorutil.ErrorDBSysDBNotSelected
	}
	tableHeaderList := []string{"relName", "recordSize", "attrCount", "nextIndexNo", "indexCount", "primaryCount", "foreignCount"}
	offsetList := []int{0, 24, 32, 40, 48, 56, 64}
	sizeList := []int{24, 8, 8, 8, 8, 8, 8}
	typeList := []types.ValueType{types.VARCHAR, types.INT, types.INT, types.INT, types.INT, types.INT, types.INT}
	colWidMap := make(map[string]int)
	for _, name := range tableHeaderList {
		colWidMap[name] = len(name)
	}

	recCnt := 0
	for _, rec := range m.dbMeta.GetRecList() {
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
		VariantTypeList: make([]types.ValueType, recCnt), // this field will be of no use when not print table meta default value for each record
		ShowingMeta:     true,
	}, nil
}
