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
	if m.DbSelected() {
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
	if !m.DbSelected() {
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

func PrintEmptySet() {
	println("+---------------+")
	println("|     empty     |")
	println("+---------------+")
}

func (m *Manager) GetTableShowingInfo(relName string, showingMeta bool) (*TablePrintInfo, error) {
	if !m.DbSelected() {
		return nil, errorutil.ErrorDbSysDbNotSelected
	}
	if _, found := m.rels[relName]; !found {
		return nil, errorutil.ErrorDbSysTableNotExisted
	}

	// get attr then header
	fileHandle, err := m.relManager.OpenFile(getTableMetaFileName(relName))
	if err != nil {
		log.V(log.DbSysLevel).Error(err)
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
		}
	} else {
		tableHeaderList = TableDescribeColumn
		offsetList = []int{offsetAttrName, offsetAttrType, offsetAttrSize, offsetAttrOffset, offsetIndexNo, offsetNull, offsetPrimary, offsetFK, offsetDefault}
		sizeList = []int{types.MaxNameSize, 8, 8, 8, 8, 1, 1, 1, int(unsafe.Sizeof(parser.Value{}))}
		typeList = []int{types.STRING, types.INT, types.INT, types.INT, types.INT, types.BOOL, types.BOOL, types.BOOL, types.NO_ATTR} // since the default value type is different, just assigned a NO_ATTR
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
		fileHandle, err := m.relManager.OpenFile(relName)
		if err != nil {
			log.V(log.DbSysLevel).Error(err)
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
	ColWidMap       map[string]int    // column width is computed from every item in the table
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

	// print content by iterating each row & column

	for i, rec := range recordList {
		str := ""
		for j := 0; j < len(info.TableHeaderList); j++ {
			// different print case are handled here
			if info.ShowingMeta {
				byteSlice := rec.Data[info.OffsetList[j] : info.OffsetList[j]+info.SizeList[j]]
				switch {
				case info.TypeList[j] == types.NO_ATTR:
					// this is the Default column, which has no attribute
					str = data2StringByTypes(byteSlice, info.VariantTypeList[i])
				case info.TableHeaderList[j] == "Type":
					str = types.ValueTypeStringMap[*(*int)(types.ByteSliceToPointer(byteSlice))]
				default:
					str = data2StringByTypes(byteSlice, info.TypeList[j])
				}

			} else {
				byteSlice := rec.Data[info.OffsetList[j] : info.OffsetList[j]+info.SizeList[j]]
				if info.NullList[j] {
					if rec.Data[info.OffsetList[j]+info.SizeList[j]] == 1 {
						// a single column always takes up (size + 1 bit)
						str = "NULL"
					} else {
						str = data2StringByTypes(byteSlice, info.TypeList[j])
					}
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

// DescribeTable is implemented since GetRecordShould be wrapped up
// since GetTableShowingInfo & PrintTableByInfo provide a unified access for table printing
func (m *Manager) DescribeTable(relName string) error {
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