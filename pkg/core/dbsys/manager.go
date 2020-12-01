package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

const DbMetaName = "db.meta"

func getTableMetaFileName(table string) string {
	return table + ".table-meta"
}

func getTableConstraintFileName(table string) string {
	return table + ".constraint-meta"
}

type Manager struct {
	relManager *record.Manager
	//idxManager
	rels       map[string]*record.FileHandle
	dbMeta     *record.FileHandle
	dbSelected string
}

var instance *Manager
var once sync.Once

func GetInstance() *Manager {
	once.Do(func() {
		log.V(log.DbSysLevel).Info("DbSys Manager starts to initialize.")
		defer log.V(log.DbSysLevel).Info("DbSys Manager has been initialized.")
		instance = &Manager{
			relManager: record.GetInstance(),
			rels:       nil,
			dbSelected: "",
		}
	})
	return instance
}

func (m *Manager) DbSelected() bool {
	if len(m.dbSelected) == 0 {
		return false
	} else {
		return true
	}
}

func (m *Manager) CreateDb(dbName string) error {
	if err := os.Mkdir(dbName, syscall.S_IRWXU); err != nil {
		log.V(log.DbSysLevel).Error(err)
		return errorutil.ErrorDbSysCreateDbFails
	}
	if err := os.Chdir(dbName); err != nil {
		log.V(log.DbSysLevel).Error(err)
		panic(0)
	}
	if err := m.relManager.CreateFile(DbMetaName, RelInfoSize); err != nil {
		return err
	}
	_ = os.Chdir("..")
	m.rels[dbName] = nil
	return nil
}

func (m *Manager) DropDb(dbName string) error {
	if m.DbSelected() {
		_ = os.Chdir("..")
	}
	if err := os.RemoveAll(dbName); err != nil {
		log.V(log.DbSysLevel).Error(err)
		return errorutil.ErrorDbSysDropDbFails

	}
	delete(m.rels, dbName)
	return nil
}

func (m *Manager) OpenDb(dbName string) error {
	if m.DbSelected() {
		_ = os.Chdir("..")
	}
	if err := os.Chdir(dbName); err != nil {
		log.V(log.DbSysLevel).Error(err)
		return errorutil.ErrorDbSysOpenDbFails
	}
	m.dbSelected = dbName
	m.dbMeta, _ = m.relManager.OpenFile(DbMetaName)
	m.rels = make(map[string]*record.FileHandle)
	recList := m.dbMeta.GetRecList()
	for i := 0; i < len(recList); i++ {
		relname := record.RecordData2TrimmedStringWithOffset(recList[i].Data, 0, types.MaxNameSize)
		m.rels[relname] = nil
	}
	return nil
}

func (m *Manager) CloseDb(dbName string) error {
	if m.DbSelected() {
		_ = os.Chdir("..")
		m.dbMeta = nil
		m.rels = nil
	}
	if err := os.Chdir(dbName); err != nil {
		log.V(log.DbSysLevel).Error(err)
		return errorutil.ErrorDbSysOpenDbFails
	}
	return nil
}

func (m *Manager) CreateTable(relName string, attrList []parser.AttrInfo, constraintList []ConstraintInfo) error {
	if !m.DbSelected() {
		return errorutil.ErrorDbSysDbNotSelected
	}
	if _, found := m.rels[relName]; found {
		return errorutil.ErrorDbSysTableExisted
	}
	if len(relName) >= types.MaxNameSize {
		return errorutil.ErrorDbSysMaxNameExceeded
	}
	// judge primary
	hasPrimary := false
	for i := 0; i < len(attrList); i++ {
		hasPrimary = hasPrimary || attrList[i].IsPrimary
	}
	if hasPrimary {
		if len(attrList) >= types.MaxAttrNums {
			return errorutil.ErrorDbSysMaxAttrExceeded
		}
	} else {
		if len(attrList) >= types.MaxAttrNums-1 {
			return errorutil.ErrorDbSysMaxAttrExceeded
		}
	}

	curSize := 0

	// generating auto-increasing id
	if !hasPrimary {
		curSize += 8 + 1  // sizeof a int and additional null flag bit (useless for this auto-generated one)
		hasPrimary = true // auto-increasing
		idAutoAttr := parser.AttrInfo{
			AttrName:      strTo24ByteArray("_id_"),
			RelName:       strTo24ByteArray(relName),
			AttrSize:      8,
			AttrOffset:    0,
			AttrType:      types.INT,
			IndexNo:       0,           // TODO
			ConstraintRID: types.RID{}, // TODO
			NullAllowed:   false,
			IsPrimary:     true,
			AutoIncrement: true,
		}
		attrList = append([]parser.AttrInfo{idAutoAttr}, attrList...)
	}

	_ = m.relManager.CreateFile(getTableMetaFileName(relName), AttrInfoSize)
	tableMetaFile, _ := m.relManager.OpenFile(getTableMetaFileName(relName))

	// todo check name duplicated?
	for i := 0; i < len(attrList); i++ {
		attrList[i].AttrOffset += curSize // used 4 bytes to mark if it's null
		_, _ = tableMetaFile.InsertRec(types.PointerToByteSlice(unsafe.Pointer(&attrList[i]), AttrInfoSize))
		curSize += attrList[i].AttrSize + 1 // null flag bit
	}

	_, _ = m.dbMeta.InsertRec(types.PointerToByteSlice(unsafe.Pointer(
		&RelInfo{
			relName:    strTo24ByteArray(relName),
			recordSize: curSize,
			idxCount:   0,
			attrCount:  len(attrList),
			consCount:  len(constraintList),
		}), RelInfoSize))

	if err := m.relManager.CreateFile(relName, curSize); err != nil {
		return err
	}
	// ToDo add constraint
	if err := m.relManager.CreateFile(getTableConstraintFileName(relName), ConstraintInfoSize); err != nil {
		return err
	}

	_ = tableMetaFile.Close()
	m.rels[relName] = nil
	return nil
}

func (m *Manager) DropTable(relName string) error {
	if !m.DbSelected() {
		return errorutil.ErrorDbSysDbNotSelected
	}
	_ = os.Remove(getTableMetaFileName(relName))
	_ = os.Remove(getTableConstraintFileName(relName))

	recList, _ := m.dbMeta.GetFilteredRecList(record.FilterCond{
		AttrSize:   types.MaxNameSize,
		AttrOffset: 0,
		CompOp:     types.OpCompEQ,
		Value:      parser.NewValueFromStr(relName),
	})
	// ToDo add constraint when deleting?
	return m.dbMeta.DeleteRec(recList[0].Rid)
}

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
	println("| "+"Database"+strings.Repeat(" ", maxLen-len("Database")), " |")
	for _, f := range names {
		println("| "+f+strings.Repeat(" ", maxLen-len(f)), " |")
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
		println("| "+"Database"+strings.Repeat(" ", maxLen-len("Database")), " |")
		for rel := range m.rels {
			println("| "+rel+strings.Repeat(" ", maxLen-len(rel)), " |")
		}
		println("+" + strings.Repeat("-", maxLen+2) + "+")

	}
}

func PrintEmptySet() {
	println("+---------------+")
	println("|     empty     |")
	println("+---------------+")
}

func (m *Manager) DescribeTables(relName string) error {
	if !m.DbSelected() {
		return errorutil.ErrorDbSysDbNotSelected
	}
	if _, found := m.rels[relName]; found {
		fileHandle, _ := m.relManager.OpenFile(getTableMetaFileName(relName))
		recordList := fileHandle.GetRecList()

		colMaxLength := make(map[string]int)
		colMaxLength["Field"] = 5
		colMaxLength["Type"] = 4
		colMaxLength["Size"] = 4
		colMaxLength["Offset"] = 6
		colMaxLength["IndexNo"] = 7
		colMaxLength["Null"] = 5
		colMaxLength["IsPrimary"] = 9
		colMaxLength["AutoIncrement"] = 13
		colMaxLength["Default"] = 10

		colList := []string{"Field", "Type", "Size", "Offset", "IndexNo", "Null", "IsPrimary", "AutoIncrement", "Default"}

		for _, rec := range recordList {
			attr := (*parser.AttrInfo)(types.ByteSliceToPointer(rec.Data))
			if len(record.RecordData2TrimmedStringWithOffset(attr.RelName[:], 0)) > colMaxLength["Field"] {
				colMaxLength["Field"] = len(record.RecordData2TrimmedStringWithOffset(attr.RelName[:], 0))
			}
			if len(types.ValueTypeStringMap[attr.AttrType]) > colMaxLength["Type"] {
				colMaxLength["Type"] = len(types.ValueTypeStringMap[attr.AttrType])
			}
		}

		for i := 0; i < len(colList); i++ {
			print("+" + strings.Repeat("-", colMaxLength[colList[i]]+2))
		}
		println("+")
		for i := 0; i < len(colList); i++ {
			print("| " + colList[i] + strings.Repeat("-", colMaxLength[colList[i]]-len(colList[i])-1) + " ")
		}
		println("|")
		for i := 0; i < len(colList); i++ {
			print("+" + strings.Repeat("-", colMaxLength[colList[i]]+2))
		}
		println("+")
		for _, rec := range recordList {
			attr := (*parser.AttrInfo)(types.ByteSliceToPointer(rec.Data))

			attrName := record.RecordData2TrimmedStringWithOffset(attr.RelName[:], 0)
			print("| " + attrName + strings.Repeat(" ", colMaxLength["Field"]-len(attrName)+1))

			attrType := types.ValueTypeStringMap[attr.AttrType]
			print("| " + attrType + strings.Repeat(" ", colMaxLength["Type"]-len(attrType)+1))

			attrSize := string(rune(attr.AttrSize))
			print("| " + attrSize + strings.Repeat(" ", colMaxLength["Offset"]-len(attrSize)+1))

			attrOffset := string(rune(attr.AttrOffset))
			print("| " + attrSize + strings.Repeat(" ", colMaxLength["Size"]-len(attrOffset)+1))

			indexNo := string(rune(attr.IndexNo))
			print("| " + attrSize + strings.Repeat(" ", colMaxLength["Size"]-len(indexNo)+1))

			if attr.IsPrimary {
				print("| " + "true" + strings.Repeat(" ", colMaxLength["Primary"]-4+1))
			} else {
				print("| " + "false" + strings.Repeat(" ", colMaxLength["Primary"]-5+1))
			}

			if attr.AutoIncrement {
				print("| " + "true" + strings.Repeat(" ", colMaxLength["AutoIncrement"]-4+1))
			} else {
				print("| " + "false" + strings.Repeat(" ", colMaxLength["AutoIncrement"]-5+1))
			}

			defaultStr := ""
			switch attr.Default.ValueType {
			case types.INT:
				defaultStr = string(rune(attr.Default.ToInt64()))
			case types.BOOL:
			}
			print("| " + defaultStr + strings.Repeat(" ", colMaxLength["AutoIncrement"]-len(defaultStr)+1))
			println("|")
		}

		for i := 0; i < len(colList); i++ {
			print("+" + strings.Repeat("-", colMaxLength[colList[i]]+2))
		}
		println("+")

	} else {
		return errorutil.ErrorDbSysTableNotExisted
	}
	panic(0) // unreachable
}
