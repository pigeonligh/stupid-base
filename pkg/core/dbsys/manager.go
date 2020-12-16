package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/index"
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"os"
	"sync"
	"unsafe"
)

const DbMetaName = "db.meta"
const PrimaryKeyIndexName = "PK_INDEX"

func getTableMetaFileName(table string) string {
	return table + ".table-meta"
}

func getTableIdxFileName(table string) string {
	return table + ".table-index"
}

func getTableDataFileName(table string) string {
	return table + ".table-data"
}

func getTableConstraintFileName(table string) string {
	return table + ".constraint-meta"
}

type Manager struct {
	relManager *record.Manager
	idxManager *index.Manager
	rels       map[string]AttrInfoMap
	dbMeta     *record.FileHandle
	dbSelected string
	dbFK       *record.FileHandle // maintain a database's overall foreign constraint
	dbPK       *record.FileHandle // maintain a database's overall primary constraint
}

var instance *Manager
var once sync.Once

func GetInstance() *Manager {
	once.Do(func() {
		log.V(log.DbSysLevel).Info("DbSys Manager starts to initialize.")
		defer log.V(log.DbSysLevel).Info("DbSys Manager has been initialized.")
		instance = &Manager{
			relManager: record.GetInstance(),
			idxManager: index.GetInstance(),
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
	if err := os.Mkdir(dbName, os.ModePerm); err != nil {
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
	return nil
}

func (m *Manager) DropDb(dbName string) error {
	if m.DbSelected() {
		_ = os.Chdir("..")
		_ = m.relManager.CloseFile(m.dbMeta.Filename)
		m.rels = nil
		m.dbMeta = nil
		m.dbFK = nil
		m.dbPK = nil
		m.dbSelected = ""
	}
	if err := os.RemoveAll(dbName); err != nil {
		log.V(log.DbSysLevel).Error(err)
		return errorutil.ErrorDbSysDropDbFails
	}
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
	m.rels = make(map[string]AttrInfoMap)
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
		if err := m.relManager.CloseFile(m.dbMeta.Filename); err != nil {
			return err
		}
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

	attrNameMap := make(map[string]bool)
	totalSize := 0
	for i := 0; i < len(attrList); i++ {
		// check if it's too big a record
		totalSize += attrList[i].AttrSize + 1
		// check name duplicated by the way
		if _, found := attrNameMap[ByteArray24tostr(attrList[i].AttrName)]; found {
			return errorutil.ErrorDbSysCreatetTableWithDupAttr
		} else {
			attrNameMap[ByteArray24tostr(attrList[i].AttrName)] = true
		}
	}
	if totalSize >= types.PageSize-int(unsafe.Sizeof(types.RecordHeaderPage{})) {
		return errorutil.ErrorDbSysBigRecordNotSupported
	}
	if len(attrList) >= types.MaxAttrNums {
		return errorutil.ErrorDbSysMaxAttrExceeded
	}

	// start to create file after all the checking above
	curSize := 0

	_ = m.relManager.CreateFile(getTableMetaFileName(relName), AttrInfoSize)

	tableMetaFile, _ := m.relManager.OpenFile(getTableMetaFileName(relName))

	defer func() {
		if err1 := m.relManager.CloseFile(tableMetaFile.Filename); err1 != nil {
			log.V(log.DbSysLevel).Error(err1)
			panic(0)
		}
		m.rels[relName] = nil
	}()

	// add record to tableMetaFile
	for i := 0; i < len(attrList); i++ {
		attrList[i].AttrOffset += curSize // used 4 bytes to mark if it's null
		_, err := tableMetaFile.InsertRec(types.PointerToByteSlice(unsafe.Pointer(&attrList[i]), AttrInfoSize))
		log.Debugf("%v %v %v", record.RecordData2TrimmedStringWithOffset(attrList[i].AttrName[:], 0), attrList[i].AttrSize, attrList[i].AttrOffset)
		if err != nil {
			panic(0)
		}
		curSize += attrList[i].AttrSize + 1 // additional null flag bit
	}

	// insert relation to dbMetaFile
	_, _ = m.dbMeta.InsertRec(types.PointerToByteSlice(unsafe.Pointer(
		&RelInfo{
			relName:      strTo24ByteArray(relName),
			recordSize:   curSize,
			attrCount:    len(attrList),
			nextIndexNo:  -1,
			indexCount:   0,
			primaryCount: 0,
			foreignCount: 0,
		}), RelInfoSize))

	// create table record file
	if err := m.relManager.CreateFile(getTableDataFileName(relName), curSize); err != nil {
		return err
	}

	// create table index file
	if err := m.relManager.CreateFile(getTableIdxFileName(relName), int(unsafe.Sizeof(IndexInfo{}))); err != nil {
		return err
	}
	return nil
}

func (m *Manager) DropTable(relName string) error {
	if !m.DbSelected() {
		return errorutil.ErrorDbSysDbNotSelected
	}
	if _, found := m.rels[relName]; !found {
		return errorutil.ErrorDbSysTableNotExisted
	}

	_ = os.Remove(getTableMetaFileName(relName))
	_ = os.Remove(getTableIdxFileName(relName))
	_ = os.Remove(getTableConstraintFileName(relName))
	_ = os.Remove(getTableDataFileName(relName))

	recList, _ := m.dbMeta.GetFilteredRecList(record.FilterCond{
		AttrSize:   types.MaxNameSize,
		AttrOffset: 0,
		CompOp:     types.OpCompEQ,
		Value:      parser.NewValueFromStr(relName),
	})
	// ToDo add constraint when deleting
	return m.dbMeta.DeleteRec(recList[0].Rid)
}
