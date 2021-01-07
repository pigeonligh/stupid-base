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

const DBMetaName = "db.meta"

func getTableMetaFileName(table string) string {
	return table + ".table-meta"
}

func getTableDataFileName(table string) string {
	return table + ".table-data"
}

const PrimaryKeyIdxName = "PRIMARY"

// table-suffix.index
func getTableIdxDataFileName(table, idxName string) string {
	return table + "-" + idxName + ".index"
}

func getTableIdxMetaFileName(table string) string {
	return table + ".table-index"
}

func getTableConstraintFileName(table string) string {
	return table + ".constraint-meta"
}

type Manager struct {
	relManager *record.Manager
	idxManager *index.Manager
	rels       map[string]AttrInfoList
	dbSelected string
	//dbMeta     RelInfoMap
	//dbFK       *record.FileHandle // maintain a database's overall foreign constraint
}

var instance *Manager
var once sync.Once

func GetInstance() *Manager {
	once.Do(func() {
		log.V(log.DBSysLevel).Info("DbSys Manager starts to initialize.")
		defer log.V(log.DBSysLevel).Info("DbSys Manager has been initialized.")
		instance = &Manager{
			relManager: record.GetInstance(),
			idxManager: index.GetInstance(),
			rels:       nil,
			dbSelected: "",
		}
	})
	return instance
}

func (m *Manager) DBSelected() bool {
	if len(m.dbSelected) == 0 {
		return false
	} else {
		return true
	}
}

func (m *Manager) CreateDB(dbName string) error {
	if err := os.Mkdir(dbName, os.ModePerm); err != nil {
		log.V(log.DBSysLevel).Error(err)
		return errorutil.ErrorDBSysCreateDBFails
	}
	if err := os.Chdir(dbName); err != nil {
		log.V(log.DBSysLevel).Error(err)
		panic(0)
	}
	m.SetDBRelInfoMap(RelInfoMap{})
	m.SetFkInfoMap(FkConstraintMap{})
	_ = os.Chdir("..")
	return nil
}

func (m *Manager) DropDB(dbName string) error {
	if m.DBSelected() {
		_ = os.Chdir("..")
		m.rels = nil
		m.dbSelected = ""
	}
	if err := os.RemoveAll(dbName); err != nil {
		log.V(log.DBSysLevel).Error(err)
		return errorutil.ErrorDBSysDropDBFails
	}
	return nil
}

func (m *Manager) OpenDB(dbName string) error {
	if m.DBSelected() {
		_ = os.Chdir("..")
	}
	if err := os.Chdir(dbName); err != nil {
		log.V(log.DBSysLevel).Error(err)
		return errorutil.ErrorDBSysOpenDBFails
	}
	m.dbSelected = dbName
	m.rels = make(map[string]AttrInfoList)
	relInfoMap := m.GetDBRelInfoMap()
	for key := range relInfoMap {
		m.rels[key] = nil
	}
	return nil
}

func (m *Manager) CloseDB(dbName string) error {
	if m.DBSelected() {
		_ = os.Chdir("..")
		m.rels = nil
		m.dbSelected = ""
	}
	if err := os.Chdir(dbName); err != nil {
		log.V(log.DBSysLevel).Error(err)
		return errorutil.ErrorDBSysOpenDBFails
	}
	return nil
}

func (m *Manager) CreateTable(relName string, attrList []parser.AttrInfo) error {
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	if _, found := m.rels[relName]; found {
		return errorutil.ErrorDBSysTableExisted
	}
	if len(relName) >= types.MaxNameSize {
		return errorutil.ErrorDBSysMaxNameExceeded
	}

	attrInfoList := make(AttrInfoList, 0)
	attrInfoMap := make(AttrInfoMap)
	pkList := make([]string, 0)
	totalSize := 0
	curSize := 0
	for i := 0; i < len(attrList); i++ {
		totalSize += attrList[i].AttrSize + 1
		attrList[i].AttrOffset += curSize   // used 4 bytes to mark if it's null
		curSize += attrList[i].AttrSize + 1 // additional null flag bit
		if _, found := attrInfoMap[attrList[i].AttrName]; found {
			return errorutil.ErrorDBSysCreateTableWithDupAttr
		}
		if attrList[i].IsPrimary {
			pkList = append(pkList, attrList[i].AttrName)
		}
		attrInfoMap[attrList[i].AttrName] = attrList[i]
		attrInfoList = append(attrInfoList, attrList[i])
	}
	if totalSize >= types.PageSize-int(unsafe.Sizeof(types.RecordHeaderPage{})) {
		return errorutil.ErrorDBSysBigRecordNotSupported
	}
	if len(attrList) >= types.MaxAttrNums {
		return errorutil.ErrorDBSysMaxAttrExceeded
	}

	// create table record file
	if err := m.relManager.CreateFile(getTableDataFileName(relName), totalSize); err != nil {
		return err
	}
	m.SetAttrInfoList(relName, attrInfoList)

	m.SetRelInfo(RelInfo{
		RelName:      relName,
		RecordSize:   totalSize,
		AttrCount:    len(attrList),
		IndexCount:   0,
		PrimaryCount: len(pkList),
		ForeignCount: 0,
	})

	if len(pkList) != 0 {
		if err := m.AddPrimaryKey(relName, pkList); err != nil {
			log.V(log.DBSysLevel).Error(err)
			panic(0)
		}
	}
	return nil
}

func (m *Manager) DropTable(relName string) error {
	if !m.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	if _, found := m.rels[relName]; !found {
		return errorutil.ErrorDBSysRelationNotExisted
	}

	attrInfoCollection := m.GetAttrInfoCollection(relName)

	for idxName := range attrInfoCollection.IdxMap {
		_ = os.Remove(getTableIdxDataFileName(relName, idxName))
	}
	_ = os.Remove(getTableMetaFileName(relName))
	//_ = os.Remove(getTableIdxMetaFileName(relName))
	_ = os.Remove(getTableConstraintFileName(relName))
	_ = os.Remove(getTableDataFileName(relName))

	relInfo := m.GetDBRelInfoMap()
	delete(relInfo, relName)

	return nil
}
