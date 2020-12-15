package dbsys

import (
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

func getTableMetaFileName(table string) string {
	return table + ".table-meta"
}

func getTableDataFileName(table string) string {
	return table + ".table-data"
}

func getTableConstraintFileName(table string) string {
	return table + ".constraint-meta"
}

type AttrInfoMap map[string]*parser.AttrInfo

type Manager struct {
	relManager *record.Manager
	//idxManager
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

	// judge primary
	primaryKeyCnt := 0
	for i := 0; i < len(attrList); i++ {
		if attrList[i].IsPrimary {
			primaryKeyCnt += 1
		}
	}

	if len(attrList) >= types.MaxAttrNums {
		return errorutil.ErrorDbSysMaxAttrExceeded
	}
	if primaryKeyCnt >= 2 {
		return errorutil.ErrorDbSysPrimaryKeyCntExceed
	}

	curSize := 0

	_ = m.relManager.CreateFile(getTableMetaFileName(relName), AttrInfoSize)
	tableMetaFile, _ := m.relManager.OpenFile(getTableMetaFileName(relName))
	defer func() {
		if err := m.relManager.CloseFile(tableMetaFile.Filename); err != nil {
			log.V(log.DbSysLevel).Error(err)
		}
		m.rels[relName] = nil
	}()

	// add record to tableMetaFile todo check name duplicated?
	for i := 0; i < len(attrList); i++ {
		attrList[i].AttrOffset += curSize // used 4 bytes to mark if it's null
		_, err := tableMetaFile.InsertRec(types.PointerToByteSlice(unsafe.Pointer(&attrList[i]), AttrInfoSize))
		log.Debugf("%v %v %v", record.RecordData2TrimmedStringWithOffset(attrList[i].AttrName[:], 0), attrList[i].AttrSize, attrList[i].AttrOffset)
		if err != nil {
			return err
		}
		curSize += attrList[i].AttrSize + 1 // additional null flag bit
	}

	// insert relation to dbMetaFile
	_, _ = m.dbMeta.InsertRec(types.PointerToByteSlice(unsafe.Pointer(
		&RelInfo{
			relName:    strTo24ByteArray(relName),
			recordSize: curSize,
			idxCount:   0,
			attrCount:  len(attrList),
		}), RelInfoSize))

	// create table record file
	if err := m.relManager.CreateFile(getTableDataFileName(relName), curSize); err != nil {
		return err
	}

	// create constraint file todo
	//if err := m.relManager.CreateFile(getTableConstraintFileName(relName), ConstraintInfoSize); err != nil {
	//	return err
	//}
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
	// ToDo add constraint when deleting
	return m.dbMeta.DeleteRec(recList[0].Rid)
}

// GetAttrInfoMap used for create fast map to accelerate get attribute
func (m *Manager) GetAttrInfoMap(relName string) AttrInfoMap {

	// m.rels must found, as it has been guaranteed in parent calls
	if infoMap, _ := m.rels[relName]; infoMap != nil {
		return infoMap
	} else {
		infoMap = m.buildAttrInfoMap(relName)
		m.rels[relName] = infoMap
		return infoMap
	}
}

func (m *Manager) buildAttrInfoMap(relName string) AttrInfoMap {
	fileHandle, err := m.relManager.OpenFile(getTableMetaFileName(relName))
	defer m.relManager.CloseFile(fileHandle.Filename)
	if err != nil {
		// once build attrName info map is recalled, it must be existed
		panic(0)
	}
	attrInfoMap := make(map[string]*parser.AttrInfo, 0)
	var rawAttrList = fileHandle.GetRecList()
	for _, rawAttr := range rawAttrList {
		attr := (*parser.AttrInfo)(types.ByteSliceToPointer(rawAttr.Data))
		attrInfoMap[ByteArray24tostr(attr.AttrName)] = attr
	}
	return attrInfoMap
}

// maybe it can be used for select & join
func (m *Manager) GetTemporalTableByAttrs(relName string, attrNameList []string, condList []record.FilterCond) TemporalTable {
	retTempTable := make(TemporalTable, 0)

	attrInfoMap := m.GetAttrInfoMap(relName)

	datafile, err := m.relManager.OpenFile(getTableDataFileName(relName))
	if err != nil {
		log.V(log.DbSysLevel).Error(errorutil.ErrorDbSysTableNotExisted)
		return nil
	}
	defer m.relManager.CloseFile(datafile.Filename)

	recordList := record.FilterOnRecList(datafile.GetRecList(), condList)
	for _, attr := range attrNameList {
		col := TableColumn{
			relName:   relName,
			attrName:  attr,
			valueList: make([]parser.Value, 0),
		}
		offset := attrInfoMap[attr].AttrOffset
		length := attrInfoMap[attr].AttrSize
		attrType := attrInfoMap[attr].AttrType
		for _, rec := range recordList {
			if rec.Data[offset+length] == 1 {
				attrType = types.NO_ATTR // mark null here
			}
			col.valueList = append(col.valueList, parser.NewValueFromByteSlice(rec.Data[offset:offset+length], attrType))
		}
		col.attrSize = length
		col.attrType = attrType
		col.nullAllowed = attrInfoMap[attr].NullAllowed
		retTempTable = append(retTempTable, col)
	}
	return retTempTable
}
