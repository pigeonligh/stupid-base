package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"unsafe"
)

type AttrInfoMap map[string]*parser.AttrInfo
type AttrInfoRidMap map[string]types.RID

type RelInfoMap map[string]*RelInfo
type RelInfoRidMap map[string]types.RID

type IdxNo2InfoMap map[int]*IndexInfo
type IdxName2RidsMap map[string][]types.RID

type IdxNo2ColNameMap map[int][]string
type AttrInfoDetailedCollection struct {
	infoMap AttrInfoMap
	ridMap  AttrInfoRidMap
	pkMap   AttrInfoMap // primary key map
	fkMap   AttrInfoMap // foreign key map
	idxMap  IdxNo2ColNameMap
}

// getRelInfoMapWithRid used for create fast map to accelerate get relation
func (m *Manager) getRelInfoMapWithRid() (RelInfoMap, RelInfoRidMap) {
	// is checked opened before
	relInfoMap := make(RelInfoMap, 0)
	relInfoRidMap := make(RelInfoRidMap, 0)
	var rawAttrList = m.dbMeta.GetRecList()
	for _, rawAttr := range rawAttrList {
		rel := (*RelInfo)(types.ByteSliceToPointer(rawAttr.Data))
		relInfoMap[ByteArray24tostr(rel.relName)] = rel
		relInfoRidMap[ByteArray24tostr(rel.relName)] = rawAttr.Rid
	}
	return relInfoMap, relInfoRidMap
}

// getIdxInfoMapWithRid
func (m *Manager) getIdxInfoMapWithRid(relName string) (IdxNo2InfoMap, IdxName2RidsMap) {
	// is checked opened before
	idxNo2InfoMap := make(IdxNo2InfoMap, 0)
	idxName2RidsMap := make(IdxName2RidsMap, 0)

	fh, _ := m.relManager.OpenFile(getTableIdxFileName(relName))
	var rawIdxList = fh.GetRecList()
	for _, rawIdx := range rawIdxList {
		idx := (*IndexInfo)(types.ByteSliceToPointer(rawIdx.Data))
		idxName := ByteArray24tostr(idx.indexName)
		if _, found := idxName2RidsMap[idxName]; !found {
			idxName2RidsMap[idxName] = []types.RID{}
		}
		idxName2RidsMap[idxName] = append(idxName2RidsMap[idxName], rawIdx.Rid)
		idxNo2InfoMap[idx.indexNo] = idx
	}
	return idxNo2InfoMap, idxName2RidsMap
}

// getAttrInfoMapViaCache used for create fast map to accelerate get attribute
// return nothing when *reload* parameter is specified
// if attrInfoMap is provided as non-nil, then won't further call m.getAttrInfoDetailedCollection
// which is a heavy function
func (m *Manager) getAttrInfoMapViaCache(relName string, reload bool, attrInfoMap AttrInfoMap) AttrInfoMap {

	// m.rels must found, as it has been guaranteed in parent calls
	// dose not consider update
	if infoMap, _ := m.rels[relName]; infoMap != nil && !reload {
		return infoMap
	} else {
		if reload {
			if attrInfoMap != nil {
				m.rels[relName] = attrInfoMap
			} else {
				m.rels[relName] = m.getAttrInfoDetailedCollection(relName).infoMap
			}
			return nil
		} else {
			return infoMap
		}
	}
}

func (m *Manager) getAttrInfoDetailedCollection(relName string) AttrInfoDetailedCollection {
	fileHandle, err := m.relManager.OpenFile(getTableMetaFileName(relName))
	defer m.relManager.CloseFile(fileHandle.Filename)
	if err != nil {
		// once build attrName info map is recalled, it must be existed
		panic(0)
	}
	attrInfoMap := make(AttrInfoMap, 0)
	pkMap := make(AttrInfoMap, 0)
	fkMap := make(AttrInfoMap, 0)
	attrInfoRidMap := make(AttrInfoRidMap, 0)
	attrIndexMap := make(IdxNo2ColNameMap, 0)

	var rawAttrList = fileHandle.GetRecList()
	for _, rawAttr := range rawAttrList {
		attr := (*parser.AttrInfo)(types.ByteSliceToPointer(rawAttr.Data))
		attrInfoMap[ByteArray24tostr(attr.AttrName)] = attr
		attrInfoRidMap[ByteArray24tostr(attr.AttrName)] = rawAttr.Rid
		if attr.IsPrimary {
			pkMap[ByteArray24tostr(attr.AttrName)] = attr
		}
		if attr.HasForeignConstraint {
			fkMap[ByteArray24tostr(attr.AttrName)] = attr
		}
		if attr.IndexNo != -1 {
			if _, found := attrIndexMap[attr.IndexNo]; !found {
				attrIndexMap[attr.IndexNo] = make([]string, 0)
				attrIndexMap[attr.IndexNo] = append(attrIndexMap[attr.IndexNo], ByteArray24tostr(attr.AttrName))
			} else {
				attrIndexMap[attr.IndexNo] = append(attrIndexMap[attr.IndexNo], ByteArray24tostr(attr.AttrName))
			}
		}
	}
	return AttrInfoDetailedCollection{
		infoMap: attrInfoMap,
		ridMap:  attrInfoRidMap,
		pkMap:   pkMap,
		fkMap:   pkMap,
		idxMap:  attrIndexMap,
	}
}

func (m *Manager) checkTableAndAttrExistence(relName string, attrNameList []string) bool {
	if len(m.dbSelected) == 0 {
		panic(0)
	}
	if _, found := m.rels[relName]; !found {
		return false
	}
	attrInfoMap := m.getAttrInfoMapViaCache(relName, false, nil)
	for _, attr := range attrNameList {
		if _, found := attrInfoMap[attr]; !found {
			return false
		}
	}
	return true
}

// insert or delete, no update
func (m *Manager) insertOrRemoveIndexInfo(relName string, idxInfo *IndexInfo, insert bool, ridList []types.RID) {
	fh, _ := m.relManager.OpenFile(getTableIdxFileName(relName))
	defer m.relManager.CloseFile(fh.Filename)
	if insert {
		if _, err := fh.InsertRec(types.PointerToByteSlice(unsafe.Pointer(idxInfo), int(unsafe.Sizeof(idxInfo)))); err != nil {
			panic(0)
		}
	} else {
		//delete
		fh.DeleteRecByBatch(ridList)
	}
}

func (m *Manager) updateRelInfo(relName string, relRID types.RID, relInfo *RelInfo, remove bool) {
	if fileHandle, err := m.relManager.OpenFile(DbMetaName); err != nil {
		panic(0)
	} else {
		defer m.relManager.CloseFile(fileHandle.Filename)
		if remove {
			err := fileHandle.DeleteRec(relRID)
			if err != nil {
				panic(0)
			}
		} else {
			if rec, err := fileHandle.GetRec(relRID); err != nil {
				panic(0)
			} else {
				originRel := (*RelInfo)(types.ByteSliceToPointer(rec.Data))
				*originRel = *relInfo
				fileHandle.ForcePage(relRID.Page)
			}
		}
	}
}

func (m *Manager) updateAttrInfo(relName string, attrName string, attrRID types.RID, attrInfo *parser.AttrInfo, remove bool) {
	// removal constraint will be checked in the previous callers
	if fileHandle, err := m.relManager.OpenFile(getTableMetaFileName(relName)); err != nil {
		panic(0)
	} else {
		defer m.relManager.CloseFile(fileHandle.Filename)
		if remove {
			err := fileHandle.DeleteRec(attrRID)
			if err != nil {
				panic(0)
			}
		} else {
			if rec, err := fileHandle.GetRec(attrRID); err != nil {
				panic(0)
			} else {
				originAttr := (*parser.AttrInfo)(types.ByteSliceToPointer(rec.Data))
				*originAttr = *attrInfo
				fileHandle.ForcePage(attrRID.Page)
			}
		}
	}
}
