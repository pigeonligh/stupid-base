package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"unsafe"
)

type RelInfoMap map[string]*RelInfo
type RelInfoRidMap map[string]types.RID

// getRelInfoMapWithRid used for create fast map to accelerate get relation
func (m *Manager) getRelInfoMapWithRid() (RelInfoMap, RelInfoRidMap) {
	// is checked opened before
	relInfoMap := make(RelInfoMap)
	relInfoRidMap := make(RelInfoRidMap)
	var rawAttrList = m.dbMeta.GetRecList()
	for _, rawAttr := range rawAttrList {
		rel := (*RelInfo)(types.ByteSliceToPointer(rawAttr.Data))
		relInfoMap[ByteArray24tostr(rel.relName)] = rel
		relInfoRidMap[ByteArray24tostr(rel.relName)] = rawAttr.Rid
	}
	return relInfoMap, relInfoRidMap
}

type IdxNo2ColsMap map[int][]string
type IdxName2ColsMap map[string][]string
type IdxName2RidsMap map[string][]types.RID

type IdxInfoDetailedCollection struct {
	no2cols   IdxNo2ColsMap
	name2cols IdxName2ColsMap
	name2rids IdxName2RidsMap
}

func (m *Manager) getIdxDetailedInfoCollection(relName string) IdxInfoDetailedCollection {
	// is checked opened before
	idxNo2ColsMap := make(IdxNo2ColsMap)
	idxName2ColsMap := make(IdxName2ColsMap)
	idxName2RidsMap := make(IdxName2RidsMap)

	fh, _ := m.relManager.OpenFile(getTableIdxMetaFileName(relName))
	defer m.relManager.CloseFile(getTableIdxMetaFileName(relName))
	var rawIdxList = fh.GetRecList()
	for _, rawIdx := range rawIdxList {
		idx := (*IndexInfo)(types.ByteSliceToPointer(rawIdx.Data))
		idxName := ByteArray24tostr(idx.idxName)

		if _, found := idxName2RidsMap[idxName]; !found {
			idxName2RidsMap[idxName] = []types.RID{}
			idxName2ColsMap[idxName] = []string{}
			idxNo2ColsMap[idx.idxNo] = []string{}
		}
		idxName2RidsMap[idxName] = append(idxName2RidsMap[idxName], rawIdx.Rid)
		idxName2ColsMap[idxName] = append(idxName2ColsMap[idxName], ByteArray24tostr(idx.col))
		idxNo2ColsMap[idx.idxNo] = append(idxNo2ColsMap[idx.idxNo], ByteArray24tostr(idx.col))

	}
	return IdxInfoDetailedCollection{
		no2cols:   idxNo2ColsMap,
		name2cols: idxName2ColsMap,
		name2rids: idxName2RidsMap,
	}
}

type tmpForeignConstraint struct {
	srcAttrs *types.AttrSet
	dstAttrs *types.AttrSet
}
type ForeignConstraintMap map[string]tmpForeignConstraint
type ForeignConstraintDetailedInfo struct {
	srcFkMap  ForeignConstraintMap
	dstFkMap  ForeignConstraintMap
	fk2relSrc map[string]string
	fk2relDst map[string]string // map from fk 2 fk dst
}

func (m *Manager) getForeignConstraintDetailedInfo(relName string) ForeignConstraintDetailedInfo {
	// one for src and another is for dst
	fkFile, err := m.relManager.OpenFile(GlbFkFileName)
	if err != nil {
		panic(0) // since open file has benn confirmed by callers
	}
	defer m.relManager.CloseFile(fkFile.Filename)
	filterConds := []types.FilterCond{
		{
			AttrSize:   24,
			AttrOffset: 24,
			CompOp:     types.OpCompEQ,
			Value:      types.NewValueFromStr(relName),
		},
		{
			AttrSize:   24,
			AttrOffset: 72,
			CompOp:     types.OpCompEQ,
			Value:      types.NewValueFromStr(relName),
		},
	} // src

	recList, _ := fkFile.GetFilteredRecList(filterConds, types.OpLogicOR)
	srcFkMap := make(ForeignConstraintMap)
	dstFkMap := make(ForeignConstraintMap)
	fk2relSrc := make(map[string]string)
	fk2relDst := make(map[string]string)

	for _, raw := range recList {
		consRec := (*ConstraintForeignInfo)(types.ByteSliceToPointer(raw.Data))
		fk := ByteArray24tostr(consRec.fkName)
		relSrc := ByteArray24tostr(consRec.relSrc)
		relDst := ByteArray24tostr(consRec.relDst)
		attrSrc := ByteArray24tostr(consRec.attrSrc)
		attrDst := ByteArray24tostr(consRec.attrDst)

		relSrcMap := m.getAttrInfoMapViaCacheOrReload(relSrc, nil)
		relDstMap := m.getAttrInfoMapViaCacheOrReload(relDst, nil)
		if relSrc == relName {
			// act as key referencing other primary keys
			if _, found := srcFkMap[fk]; !found {
				srcFkMap[fk] = tmpForeignConstraint{
					srcAttrs: types.NewAttrSet(),
					dstAttrs: types.NewAttrSet(),
				}
			}
			srcFkMap[fk].srcAttrs.AddSingleAttr(relSrcMap[attrSrc].AttrInfo)
			srcFkMap[fk].dstAttrs.AddSingleAttr(relDstMap[attrDst].AttrInfo)
		}
		if relDst == relName {
			// act as a referenced primary key
			if _, found := dstFkMap[fk]; !found {
				dstFkMap[fk] = tmpForeignConstraint{
					srcAttrs: types.NewAttrSet(),
					dstAttrs: types.NewAttrSet(),
				}
			}
			dstFkMap[fk].srcAttrs.AddSingleAttr(relSrcMap[attrSrc].AttrInfo)
			dstFkMap[fk].dstAttrs.AddSingleAttr(relDstMap[attrDst].AttrInfo)
		}
		fk2relSrc[fk] = relSrc
		fk2relDst[fk] = relDst
	}
	return ForeignConstraintDetailedInfo{
		srcFkMap:  srcFkMap,
		dstFkMap:  dstFkMap,
		fk2relSrc: fk2relSrc,
		fk2relDst: fk2relDst,
	}
}

type AttrInfoMap map[string]*parser.AttrInfo
type AttrInfoRidMap map[string]types.RID
type AttrNameList []string

type Col2IdxNoMap map[string]int
type AttrInfoDetailedCollection struct {
	nameList       AttrNameList
	infoMap        AttrInfoMap
	ridMap         AttrInfoRidMap
	pkMap          AttrInfoMap // primary key map
	fkMap          AttrInfoMap // foreign key map
	nkMap          AttrInfoMap // null key map
	col2idxNameMap Col2IdxNoMap
}

// getAttrInfoMapViaCacheOrReload used for create fast map to accelerate get attribute
// return nothing when reloadInfoMap is specified
// return the cached info when is nil
// which is a heavy function
func (m *Manager) getAttrInfoMapViaCacheOrReload(relName string, reloadInfoMap AttrInfoMap) AttrInfoMap {
	// m.rels must found, as it has been guaranteed in parent calls
	if reloadInfoMap != nil {
		m.rels[relName] = reloadInfoMap
		return nil // reload using parameter map
	}

	if res := m.rels[relName]; res != nil {
		return res
	}
	m.rels[relName] = m.getAttrInfoDetailedCollection(relName).infoMap
	return m.rels[relName]
}

func (m *Manager) getAttrInfoDetailedCollection(relName string) AttrInfoDetailedCollection {
	fileHandle, err := m.relManager.OpenFile(getTableMetaFileName(relName))
	defer m.relManager.CloseFile(fileHandle.Filename)
	if err != nil {
		// once build attrName info map is recalled, it must be existed
		panic(0)
	}
	attrNameList := make([]string, 0)
	attrInfoMap := make(AttrInfoMap)
	pkMap := make(AttrInfoMap)
	fkMap := make(AttrInfoMap)
	nkMap := make(AttrInfoMap)
	attrInfoRidMap := make(AttrInfoRidMap)
	attrIndexMap := make(Col2IdxNoMap)

	var rawAttrList = fileHandle.GetRecList()
	for _, rawAttr := range rawAttrList {
		attr := (*parser.AttrInfo)(types.ByteSliceToPointer(rawAttr.Data))
		attrName := ByteArray24tostr(attr.AttrName)
		attrNameList = append(attrNameList, attrName)
		attrInfoMap[attrName] = attr
		attrInfoRidMap[attrName] = rawAttr.Rid
		if attr.IsPrimary {
			pkMap[attrName] = attr
		}
		if attr.HasForeignConstraint {
			fkMap[attrName] = attr
		}
		if attr.NullAllowed {
			nkMap[attrName] = attr
		}
		if attr.IndexNo != -1 {
			attrIndexMap[attrName] = attr.IndexNo
		}
	}
	return AttrInfoDetailedCollection{
		nameList:       attrNameList,
		infoMap:        attrInfoMap,
		ridMap:         attrInfoRidMap,
		pkMap:          pkMap,
		fkMap:          pkMap,
		col2idxNameMap: attrIndexMap,
	}
}

// insert or delete, no update
func (m *Manager) insertOrRemoveIndexInfo(relName string, idxInfo *IndexInfo, insert bool, ridList []types.RID) {
	fh, _ := m.relManager.OpenFile(getTableIdxMetaFileName(relName))
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
	if fileHandle, err := m.relManager.OpenFile(DBMetaName); err != nil {
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

func (m *Manager) updateAttrInfo(relName string, attrRID types.RID, attrInfo *parser.AttrInfo, remove bool) {
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
