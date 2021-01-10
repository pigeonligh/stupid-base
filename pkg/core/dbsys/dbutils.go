package dbsys

import (
	"encoding/gob"
	"os"

	"github.com/pigeonligh/stupid-base/pkg/core/env"
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
)

func compareBytes(attr1, attr2 []byte) int {
	if len(attr1) > len(attr2) {
		return -compareBytes(attr2, attr1)
	}
	compareLength := len(attr1)
	for i := 0; i < compareLength; i++ {
		if attr1[i] == attr2[i] {
			continue
		}
		if attr1[i] < attr2[i] {
			return 1
		}
		return -1
	}
	if len(attr1) < len(attr2) {
		return 1
	}
	return 0
}

func uniqueStringList(list1 []string) []string {
	tmpMap := make(map[string]int)
	for _, attr := range list1 {
		tmpMap[attr] = 10
	}
	retList := make([]string, 0)
	for key := range tmpMap {
		retList = append(retList, key)
	}
	return retList
}

func checkIfaLEb(a, b []string) bool {
	tmpMap := make(map[string]int)
	for _, attr := range b {
		tmpMap[attr] = 10
	}
	for _, attr := range a {
		if _, found := tmpMap[attr]; !found {
			return false
		}
	}
	return true
}

type RelInfoMap map[string]RelInfo

func (m *Manager) GetDBRelInfoMap() RelInfoMap {
	// existence has been checked
	file, err := os.OpenFile(env.WorkDir+"/"+DBMetaName, os.O_RDWR|os.O_SYNC, 0666)
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	var decodedMap RelInfoMap
	d := gob.NewDecoder(file)
	// Decoding the serialized data
	err = d.Decode(&decodedMap)
	if err != nil {
		panic(err)
	}
	return decodedMap
}

func (m *Manager) SetDBRelInfoMap(infoMap RelInfoMap) {
	file, err := os.OpenFile(env.WorkDir+"/"+DBMetaName, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		panic(err)
	}
	e := gob.NewEncoder(file)
	// Encoding the map
	err = e.Encode(infoMap)
	if err != nil {
		panic(err)
	}
}

func (m *Manager) SetRelInfo(info RelInfo) {
	relInfoMap := m.GetDBRelInfoMap()
	relInfoMap[info.RelName] = info
	m.SetDBRelInfoMap(relInfoMap)
}

//type IdxName2ColsMap map[string][]string
//type IdxCol2NameMap map[string]string
//type IdxInfoCollection struct {
//	Name2Cols IdxName2ColsMap
//	Col2Name  IdxCol2NameMap
//}
//
//func (m *Manager) GetIdxInfoCollection(relName string) IdxInfoCollection {
//	file, err := os.OpenFile(env.WorkDir + "/" + getTableIdxMetaFileName(relName), os.O_RDWR|os.O_SYNC, 0666)
//	defer func() {
//		if err := file.Close(); err != nil {
//			panic(err)
//		}
//	}()
//	var decodedMap IdxInfoCollection
//	d := gob.NewDecoder(file)
//	// Decoding the serialized data
//	err = d.Decode(&decodedMap)
//	if err != nil {
//		panic(err)
//	}
//	return decodedMap
//}
//
//func (m *Manager) SetIdxInfoCollection(relName string, collection IdxInfoCollection) {
//	// idx existence has been checked in upper callers
//	file, err := os.OpenFile(env.WorkDir + "/" + getTableIdxMetaFileName(relName), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
//	defer func() {
//		if err := file.Close(); err != nil {
//			panic(err)
//		}
//	}()
//	if err != nil {
//		panic(err)
//	}
//	e := gob.NewEncoder(file)
//	// Encoding the map
//	err = e.Encode(collection)
//	if err != nil {
//		panic(err)
//	}
//}
//

type FkConstraint struct {
	FkName  string
	SrcRel  string
	DstRel  string
	SrcAttr []string
	DstAttr []string
}

const GlbFkFileName = "db.fk-meta"

type FkConstraintMap map[string]FkConstraint

func (m *Manager) GetFkInfoMap() FkConstraintMap {
	file, err := os.OpenFile(env.WorkDir+"/"+GlbFkFileName, os.O_RDWR|os.O_SYNC, 0666)
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	var decodedMap FkConstraintMap
	d := gob.NewDecoder(file)
	// Decoding the serialized data
	err = d.Decode(&decodedMap)
	if err != nil {
		panic(err)
	}
	return decodedMap
}

func (m *Manager) SetFkInfoMap(constraintMap FkConstraintMap) {
	file, err := os.OpenFile(env.WorkDir+"/"+GlbFkFileName, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	if err != nil {
		panic(err)
	}
	e := gob.NewEncoder(file)
	// Encoding the map
	err = e.Encode(constraintMap)
	if err != nil {
		panic(err)
	}
}

type AttrInfoMap map[string]parser.AttrInfo
type AttrInfoList []parser.AttrInfo
type AttrInfoCollection struct {
	NameList []string
	PkList   []string
	NkList   []string
	InfoMap  AttrInfoMap
	FkMap    map[string][]string
	IdxMap   map[string][]string
}

func (m *Manager) GetAttrInfoList(relName string) AttrInfoList {
	if res := m.rels[relName]; res != nil {
		return res
	}
	file, err := os.OpenFile(env.WorkDir+"/"+getTableMetaFileName(relName), os.O_RDWR|os.O_SYNC, 0666)
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	var decodedMap AttrInfoList
	d := gob.NewDecoder(file)
	// Decoding the serialized data
	err = d.Decode(&decodedMap)
	if err != nil {
		panic(err)
	}
	m.rels[relName] = decodedMap
	return decodedMap
}

func (m *Manager) SetAttrInfoList(relName string, attrInfoList AttrInfoList) {
	m.rels[relName] = attrInfoList
	file, err := os.OpenFile(env.WorkDir+"/"+getTableMetaFileName(relName), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	if err != nil {
		panic(err)
	}
	e := gob.NewEncoder(file)
	// Encoding the map
	err = e.Encode(attrInfoList)
	if err != nil {
		panic(err)
	}
}

func (m *Manager) SetAttrInfoListByCollection(relName string, collection AttrInfoCollection) {
	attrInfoList := make(AttrInfoList, 0)
	for _, name := range collection.NameList {
		attrInfoList = append(attrInfoList, collection.InfoMap[name])
	}
	m.SetAttrInfoList(relName, attrInfoList)
	file, err := os.OpenFile(env.WorkDir+"/"+getTableMetaFileName(relName), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	if err != nil {
		panic(err)
	}
	e := gob.NewEncoder(file)
	// Encoding the map
	err = e.Encode(attrInfoList)
	if err != nil {
		panic(err)
	}
}

func (m *Manager) GetAttrInfoCollection(relName string) AttrInfoCollection {
	attrInfoList := m.GetAttrInfoList(relName)

	attrNameList := make([]string, 0)
	attrInfoMap := make(AttrInfoMap)
	pkList := make([]string, 0)
	nkList := make([]string, 0)
	fkMap := make(map[string][]string)
	idxMap := make(map[string][]string)

	for _, attr := range attrInfoList {
		attrInfoMap[attr.AttrName] = attr
		attrNameList = append(attrNameList, attr.AttrName)
		if attr.IsPrimary {
			pkList = append(pkList, attr.AttrName)
		}
		if attr.NullAllowed {
			nkList = append(nkList, attr.AttrName)
		}
		if len(attr.FkName) != 0 {
			if _, found := fkMap[attr.FkName]; !found {
				fkMap[attr.FkName] = make([]string, 0)
			}
			fkMap[attr.FkName] = append(fkMap[attr.FkName], attr.AttrName)
		}
		if len(attr.IndexName) != 0 {
			if _, found := idxMap[attr.IndexName]; !found {
				idxMap[attr.IndexName] = make([]string, 0)
			}
			idxMap[attr.IndexName] = append(idxMap[attr.IndexName], attr.AttrName)
		}
	}
	return AttrInfoCollection{
		NameList: attrNameList,
		FkMap:    fkMap,
		PkList:   pkList,
		NkList:   nkList,
		InfoMap:  attrInfoMap,
		IdxMap:   idxMap,
	}
}

func (m *Manager) GetAttrSetFromAttrs(relName string, attrNames []string) types.AttrSet {
	attrInfoMap := m.GetAttrInfoCollection(relName).InfoMap
	attrSet := types.AttrSet{}
	for _, attr := range attrNames {
		attrSet.AddSingleAttr(attrInfoMap[attr].AttrInfo)
	}
	return attrSet
}

func (m *Manager) checkDBTableAndAttrExistence(relName string, attrNameList []string) error {
	if len(m.dbSelected) == 0 {
		return errorutil.ErrorDBSysDBNotSelected
	}
	if _, found := m.rels[relName]; !found {
		return errorutil.ErrorDBSysRelationNotExisted
	}
	if len(uniqueStringList(attrNameList)) != len(attrNameList) {
		return errorutil.ErrorDBSysDuplicatedAttrsFound
	}

	if attrNameList != nil {
		attrInfoMap := m.GetAttrInfoCollection(relName).InfoMap
		for _, attr := range attrNameList {
			if _, found := attrInfoMap[attr]; !found {
				return errorutil.ErrorDBSysAttrNotExisted
			}
		}
	}
	return nil
}
