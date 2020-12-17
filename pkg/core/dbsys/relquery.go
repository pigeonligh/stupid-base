package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

// maybe it can be used for select & join
func (m *Manager) GetTemporalTableByAttrs(relName string, attrNameList []string, condList []record.FilterCond) TemporalTable {
	retTempTable := make(TemporalTable, 0)

	attrInfoMap := m.getAttrInfoMapViaCache(relName, false)

	datafile, err := m.relManager.OpenFile(getTableDataFileName(relName))
	if err != nil {
		log.V(log.DbSysLevel).Error(errorutil.ErrorDbSysRelationNotExisted)
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
