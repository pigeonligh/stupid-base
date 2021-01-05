package dbsys

import (
	"testing"

	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

func TestDbSys(t *testing.T) {

	log.SetLevel(log.RecordLevel | log.StorageLevel | log.ExprLevel | log.DBSysLevel)

	manager := GetInstance()

	db1 := "testdb_1"
	db2 := "testdb_2"
	db3 := "testdb_3"

	if err := manager.CreateDB(db1); err != nil {
		t.Error(err)
		return
	}
	manager.ShowDatabases()
	if err := manager.OpenDB(db1); err != nil {
		t.Error(err)
		return
	}
	manager.ShowTables()

	rel1 := "rel1"
	attrInfoList := []parser.AttrInfo{
		{
			AttrName: strTo24ByteArray("attr1"),
			RelName:  strTo24ByteArray(rel1),
			AttrInfo: types.AttrInfo{
				AttrSize:             8,
				AttrType:             types.INT,
				IndexNo:              -1,
				NullAllowed:          false,
				IsPrimary:            false,
				HasForeignConstraint: false,
			},
		},
		{
			AttrName: strTo24ByteArray("attr2"),
			RelName:  strTo24ByteArray(rel1),
			AttrInfo: types.AttrInfo{
				AttrSize:             8,
				AttrType:             types.FLOAT,
				IndexNo:              -1,
				NullAllowed:          true,
				IsPrimary:            false,
				HasForeignConstraint: false,
			},
		},
		{
			AttrName: strTo24ByteArray("attr3"),
			RelName:  strTo24ByteArray(rel1),
			AttrInfo: types.AttrInfo{
				AttrSize: 24,
				AttrType: types.VARCHAR,
				IndexNo:  0,
			},
		},
	}
	if err := manager.CreateTable(rel1, attrInfoList, []ConstraintInfo{}); err != nil {
		t.Error(err)
		return
	}

	nameMap := make(map[int]string)
	nameMap[0] = "Alice"
	nameMap[1] = "Bob"
	nameMap[2] = "Carol"
	nameMap[3] = "Dog"
	nameMap[4] = "Emily"
	nameMap[5] = "Fred"
	nameMap[6] = "Harry"

	for i := 0; i < 64; i++ {
		err := manager.InsertRow(rel1, []types.Value{types.NewValueFromInt64(i), types.NewValueFromFloat64(0.1 + float64(i)), types.NewValueFromStr(nameMap[i%len(nameMap)])})
		if err != nil {
			t.Error(err)
			return
		}
	}
	if err := manager.CreateIndex("idx1", rel1, []string{"attr1"}, true); err != nil {
		t.Error(err)
		return
	}

	if err := manager.PrintTableMeta(rel1); err != nil {
		t.Error(err)
		return
	}
	if err := manager.PrintTableData(rel1); err != nil {
		t.Error(err)
		return
	}
	if err := manager.PrintTableIndex(rel1); err != nil {
		t.Error(err)
		return
	}
	if err := manager.ShowTablesWithDetails(); err != nil {
		t.Error(err)
		return
	}

	rel2 := "rel2"
	attrInfoList = []parser.AttrInfo{
		{
			AttrName: strTo24ByteArray("attr1"),
			RelName:  strTo24ByteArray(rel2),
			AttrInfo: types.AttrInfo{
				AttrSize:             8,
				AttrType:             types.INT,
				IndexNo:              0,
				NullAllowed:          false,
				IsPrimary:            false,
				HasForeignConstraint: false,
			},
		},
		{
			AttrName: strTo24ByteArray("attr2"),
			RelName:  strTo24ByteArray(rel2),
			AttrInfo: types.AttrInfo{
				AttrSize:             8,
				AttrType:             types.FLOAT,
				IndexNo:              0,
				NullAllowed:          true,
				IsPrimary:            false,
				HasForeignConstraint: false,
			},
		},
		{
			AttrName: strTo24ByteArray("attr3"),
			RelName:  strTo24ByteArray(rel2),
			AttrInfo: types.AttrInfo{
				AttrSize: 24,
				AttrType: types.VARCHAR,
				IndexNo:  0,
			},
		},
	}
	if err := manager.CreateTable(rel2, attrInfoList, []ConstraintInfo{}); err != nil {
		t.Error(err)
		return
	}

	tmpTable := manager.GetTemporalTableByAttrs(rel2, []string{"attr1", "attr2"}, []types.FilterCond{})
	manager.PrintTableByTmpColumns(tmpTable)

	// delete
	if err := manager.DropDB(db1); err != nil {
		t.Error(err)
		return
	}
	if err := manager.DropDB(db2); err != nil {
		t.Error(err)
		return
	}
	if err := manager.DropDB(db3); err != nil {
		t.Error(err)
		return
	}
}
