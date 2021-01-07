package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"testing"
	"time"
)

func TestDbSys(t *testing.T) {

	log.SetLevel(log.ExprLevel | log.DBSysLevel | log.IndexLevel)

	manager := GetInstance()

	db1 := "testdb_1"
	db2 := "testdb_2"
	db3 := "testdb_3"

	if err := manager.CreateDB(db1); err != nil {
		t.Error(err)
		return
	}
	//manager.ShowDatabases()
	if err := manager.OpenDB(db1); err != nil {
		t.Error(err)
		return
	}
	//manager.ShowTables()

	rel1 := "rel1"
	attrInfoList := []parser.AttrInfo{
		{
			AttrName:             "attr1",
			RelName:              rel1,
			IsPrimary:            false,
			HasForeignConstraint: false,
			AttrInfo: types.AttrInfo{
				AttrSize:    8,
				AttrType:    types.INT,
				NullAllowed: false,
			},
		},
		{
			AttrName:             "attr2",
			RelName:              rel1,
			HasForeignConstraint: false,
			IsPrimary:            false,
			AttrInfo: types.AttrInfo{
				AttrSize:    8,
				AttrType:    types.FLOAT,
				NullAllowed: false,
			},
		},
		{
			AttrName: "attr3",
			RelName:  rel1,
			AttrInfo: types.AttrInfo{
				AttrSize: 24,
				AttrType: types.VARCHAR,
			},
			Default: types.NewValueFromStr("THIS DEFAULT VALUE HHHHHHHHHHHHHHHHHHHHHH"),
		},
		{
			AttrName: "attr4",
			RelName:  rel1,
			AttrInfo: types.AttrInfo{
				AttrSize: 8,
				AttrType: types.DATE,
			},
		},
		{
			AttrName: "attr5",
			RelName:  rel1,
			AttrInfo: types.AttrInfo{
				AttrSize: 1,
				AttrType: types.BOOL,
			},
		},
	}
	if err := manager.CreateTable(rel1, attrInfoList); err != nil {
		t.Error(err)
		return
	}

	t.Log(manager.GetDBRelInfoMap())
	t.Log(manager.GetAttrInfoList(rel1))
	t.Log(manager.GetFkInfoMap())

	nameMap := make(map[int]string)
	nameMap[0] = "Alice fucks"
	nameMap[1] = "Bob sucks"
	nameMap[2] = "Carol shits"
	nameMap[3] = "Dog barks"
	nameMap[4] = "Emily sicks"
	nameMap[5] = "Fred haha"
	nameMap[6] = "Harry hey hey"

	for i := 0; i < 64; i++ {
		time := time.Now().AddDate(i, 0, 0)
		err := manager.InsertRow(rel1,
			[]types.Value{
				types.NewValueFromInt64(i),
				types.NewValueFromFloat64(0.1 + float64(i)),
				types.NewValueFromStr(nameMap[i%len(nameMap)]),
				types.NewValueFromDate(time),
				types.NewValueFromBool(i%2 == 0),
			})
		if err != nil {
			t.Error(err)
			return
		}
	}

	////if err := manager.CreateIndex("idx1", rel1, []string{"attr1"}, true); err != nil {
	////	t.Error(err)
	////	return
	////}
	//
	//if err := manager.PrintTableMeta(rel1); err != nil {
	//	t.Error(err)
	//	return
	//}
	//if err := manager.PrintTableIndex(rel1); err != nil {
	//	t.Error(err)
	//	return
	//}
	//
	if err := manager.PrintTableData(rel1); err != nil {
		t.Error(err)
		return
	}
	//
	//manager.ShowTablesWithDetails()
	//
	//// bug: when value type is not compatible from attr type, behavior is undefined
	////expr1 := parser.NewExprCompQuickAttrCompValue(8, 0, types.OpCompLE, types.NewValueFromInt64(10))
	//expr2 := parser.NewExprCompQuickAttrCompValue(8, 0, types.OpCompGE, types.NewValueFromInt64(40))
	//expr := parser.NewExprLogic(nil, types.OpLogicNOT, expr2)
	//
	//tmpTable, err := manager.GetTemporalTable(rel1, []string{"attr1", "attr2"}, expr)
	//if err != nil {
	//	t.Error(err)
	//	return
	//}
	//
	//manager.PrintTemporalTable(tmpTable)

	//rel2 := "rel2"
	//attrInfoList = []parser.AttrInfo{
	//	{
	//		AttrName: strTo24ByteArray("attr1"),
	//		RelName:  strTo24ByteArray(rel2),
	//		AttrInfo: types.AttrInfo{
	//			AttrSize:             8,
	//			AttrType:             types.INT,
	//			IndexNo:              0,
	//			NullAllowed:          false,
	//			IsPrimary:            false,
	//			HasForeignConstraint: false,
	//		},
	//	},
	//	{
	//		AttrName: strTo24ByteArray("attr2"),
	//		RelName:  strTo24ByteArray(rel2),
	//		AttrInfo: types.AttrInfo{
	//			AttrSize:             8,
	//			AttrType:             types.FLOAT,
	//			IndexNo:              0,
	//			NullAllowed:          true,
	//			IsPrimary:            false,
	//			HasForeignConstraint: false,
	//		},
	//	},
	//	{
	//		AttrName: strTo24ByteArray("attr3"),
	//		RelName:  strTo24ByteArray(rel2),
	//		AttrInfo: types.AttrInfo{
	//			AttrSize: 24,
	//			AttrType: types.VARCHAR,
	//			IndexNo:  0,
	//		},
	//	},
	//}
	//if err := manager.CreateTable(rel2, attrInfoList, []ConstraintInfo{}); err != nil {
	//	t.Error(err)
	//	return
	//}

	//tmpTable := manager.GetTemporalTableByAttrs(rel2, []string{"attr1", "attr2"}, []types.FilterCond{})
	//manager.PrintTableByTmpColumns(tmpTable)

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
