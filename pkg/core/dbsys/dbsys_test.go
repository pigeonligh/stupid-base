package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"strconv"
	"testing"
)

func TestDbSys(t *testing.T) {
	//log.SetLevel(log.DBSysLevel)

	manager := GetInstance()

	db1 := "testdb_1"
	//db2 := "testdb_2"
	//db3 := "testdb_3"

	if err := manager.CreateDB(db1); err != nil {
		t.Error(err)
		return
	}
	//manager.PrintDatabases()
	if err := manager.OpenDB(db1); err != nil {
		t.Error(err)
		return
	}
	//manager.PrintTables()

	rel1 := "rel1"
	attrInfoList := []parser.AttrInfo{
		{
			AttrName:  "attr1",
			RelName:   rel1,
			IsPrimary: false,
			AttrInfo: types.AttrInfo{
				AttrSize:    8,
				AttrType:    types.INT,
				NullAllowed: false,
			},
		},
		{
			AttrName:  "attr2",
			RelName:   rel1,
			IsPrimary: false,
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

	nameMap := make(map[int]string)
	nameMap[0] = "Alice fucks"
	nameMap[1] = "Bob sucks"
	nameMap[2] = "Carol shits"
	nameMap[3] = "Dog barks"
	nameMap[4] = "Emily sicks"
	nameMap[5] = "Fred haha"
	nameMap[6] = "Harry hey hey"

	for i := 0; i < 10000; i++ {
		//time := time.Now().AddDate(0, 0, i)
		err := manager.InsertRow(rel1,
			[]string{
				strconv.Itoa(i), strconv.FormatFloat(0.1+float64(i), 'g', 10, 64),
				nameMap[i%len(nameMap)], "2018-Feb-28", strconv.FormatBool(i%2 == 0),
			})
		if err != nil {
			t.Error(err)
			return
		}
	}
	if err := manager.AddPrimaryKey(rel1, []string{"attr1"}); err != nil {
		t.Error(err)
		return
	}

	if err := manager.DeleteRows(rel1, parser.NewExprCompQuickAttrCompValue(8, 0, types.OpCompLE, types.NewValueFromInt64(1000))); err != nil {
		t.Error(err)
		return
	}

	if err := manager.DropDB(db1); err != nil {
		t.Error(err)
		return
	}

	return

	rel2 := "rel2"
	attrInfoList2 := []parser.AttrInfo{
		{
			AttrName:  "attr1",
			RelName:   rel1,
			IsPrimary: false,
			AttrInfo: types.AttrInfo{
				AttrSize:    8,
				AttrType:    types.INT,
				NullAllowed: false,
			},
		},
		{
			AttrName:  "attr2",
			RelName:   rel1,
			IsPrimary: false,
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
	}

	if err := manager.CreateTable(rel2, attrInfoList2); err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 8; i++ {
		//time := time.Now().AddDate(0, 0, i)
		err := manager.InsertRow(rel2, []string{strconv.Itoa(i), strconv.FormatFloat(0.1+float64(i), 'g', 10, 64), nameMap[i%len(nameMap)]})
		if err != nil {
			t.Error(err)
			return
		}
	}

	//manager.SelectSingleTableByExpr(rel1, []string{}, parser.NewExprCompQuickAttrCompValue(8, 0, types.OpCompLE, types.NewValueFromInt64(1)), true)

	manager.PrintTablesWithDetails()
	manager.PrintTableMeta(rel1)
	manager.PrintTableMeta(rel2)
	manager.SelectSingleTableByExpr(rel1, []string{}, nil, true)
	manager.SelectSingleTableByExpr(rel2, []string{}, nil, true)

	log.Debug("add multi-cols primary key test")
	if err := manager.AddPrimaryKey(rel1, []string{"attr1", "attr3"}); err != nil {
		t.Error(err)
		return
	}
	if _, err := manager.SelectSingleTableByExpr(rel1, []string{}, nil, true); err != nil {
		t.Log(err)
		return
	}
	manager.PrintTableMeta(rel1)
	log.Debug("Insert invalid value for rel1")
	if err := manager.InsertRow(rel1,
		[]string{
			strconv.Itoa(1), strconv.FormatFloat(0.1+float64(1), 'g', 10, 64),
			nameMap[1%len(nameMap)], "2018-Feb-28", strconv.FormatBool(6%2 == 0),
		}); err != nil {
		t.Log(err)
	}

	log.Debug("Insert valid value for rel2")
	if err := manager.InsertRow(rel1,
		[]string{
			strconv.Itoa(1), strconv.FormatFloat(0.1+float64(6), 'g', 10, 64),
			nameMap[2%len(nameMap)], "2018-Feb-28", strconv.FormatBool(6%2 == 0),
		}); err != nil {
		t.Error(err)
		return
	}
	_, _ = manager.SelectSingleTableByExpr(rel1, []string{}, nil, true)

	log.Debug("add single primary key test")
	if err := manager.AddPrimaryKey(rel2, []string{"attr3"}); err != nil {
		t.Log(err)
	}
	manager.PrintTableMeta(rel2)
	if err := manager.AddPrimaryKey(rel2, []string{"attr1"}); err != nil {
		t.Error(err)
		return
	}
	manager.PrintTableMeta(rel2)
	log.Debug("remove primary key")
	if err := manager.DropPrimaryKey(rel2); err != nil {
		t.Log(err)
	}
	manager.PrintTableMeta(rel2)

	log.Debug("add foreign key test")
	if err := manager.AddForeignKey("fk1", rel2, []string{"attr1", "attr3"}, rel1, []string{"attr1", "attr3"}); err != nil {
		t.Error(err)
		return
	}
	manager.PrintDBForeignInfos()
	manager.PrintTableMeta(rel2)
	manager.PrintTableMeta(rel1)

	log.Debug("delete invalid rows")
	if err := manager.DeleteRows(rel1, parser.NewExprCompQuickAttrCompValue(8, 0, types.OpCompLE, types.NewValueFromInt64(10))); err != nil {
		log.Warning(err)
	}
	log.Debug("delete valid rows")
	if err := manager.DeleteRows(rel1, parser.NewExprCompQuickAttrCompValue(8, 0, types.OpCompGE, types.NewValueFromInt64(9))); err != nil {
		log.Error(err)
		return
	}
	log.Debug("insert invalid rows")
	if err := manager.InsertRow(rel2, []string{strconv.Itoa(10), strconv.FormatFloat(0.1+float64(10), 'g', 10, 64), nameMap[10%len(nameMap)]}); err != nil {
		log.Warning(err)
	}
	log.Debug("insert valid rows")
	if err := manager.InsertRow(rel2, []string{strconv.Itoa(1), strconv.FormatFloat(0.1+float64(10), 'g', 10, 64), nameMap[1%len(nameMap)]}); err != nil {
		log.Error(err)
		return
	}

	tmp1, _ := manager.SelectSingleTableByExpr(rel1, []string{},
		parser.NewExprCompQuickAttrCompValue(8, 0, types.OpCompLE, types.NewValueFromInt64(5)), true)

	//tmp2, _ := manager.SelectSingleTableByExpr(rel2, []string{},
	//	parser.NewExprCompQuickAttrCompValue(8, 0, types.OpCompGE, types.NewValueFromInt64(4)), true)
	//
	//attrInfoMap1 := manager.GetAttrInfoCollection(rel1).InfoMap
	//attrInfoMap2 := manager.GetAttrInfoCollection(rel2).InfoMap

	tmpMap := map[string]AttrInfoList{
		rel1: manager.GetAttrInfoList(rel1),
		//rel2: manager.GetAttrInfoList(rel2),
	}

	//l := parser.NewExprAttr(attrInfoMap1["attr1"])
	//r := parser.NewExprAttr(attrInfoMap2["attr1"])
	//parser.NewExprComp(l, types.OpCompEQ, r)
	if err := manager.SelectFromMultiple([]*TemporalTable{tmp1}, tmpMap, nil); err != nil {
		t.Error(err)
	}

	// delete
	if err := manager.DropDB(db1); err != nil {
		t.Error(err)
		return
	}

	return

	// test primary key

	if err := manager.AddPrimaryKey(rel2, []string{"attr1", "attr3"}); err != nil {
		t.Error(err)
		return
	}

	if err := manager.DropPrimaryKey(rel1); err != nil {
		t.Log(err)
	}

	if err := manager.DeleteRows(rel1, parser.NewExprCompQuickAttrCompValue(8, 0, types.OpCompEQ, types.NewValueFromInt64(1))); err != nil {
		t.Log(err)
	}

	manager.PrintTablesWithDetails()
	manager.PrintTableMeta(rel1)
	manager.PrintTableMeta(rel2)

	// test foreign key
	for i := 0; i < 5; i++ {
		_ = manager.InsertRow(rel2,
			[]string{
				strconv.Itoa(i), strconv.FormatFloat(0.1+float64(i), 'g', 10, 64),
				nameMap[i%len(nameMap)]},
		)
	}
	// test foreign key
	for i := 20; i < 30; i++ {
		_ = manager.InsertRow(rel2,
			[]string{
				strconv.Itoa(i), strconv.FormatFloat(0.1+float64(i), 'g', 10, 64),
				nameMap[i%len(nameMap)],
			},
		)
	}
	// test foreign key
	for i := 0; i < 10; i++ {
		err := manager.InsertRow(rel1,
			[]string{
				strconv.Itoa(i), strconv.FormatFloat(0.1+float64(i), 'g', 10, 64),
				nameMap[i%len(nameMap)], "2018-Feb-28", strconv.FormatBool(i%2 == 0)},
		)
		if err != nil {
			t.Error(err)
			return
		}
	}
	if err := manager.DeleteRows(rel1, parser.NewExprCompQuickAttrCompValue(8, 0, types.OpCompGE, types.NewValueFromInt64(110))); err != nil {
		t.Error(err)
		return
	}

	//manager.PrintDatabases()
	//manager.PrintTables()
	manager.PrintTablesWithDetails()
	manager.PrintTableMeta(rel1)
	manager.PrintTableMeta(rel2)
	manager.SelectSingleTableByExpr(rel1, []string{}, nil, true)
	manager.SelectSingleTableByExpr(rel2, []string{}, nil, true)

	manager.PrintDBForeignInfos()
	if err := manager.DropForeignKey("fk1"); err != nil {
		t.Error(err)
		return
	}
	manager.PrintDBForeignInfos()

	//
	if err := manager.PrintTableData(rel2); err != nil {
		t.Error(err)
		return
	}
	if err := manager.PrintTableData(rel1); err != nil {
		t.Error(err)
		return
	}

	if err := manager.DropColumn(rel1, "attr2"); err != nil {
		t.Error(err)
	}
	if err := manager.DropColumn(rel1, "attr1"); err != nil {
		t.Log(err)
	}
	if err := manager.AddColumn(rel2, "attrAdd", parser.AttrInfo{
		AttrInfo: types.AttrInfo{
			AttrSize:    40,
			AttrOffset:  0,
			AttrType:    types.VARCHAR,
			NullAllowed: true,
		},
		IsPrimary: false,
		AttrName:  "attrAdd",
		Default:   types.NewValueFromStr("Hi i am added"),
	}); err != nil {
		t.Error(err)
		return
	}
	if err := manager.AddColumn(rel1, "attrAdd", parser.AttrInfo{
		AttrInfo: types.AttrInfo{
			AttrSize:    40,
			AttrOffset:  0,
			AttrType:    types.VARCHAR,
			NullAllowed: true,
		},
		IsPrimary: false,
		AttrName:  "attrAdd",
		Default:   types.NewValueFromStr("Hi i am added"),
	}); err != nil {
		t.Error(err)
		return
	}

	manager.PrintTablesWithDetails()
	manager.PrintTableMeta(rel1)
	_ = manager.PrintTableData(rel1)
	manager.PrintTableMeta(rel2)
	_ = manager.PrintTableData(rel2)

	if err := manager.DropPrimaryKey(rel1); err != nil {
		t.Error(err)
		return
	}
	manager.PrintTablesWithDetails()
	manager.PrintTableMeta(rel1)
	if err := manager.UpdateRows(rel1, []string{"attr1"}, []string{"1"}, nil); err != nil {
		t.Error(err)
		return
	}

	if _, err := manager.SelectSingleTableByExpr(rel1, []string{}, nil, true); err != nil {
		t.Error(err)
		return
	}

	// delete
	if err := manager.DropDB(db1); err != nil {
		t.Error(err)
		return
	}
	return
}
