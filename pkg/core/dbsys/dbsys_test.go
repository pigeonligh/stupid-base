package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"testing"
)

func TestDbSys(t *testing.T) {

	log.SetLevel(log.RecordLevel | log.StorageLevel | log.ExprLevel | log.DbSysLevel)

	manager := GetInstance()

	db1 := "testdb_1"
	db2 := "testdb_2"
	db3 := "testdb_3"

	if err := manager.CreateDb(db1); err != nil {
		t.Error(err)
		return
	}
	//if err := manager.CreateDb(db2); err != nil {
	//	t.Error(err)
	//	return
	//}
	//if err := manager.CreateDb(db3); err != nil {
	//	t.Error(err)
	//	return
	//}
	manager.ShowDatabases()
	if err := manager.OpenDb(db1); err != nil {
		t.Error(err)
		return
	}
	manager.ShowTables()

	rel1 := "rel1"
	//rel2 = "rel2"
	//rel3 = "rel3"
	attrInfoList := []parser.AttrInfo{
		{
			AttrName:      strTo24ByteArray("attr1"),
			RelName:       strTo24ByteArray(rel1),
			AttrSize:      8,
			AttrType:      types.INT,
			IndexNo:       0,
			NullAllowed:   false,
			IsPrimary:     false,
			AutoIncrement: false,
		},
		{
			AttrName:      strTo24ByteArray("attr2"),
			RelName:       strTo24ByteArray(rel1),
			AttrSize:      8,
			AttrType:      types.FLOAT,
			IndexNo:       0,
			NullAllowed:   true,
			IsPrimary:     false,
			AutoIncrement: false,
		},
		{
			AttrName: strTo24ByteArray("attr3"),
			RelName:  strTo24ByteArray(rel1),
			AttrSize: 24,
			AttrType: types.STRING,
			IndexNo:  0,
		},
	}
	if err := manager.CreateTable(rel1, attrInfoList, []ConstraintInfo{}); err != nil {
		t.Error(err)
		return
	}
	if err := manager.DescribeTable(rel1); err != nil {
		t.Error(err)
		return
	}

	// delete
	if err := manager.DropDb(db1); err != nil {
		t.Error(err)
		return
	}
	if err := manager.DropDb(db2); err != nil {
		t.Error(err)
		return
	}
	if err := manager.DropDb(db3); err != nil {
		t.Error(err)
		return
	}
}
