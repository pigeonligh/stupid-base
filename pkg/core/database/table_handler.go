package database

import (
	"fmt"
	"strconv"

	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

func (db *Database) solveCreateTable(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.CreateTable)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	tableName := stmt.Table.Name.CompliantName()

	attrList := []parser.AttrInfo{}
	for _, col := range stmt.TableSpec.Columns {
		var attr parser.AttrInfo

		attr.AttrName = col.Name.CompliantName()
		attr.RelName = tableName
		attr.NullAllowed = !col.Type.NotNull
		attr.IsPrimary = false
		attr.AttrType = types.GetValueType(col.Type.Type)
		if col.Type.Length == nil {
			attr.AttrSize = types.ValueTypeDefaultSize[attr.AttrType]
		} else {
			s := string(col.Type.Length.Val)
			attr.AttrSize, _ = strconv.Atoi(s)
		}

		if col.Type.Default == nil {
			attr.Default.ValueType = types.NO_ATTR
		} else {
			attr.Default.ValueType = attr.AttrType
			value := col.Type.Default.(*sqlparser.Literal)
			copy(attr.Default.Value[:], value.Val[:])
		}
		attrList = append(attrList, attr)
	}

	err := db.sysManager.CreateTable(stmt.Table.Name.CompliantName(), attrList)
	if err != nil {
		return err
	}

	for _, index := range stmt.TableSpec.Indexes {
		names := []string{}
		for _, col := range index.Columns {
			names = append(names, col.Column.CompliantName())
		}
		if index.Info.Primary {
			err := db.sysManager.AddPrimaryKey(tableName, names)
			if err != nil {
				return err
			}
		}
		err := db.sysManager.CreateIndex("", tableName, names, !index.Info.Unique)
		if err != nil {
			return err
		}
	}
	for _, constraint := range stmt.TableSpec.Constraints {
		if foreign, ok := constraint.Details.(*sqlparser.ForeignKeyDefinition); ok {
			src := []string{}
			dst := []string{}
			for i, _ := range foreign.Source {
				src = append(src, foreign.Source[i].CompliantName())
				dst = append(dst, foreign.ReferencedColumns[i].CompliantName())
			}
			tab := foreign.ReferencedTable.Name.CompliantName()
			err := db.sysManager.AddForeignKey("", tableName, src, tab, dst)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *Database) solveDropTable(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.DropTable)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	for _, table := range stmt.FromTables {
		if err := db.sysManager.DropTable(table.Name.CompliantName()); err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) solveAlterTable(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.AlterTable)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	fmt.Println("TODO:", stmt)

	return nil
}
