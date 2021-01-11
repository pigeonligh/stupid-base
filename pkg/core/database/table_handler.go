package database

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/pigeonligh/stupid-base/pkg/core/dbsys"
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

func columnDefinitionToAttrInfo(col *sqlparser.ColumnDefinition, tableName string) parser.AttrInfo {
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
	return attr
}

func (db *Database) addIndexDefinition(
	index *sqlparser.IndexDefinition,
	tableName string,
) error {
	names := []string{}
	for _, col := range index.Columns {
		names = append(names, col.Column.CompliantName())
	}
	if index.Info.Primary {
		err := db.sysManager.AddPrimaryKey(tableName, names)
		if err != nil {
			return err
		}
	} else {
		indexName := index.Info.Name.CompliantName()
		err := db.sysManager.CreateIndex(indexName, tableName, names, !index.Info.Unique)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) addConstraintDefinition(
	constraint *sqlparser.ConstraintDefinition,
	tableName string,
) error {
	keyName := constraint.Name
	if foreign, ok := constraint.Details.(*sqlparser.ForeignKeyDefinition); ok {
		src := []string{}
		dst := []string{}
		for i := range foreign.Source {
			src = append(src, foreign.Source[i].CompliantName())
			dst = append(dst, foreign.ReferencedColumns[i].CompliantName())
		}
		tab := foreign.ReferencedTable.Name.CompliantName()
		err := db.sysManager.AddForeignKey(keyName, tableName, src, tab, dst)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) solveCreateTable(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.CreateTable)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	tableName := stmt.Table.Name.CompliantName()

	attrList := []parser.AttrInfo{}
	for _, col := range stmt.TableSpec.Columns {
		attr := columnDefinitionToAttrInfo(col, tableName)
		attrList = append(attrList, attr)
	}

	err := db.sysManager.CreateTable(stmt.Table.Name.CompliantName(), attrList)
	if err != nil {
		return err
	}

	for _, index := range stmt.TableSpec.Indexes {
		if err := db.addIndexDefinition(index, tableName); err != nil {
			return err
		}

	}
	for _, constraint := range stmt.TableSpec.Constraints {
		if err := db.addConstraintDefinition(constraint, tableName); err != nil {
			return err
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

	tableName := stmt.Table.Name.CompliantName()
	for _, option := range stmt.AlterOptions {
		fmt.Println(reflect.TypeOf(option))
		switch option.(type) {
		case *sqlparser.AddColumns:
			if act, ok := option.(*sqlparser.AddColumns); ok {
				for _, col := range act.Columns {
					attr := columnDefinitionToAttrInfo(col, tableName)
					err := db.sysManager.AddColumn(tableName, col.Name.CompliantName(), attr)
					if err != nil {
						return err
					}
				}
				return nil
			}

		case *sqlparser.RenameTable:
			if act, ok := option.(*sqlparser.RenameTable); ok {
				err := db.sysManager.RenameTable(tableName, act.Table.Name.CompliantName())
				if err != nil {
					return err
				}
				return nil
			}

		case *sqlparser.DropColumn:
			if act, ok := option.(*sqlparser.DropColumn); ok {
				err := db.sysManager.DropColumn(tableName, act.Name.Name.CompliantName())
				if err != nil {
					return err
				}
				return nil
			}

		case *sqlparser.AddConstraintDefinition:
			if act, ok := option.(*sqlparser.AddConstraintDefinition); ok {
				if err := db.addConstraintDefinition(act.ConstraintDefinition, tableName); err != nil {
					return err
				}
				return nil
			}

		case *sqlparser.DropKey:
			if act, ok := option.(*sqlparser.DropKey); ok {
				switch act.Type {
				case sqlparser.ForeignKeyType:
					if err := db.sysManager.DropForeignKey(act.Name); err != nil {
						return err
					}
				case sqlparser.NormalKeyType:
					if err := db.sysManager.DropIndex(tableName, act.Name); err != nil {
						return err
					}
				case sqlparser.PrimaryKeyType:
					if err := db.sysManager.DropPrimaryKey(tableName); err != nil {
						return err
					}
				}
				return nil
			}

		case *sqlparser.AddIndexDefinition:
			if act, ok := option.(*sqlparser.AddIndexDefinition); ok {
				if err := db.addIndexDefinition(act.IndexDefinition, tableName); err != nil {
					return err
				}
				return nil
			}

		case *sqlparser.ChangeColumn:
			if act, ok := option.(*sqlparser.ChangeColumn); ok {
				colName := act.OldColumn.Name.CompliantName()
				col := act.NewColDefinition
				attr := columnDefinitionToAttrInfo(col, tableName)

				changeField := dbsys.ChangeDefault | dbsys.ChangeNull | dbsys.ChangeValueType

				err := db.sysManager.ChangeColumn(tableName, colName, &attr, changeField)
				if err != nil {
					return err
				}
				return nil
			}

		case *sqlparser.AlgorithmValue:
		case *sqlparser.AlterCharset:
		case *sqlparser.LockOption:
		case *sqlparser.AlterColumn:
		case *sqlparser.Force:
		case *sqlparser.KeyState:
		case *sqlparser.ModifyColumn:
		case *sqlparser.OrderByOption:
		case *sqlparser.RenameIndex:
		case *sqlparser.TableOptions:
		case *sqlparser.TablespaceOperation:
		case *sqlparser.Validation:
		}
	}

	fmt.Println("unsolve")
	return nil
}
