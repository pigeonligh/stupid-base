package database

import (
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

func exprToString(expr sqlparser.Expr) string {
	if value, ok := expr.(*sqlparser.Literal); ok {
		if value == nil {
			return types.MagicNullString
		} else {
			return string(value.Val)
		}
	}
	// parse failed, treat as NULL
	return types.MagicNullString
}

func (db *Database) solveSelect(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Select)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	tableNames := []string{}
	attrTables := []string{}
	attrNames := []string{}
	attrFuncs := []types.ClusterType{}

	if !db.sysManager.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	relMap := db.sysManager.GetDBRelInfoMap()

	for _, expr := range stmt.From {
		if ate, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			table, _ := ate.TableName()
			tableName := table.Name.CompliantName()
			tableName = strings.ToLower(tableName)

			if _, ok := relMap[tableName]; !ok {
				return errorutil.ErrorDBSysRelationNotExisted
			}

			tableNames = append(tableNames, tableName)
		}
	}

LOOP:
	for _, expr := range stmt.SelectExprs {
		switch expr.(type) {
		case *sqlparser.StarExpr:
			attrTables = nil
			attrNames = nil
			attrFuncs = nil
			break LOOP
		case *sqlparser.AliasedExpr:
			ae := expr.(*sqlparser.AliasedExpr)
			switch ae.Expr.(type) {
			case *sqlparser.ColName:
				col := ae.Expr.(*sqlparser.ColName)
				attrTable := col.Qualifier.Name.CompliantName()
				attrName := col.Name.CompliantName()
				attrTables = append(attrTables, strings.ToLower(attrTable))
				attrNames = append(attrNames, strings.ToLower(attrName))
				attrFuncs = append(attrFuncs, types.NoneCluster)
			case *sqlparser.FuncExpr:
				fun := ae.Expr.(*sqlparser.FuncExpr)
				funcType := types.NoneCluster
				switch fun.Name.Lowered() {
				case "min":
					funcType = types.MinCluster
				case "max":
					funcType = types.MaxCluster
				case "sum":
					funcType = types.SumCluster
				case "avg":
					funcType = types.AverageCluster
				default:
					return errorutil.ErrorUndefinedBehaviour
				}

				for _, fexpr := range fun.Exprs {
					if fae, ok := fexpr.(*sqlparser.AliasedExpr); ok {
						if col, ok := fae.Expr.(*sqlparser.ColName); ok {
							attrTable := col.Qualifier.Name.CompliantName()
							attrName := col.Name.CompliantName()
							attrTables = append(attrTables, strings.ToLower(attrTable))
							attrNames = append(attrNames, strings.ToLower(attrName))
							attrFuncs = append(attrFuncs, funcType)
						} else {
							return errorutil.ErrorUndefinedBehaviour
						}
					} else {
						return errorutil.ErrorUndefinedBehaviour
					}
				}
			default:
				return errorutil.ErrorUndefinedBehaviour
			}
		}
	}

	table, err := db.sysManager.SelectTablesByWhereExpr(tableNames, attrTables, attrNames, attrFuncs, stmt.Where)

	if err != nil {
		return err
	}

	db.sysManager.PrintTemporalTable(table)
	return nil
}

func (db *Database) solveInsert(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Insert)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	if !db.sysManager.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	relMap := db.sysManager.GetDBRelInfoMap()

	tableName := stmt.Table.Name.CompliantName()
	tableName = strings.ToLower(tableName)

	if _, ok := relMap[tableName]; !ok {
		return errorutil.ErrorDBSysRelationNotExisted
	}

	values := stmt.Rows.(sqlparser.Values)

	for _, tuple := range values {
		list := []string{}
		for _, expr := range tuple {
			list = append(list, exprToString(expr))
		}

		err := db.sysManager.InsertRow(tableName, list)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) solveUpdate(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Update)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	tableNames := []string{}
	attrNames := []string{}
	attrValues := []string{}

	if !db.sysManager.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	relMap := db.sysManager.GetDBRelInfoMap()

	for _, expr := range stmt.TableExprs {
		if ate, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			table, _ := ate.TableName()
			tableName := table.Name.CompliantName()
			tableName = strings.ToLower(tableName)

			if _, ok := relMap[tableName]; !ok {
				return errorutil.ErrorDBSysRelationNotExisted
			}

			tableNames = append(tableNames, tableName)
		}
	}

	for _, expr := range stmt.Exprs {
		attrName := expr.Name.Name.CompliantName()
		attrNames = append(attrNames, strings.ToLower(attrName))
		attrValues = append(attrValues, exprToString(expr.Expr))
	}

	for _, tableName := range tableNames {
		attrs := db.sysManager.GetAttrInfoList(tableName)
		where, err := parser.SolveWhere(stmt.Where, attrs, tableName, nil)
		if err != nil {
			return err
		}

		err = db.sysManager.UpdateRows(tableName, attrNames, attrValues, where)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) solveDelete(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Delete)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	tableNames := []string{}

	if !db.sysManager.DBSelected() {
		return errorutil.ErrorDBSysDBNotSelected
	}
	relMap := db.sysManager.GetDBRelInfoMap()

	for _, expr := range stmt.TableExprs {
		if ate, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			table, _ := ate.TableName()
			tableName := table.Name.CompliantName()
			tableName = strings.ToLower(tableName)

			if _, ok := relMap[tableName]; !ok {
				return errorutil.ErrorDBSysRelationNotExisted
			}

			tableNames = append(tableNames, tableName)
		}
	}

	for _, tableName := range tableNames {
		attrs := db.sysManager.GetAttrInfoList(tableName)
		where, err := parser.SolveWhere(stmt.Where, attrs, tableName, nil)
		if err != nil {
			return err
		}

		err = db.sysManager.DeleteRows(tableName, where)
		if err != nil {
			return err
		}
	}
	return nil
}
