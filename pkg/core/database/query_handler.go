package database

import (
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/core/dbsys"
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

	for _, expr := range stmt.SelectExprs {
		switch expr.(type) {
		case *sqlparser.StarExpr:
			attrNames = nil
			break
		case *sqlparser.AliasedExpr:
			ae := expr.(*sqlparser.AliasedExpr)
			if col, ok := ae.Expr.(*sqlparser.ColName); ok {
				attrTable := col.Qualifier.Name.CompliantName()
				attrName := col.Name.CompliantName()
				attrTables = append(attrTables, strings.ToLower(attrTable))
				attrNames = append(attrNames, strings.ToLower(attrName))
			}
		}
	}

	tables := []*dbsys.TemporalTable{}
	allAttrs := dbsys.AttrInfoList{}

	selectedAttrs := map[string]dbsys.AttrInfoList{}

	for _, tableName := range tableNames {
		attrs := db.sysManager.GetAttrInfoList(tableName)
		where, err := parser.SolveWhere(stmt.Where, attrs, tableName)
		if err != nil {
			return err
		}

		table, err := db.sysManager.SelectSingleTableByExpr(tableName, nil, where, false)
		if err != nil {
			return err
		}

		tables = append(tables, table)
		allAttrs = append(allAttrs, attrs...)
	}

	if attrNames == nil {
		// select *
		for _, attr := range allAttrs {
			if _, ok := selectedAttrs[attr.RelName]; !ok {
				selectedAttrs[attr.RelName] = dbsys.AttrInfoList{}
			}
			selectedAttrs[attr.RelName] = append(selectedAttrs[attr.RelName], attr)
		}
	} else {
		for index, attrName := range attrNames {
			attr, err := parser.GetAttrFromList(allAttrs, attrTables[index], attrName)
			if err != nil {
				return err
			}
			if attr != nil {
				if _, ok := selectedAttrs[attr.RelName]; !ok {
					selectedAttrs[attr.RelName] = dbsys.AttrInfoList{}
				}
				selectedAttrs[attr.RelName] = append(selectedAttrs[attr.RelName], *attr)
			}
		}
	}

	where, err := parser.SolveWhere(stmt.Where, allAttrs, "")
	if err != nil {
		return err
	}

	err = db.sysManager.SelectFromMultiple(tables, selectedAttrs, where)
	if err != nil {
		return err
	}

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
		where, err := parser.SolveWhere(stmt.Where, attrs, tableName)
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
		where, err := parser.SolveWhere(stmt.Where, attrs, tableName)
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
