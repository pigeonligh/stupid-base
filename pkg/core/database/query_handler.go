package database

import (
	"fmt"

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

func solveWhere(expr sqlparser.Expr, attrs dbsys.AttrInfoList, tableName string) (*parser.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	result, ambiguity, err := splitExprForUnionQuery(expr, attrs, tableName)
	if err != nil {
		return nil, err
	}
	if !ambiguity {
		return result, nil
	}
	return parser.NewExprConst(types.NewValueFromBool(true)), nil
}

func (db *Database) solveSelect(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Select)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	tableNames := []string{}
	attrNames := []string{}

	for _, expr := range stmt.From {
		if ate, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			table, _ := ate.TableName()
			tableName := table.Name.CompliantName()
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
				attrNames = append(attrNames, col.Name.CompliantName())
			}
		}
	}

	tables := []*dbsys.TemporalTable{}
	allAttrs := dbsys.AttrInfoList{}

	selectedAttrs := map[string]dbsys.AttrInfoList{}

	for _, tableName := range tableNames {
		// attrs := db.sysManager.GetAttrInfoList(tableName)
		attrs := dbsys.AttrInfoList{}
		where, err := solveWhere(stmt.Where.Expr, attrs, tableName)
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

	findFn := func(string) *parser.AttrInfo {
		return nil
	}
	for _, attrName := range attrNames {
		attr := findFn(attrName)
		if attr != nil {
			if _, ok := selectedAttrs[attr.RelName]; !ok {
				selectedAttrs[attr.RelName] = dbsys.AttrInfoList{}
			}
			selectedAttrs[attr.RelName] = append(selectedAttrs[attr.RelName], *attr)
		}
	}

	where, err := solveWhere(stmt.Where.Expr, allAttrs, "")
	if err != nil {
		return err
	}

	fmt.Println(where)
	/*
		err := db.sysManager.UnionQuery(tables, selectedAttrs, where)
		if err != nil {
			return err
		}
	*/

	return nil
}

func (db *Database) solveInsert(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Insert)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	tableName := stmt.Table.Name.CompliantName()
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

	for _, expr := range stmt.TableExprs {
		if ate, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			table, _ := ate.TableName()
			tableName := table.Name.CompliantName()
			tableNames = append(tableNames, tableName)
		}
	}

	for _, expr := range stmt.Exprs {
		attrNames = append(attrNames, expr.Name.Name.CompliantName())
		attrValues = append(attrValues, exprToString(expr.Expr))
	}

	for _, tableName := range tableNames {
		attrs := db.sysManager.GetAttrInfoList(tableName)
		where, err := solveWhere(stmt.Where.Expr, attrs, tableName)
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

	for _, expr := range stmt.TableExprs {
		if ate, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			table, _ := ate.TableName()
			tableName := table.Name.CompliantName()
			tableNames = append(tableNames, tableName)
		}
	}

	for _, tableName := range tableNames {
		attrs := db.sysManager.GetAttrInfoList(tableName)
		where, err := solveWhere(stmt.Where.Expr, attrs, tableName)
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
