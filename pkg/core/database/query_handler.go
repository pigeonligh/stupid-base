package database

import (
	"fmt"
	"reflect"

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

func (db *Database) solveWhere(expr sqlparser.Expr, attrs dbsys.AttrInfoList, tableName string) *parser.Expr {
	if expr == nil {
		return nil
	}
	if expr, ok := expr.(*sqlparser.ComparisonExpr); ok {
		fmt.Println(expr.Operator.ToString())
		fmt.Println(reflect.TypeOf(expr.Left))
		fmt.Println(reflect.TypeOf(expr.Right))
	}
	return nil
}

func (db *Database) solveSelect(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Select)
	if !ok {
		return errorutil.ErrorParseCommand
	}
	fmt.Println("Select:", stmt)

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

	for _, tableName := range tableNames {
		attrs := db.sysManager.GetAttrInfoList(tableName)
		where := db.solveWhere(stmt.Where.Expr, attrs, tableName)

		table, err := db.sysManager.SelectSingleTableByExpr(tableName, nil, where, false)
		if err != nil {
			return err
		}

		tables = append(tables, table)
		allAttrs = append(allAttrs, attrs...)
	}

	where := db.solveWhere(stmt.Where.Expr, allAttrs, "")
	fmt.Println(where)
	/*
		err := db.sysManager.UnionQuery(tables, attrNames, where)
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
		where := db.solveWhere(stmt.Where.Expr, attrs, tableName)

		err := db.sysManager.UpdateRows(tableName, attrNames, attrValues, where)
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

	for _, target := range stmt.Targets {
		tableName := target.Name.CompliantName()
		attrs := db.sysManager.GetAttrInfoList(tableName)
		where := db.solveWhere(stmt.Where.Expr, attrs, tableName)

		err := db.sysManager.DeleteRows(tableName, where)
		if err != nil {
			return err
		}
	}
	return nil
}
