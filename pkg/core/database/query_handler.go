package database

import (
	"fmt"

	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

func solveWhere(*sqlparser.Where) *parser.Expr {
	// TODO
	return nil
}

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

	where := solveWhere(stmt.Where)
	for _, tableName := range tableNames {
		_, err := db.sysManager.SelectSingleTableByExpr(tableName, attrNames, where, true)
		if err != nil {
			return err
		}
	}

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

	where := solveWhere(stmt.Where)

	for _, tableName := range tableNames {
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
		expr := solveWhere(stmt.Where)
		err := db.sysManager.DeleteRows(tableName, expr)
		if err != nil {
			return err
		}
	}
	return nil
}
