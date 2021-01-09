package core

import (
	"fmt"

	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

func (db *Database) solveUse(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Use)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	fmt.Println(stmt.DBName)
	return nil
}

func (db *Database) solveDBDDL(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.DBDDL)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	fmt.Println(stmt.Action, stmt.DBName)
	return nil
}

func (db *Database) solveDDL(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.DDL)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	fmt.Println(sqlparser.String(stmt))
	fmt.Println(stmt.Action, stmt.Table.Name)
	fmt.Println(stmt)
	return nil
}
