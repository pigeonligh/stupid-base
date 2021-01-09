package database

import (
	"fmt"

	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

func (db *Database) solveShow(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Show)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	switch stmt.Internal.(type) {
	case *sqlparser.ShowBasic: // show databases
		db.sysManager.PrintDatabases()
	case *sqlparser.ShowLegacy: // show tables
		db.sysManager.PrintTablesWithDetails()
	}
	return nil
}

func (db *Database) solveOtherRead(sql string) error {
	fmt.Println("TODO:", sql)
	return nil
}
