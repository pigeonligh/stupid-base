package database

import (
	"os"
	"strings"

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

func (db *Database) solveString(sql string, originError error) error {
	if strings.ToLower(sql) == "exit" {
		os.Exit(0)
	}

	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return nil
	}

	if strings.ToLower(fields[0]) == "desc" {
		if !db.sysManager.DBSelected() {
			return errorutil.ErrorDBSysDBNotSelected
		}

		if len(fields) == 2 && strings.ToLower(fields[1]) == "fk" {
			db.sysManager.PrintDBForeignInfos()
			return nil
		}

		relMap := db.sysManager.GetDBRelInfoMap()

		for i, name := range fields {
			if i == 0 {
				continue
			}
			name := strings.Trim(name, "`'\"")
			name = strings.ToLower(name)

			if _, ok := relMap[name]; ok {
				db.sysManager.PrintTableMeta(name)
			} else {
				return errorutil.ErrorDBSysRelationNotExisted
			}
		}
	}

	return originError
}
