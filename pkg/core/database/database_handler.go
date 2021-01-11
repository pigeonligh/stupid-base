package database

import (
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

func (db *Database) solveUse(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Use)
	if !ok {
		return errorutil.ErrorParseCommand
	}
	if len(db.nowDatabase) > 0 {
		if err := db.sysManager.CloseDB(db.nowDatabase); err != nil {
			return err
		}
	}

	dbName := stmt.DBName.CompliantName()
	dbName = strings.ToLower(dbName)
	if err := db.sysManager.OpenDB(dbName); err != nil {
		return err
	}
	db.nowDatabase = dbName

	return nil
}

func (db *Database) solveCreateDatabase(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.CreateDatabase)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	return db.sysManager.CreateDB(stmt.DBName)
}

func (db *Database) solveDropDatabase(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.DropDatabase)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	return db.sysManager.DropDB(stmt.DBName)
}
