/*
Copyright (c) 2020, pigeonligh.
*/

package database

import (
	"io"
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/core/dbsys"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Database is the context for stupid-base
type Database struct {
	sysManager *dbsys.Manager

	nowDatabase string
}

// New returns a database context
func New() (*Database, error) {
	return &Database{
		sysManager: dbsys.GetInstance(),

		nowDatabase: "",
	}, nil
}

// Run runs the database
func (db *Database) Run(sqls string) error {
	reader := strings.NewReader(sqls)
	token := sqlparser.NewTokenizer(reader)

	for {
		startPosition := token.Position
		stmt, err := sqlparser.ParseNext(token)
		endPosition := token.Position
		if err == io.EOF {
			break
		}
		if err != nil {
			sql := strings.Trim(sqls[startPosition:endPosition-1], " \n;")
			err = db.solveString(sql, err)
		}
		if err != nil {
			return err
		}

		var solveFunc func(sqlparser.Statement) error = nil

		switch stmt.(type) {
		case *sqlparser.CreateDatabase:
			solveFunc = db.solveCreateDatabase

		case *sqlparser.DropDatabase:
			solveFunc = db.solveDropDatabase

		case *sqlparser.CreateTable:
			solveFunc = db.solveCreateTable

		case *sqlparser.DropTable:
			solveFunc = db.solveDropTable

		case *sqlparser.AlterTable:
			solveFunc = db.solveAlterTable

		case *sqlparser.Select:
			solveFunc = db.solveSelect

		case *sqlparser.Insert:
			solveFunc = db.solveInsert

		case *sqlparser.Update:
			solveFunc = db.solveUpdate

		case *sqlparser.Delete:
			solveFunc = db.solveDelete

		case *sqlparser.Use:
			solveFunc = db.solveUse

		case *sqlparser.Show:
			solveFunc = db.solveShow

		default:
		}

		if solveFunc != nil {
			if err := solveFunc(stmt); err != nil {
				return err
			}
		} else {
			sql := strings.Trim(sqls[startPosition:endPosition-1], " \n;")
			if err := db.solveString(sql, nil); err != nil {
				return err
			}
		}
	}

	return nil
}
