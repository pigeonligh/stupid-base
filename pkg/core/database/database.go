/*
Copyright (c) 2020, pigeonligh.
*/

package database

import (
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/core/dbsys"
	"github.com/pigeonligh/stupid-base/pkg/core/query"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Database is the context for stupid-base
type Database struct {
	sysManager   *dbsys.Manager
	queryManager *query.Manager

	nowDatabase string
}

// New returns a database context
func New() (*Database, error) {
	return &Database{
		sysManager:   dbsys.GetInstance(),
		queryManager: query.GetInstance(),

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
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		endPosition := token.Position

		// fmt.Println(sqls)
		fmt.Println(sqlparser.String(stmt))

		var solveFunc func(sqlparser.Statement) error = nil
		var solveStringFunc func(string) error = nil
		var solveString string = ""

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

		case *sqlparser.OtherRead:
			solveStringFunc = db.solveOtherRead
			solveString = strings.Trim(sqls[startPosition:endPosition], " ")

		default:
		}

		if solveFunc != nil {
			if err := solveFunc(stmt); err != nil {
				return err
			}
		} else {
			if solveStringFunc != nil {
				if err := solveStringFunc(solveString); err != nil {
					return err
				}
			} else {
				stmtType := reflect.TypeOf(stmt)
				fmt.Println(stmtType)
				return errorutil.ErrorParseCommand
			}
		}
	}

	return nil
}
