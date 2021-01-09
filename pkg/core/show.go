package core

import (
	"fmt"
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

func (db *Database) solveShow(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Show)
	if !ok {
		return errorutil.ErrorParseCommand
	}

	fmt.Println(stmt.Type)
	switch strings.ToLower(stmt.Type) {
	case sqlparser.KeywordString(sqlparser.DATABASES):
		fmt.Println("TODO: show databses")
	case sqlparser.KeywordString(sqlparser.TABLES):
		fmt.Println("TODO: show tables")
		fmt.Println(stmt)
	default:
		return errorutil.ErrorUnknownCommand
	}
	return nil
}

func (db *Database) solveOtherRead(sql string) error {
	fmt.Println("TODO:", sql)
	return nil
}
