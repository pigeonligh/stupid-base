package database

import (
	"fmt"

	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

func (db *Database) solveSelect(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Select)
	if !ok {
		return errorutil.ErrorParseCommand
	}
	fmt.Println("Select:", stmt)
	// TODO
	return nil
}

func (db *Database) solveInsert(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Insert)
	if !ok {
		return errorutil.ErrorParseCommand
	}
	fmt.Println("Insert:", stmt)
	// TODO
	return nil
}

func (db *Database) solveUpdate(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Update)
	if !ok {
		return errorutil.ErrorParseCommand
	}
	fmt.Println("Update:", stmt)
	// TODO
	return nil
}

func (db *Database) solveDelete(obj sqlparser.Statement) error {
	stmt, ok := obj.(*sqlparser.Delete)
	if !ok {
		return errorutil.ErrorParseCommand
	}
	fmt.Println("Delete:", stmt)
	// TODO
	return nil
}
