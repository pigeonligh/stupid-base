/*
Copyright (c) 2020, pigeonligh.
*/

package core

import (
	"fmt"
	"reflect"

	"github.com/xwb1989/sqlparser"
)

// Database is the context for stupid-base
type Database struct {
	//
}

// NewDatabase returns a database context
func NewDatabase() (*Database, error) {
	return &Database{
		//
	}, nil
}

// Run runs the database
func (db *Database) Run(sql string) {
	tree, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("%v: %v\n", reflect.TypeOf(tree), sqlparser.String(tree))
}
