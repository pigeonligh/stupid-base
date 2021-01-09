package database

import (
	"fmt"

	"github.com/pigeonligh/stupid-base/pkg/core/types"
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

	tableName := stmt.Table.Name.CompliantName()

	values := stmt.Rows.(sqlparser.Values)

	for _, tuple := range values {
		valueList := []types.Value{}

		for _, expr := range tuple {
			value, ok := expr.(*sqlparser.Literal)
			if !ok {
				// Error
			}

			var val types.Value
			val.ValueType = types.NO_ATTR
			copy(val.Value[:], value.Val[:])
			// TODO: the type

			valueList = append(valueList, val)
		}

		err := db.sysManager.InsertRow(tableName, valueList)
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
