package database

import (
	"fmt"
	"reflect"

	"github.com/pigeonligh/stupid-base/pkg/core/dbsys"
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"vitess.io/vitess/go/vt/sqlparser"
)

func splitExprForUnionQuery(
	expr sqlparser.Expr,
	attrs dbsys.AttrInfoList,
	tableName string,
) (*parser.Expr, bool, error) {
	switch expr.(type) {
	case *sqlparser.ComparisonExpr:
	case *sqlparser.Literal:
	}
	if expr, ok := expr.(*sqlparser.ComparisonExpr); ok {
		fmt.Println(expr.Operator.ToString())
		fmt.Println(reflect.TypeOf(expr.Left))
		fmt.Println(reflect.TypeOf(expr.Right))
	}
	return nil, true, nil
}
