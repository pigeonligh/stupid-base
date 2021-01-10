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
	case *sqlparser.AndExpr:
		expr := expr.(*sqlparser.AndExpr)

		splitExprForUnionQuery(expr.Left, attrs, tableName)
		splitExprForUnionQuery(expr.Right, attrs, tableName)

		fmt.Println("and")

	case *sqlparser.OrExpr:
		expr := expr.(*sqlparser.OrExpr)

		splitExprForUnionQuery(expr.Left, attrs, tableName)
		splitExprForUnionQuery(expr.Right, attrs, tableName)

		fmt.Println("or")

	case *sqlparser.NotExpr:
		expr := expr.(*sqlparser.NotExpr)

		splitExprForUnionQuery(expr.Expr, attrs, tableName)

		fmt.Println("not")

	case *sqlparser.ComparisonExpr:
		expr := expr.(*sqlparser.ComparisonExpr)

		splitExprForUnionQuery(expr.Left, attrs, tableName)
		splitExprForUnionQuery(expr.Right, attrs, tableName)

		fmt.Println(expr.Operator.ToString())

	case *sqlparser.Literal:
		value := expr.(*sqlparser.Literal)
		fmt.Println("val", string(value.Val))

	case *sqlparser.ColName:
		col := expr.(*sqlparser.ColName)
		fmt.Println("col", col.Name.CompliantName())

	default:
		fmt.Println(reflect.TypeOf(expr))
	}
	return nil, true, nil
}
