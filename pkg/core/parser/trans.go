package parser

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

var compOpTrans = map[sqlparser.ComparisonExprOperator]types.OpType{
	sqlparser.EqualOp:        types.OpCompEQ,
	sqlparser.LessThanOp:     types.OpCompLT,
	sqlparser.GreaterThanOp:  types.OpCompGT,
	sqlparser.LessEqualOp:    types.OpCompLE,
	sqlparser.GreaterEqualOp: types.OpCompGE,
	sqlparser.NotEqualOp:     types.OpCompNE,
	sqlparser.LikeOp:         types.OpCompLIKE,
	sqlparser.NotLikeOp:      types.OpCompNOTLIKE,

	// sqlparser.RegexpOp:        0,
	// sqlparser.NotRegexpOp:     0,
	// sqlparser.InOp:            0,
	// sqlparser.NotInOp:         0,
	// sqlparser.NullSafeEqualOp: 0,
}

func GetAttrFromList(attrs AttrInfoList, tableName, colName string) (*AttrInfo, error) {
	var result *AttrInfo = nil
	for index, attr := range attrs {
		if attr.AttrName == colName && (tableName == "" || tableName == attr.RelName) {
			if result != nil {
				return nil, errorutil.ErrorColDuplicated
			}
			result = &attrs[index]
		}
	}
	if result == nil {
		return nil, errorutil.ErrorColNotFound
	}
	return result, nil
}

func splitExprForUnionQuery(
	expr sqlparser.Expr,
	attrs AttrInfoList,
	tableName string,
) (*Expr, bool, error) {
	switch expr.(type) {
	case *sqlparser.AndExpr:
		expr := expr.(*sqlparser.AndExpr)

		lexpr, lambiguity, err := splitExprForUnionQuery(expr.Left, attrs, tableName)
		if err != nil {
			return nil, false, err
		}
		rexpr, rambiguity, err := splitExprForUnionQuery(expr.Right, attrs, tableName)
		if err != nil {
			return nil, false, err
		}

		if lambiguity && rambiguity {
			return nil, true, nil
		}
		if lambiguity {
			return rexpr, false, nil
		}
		if rambiguity {
			return lexpr, false, nil
		}
		return NewExprLogic(lexpr, types.OpLogicAND, rexpr), false, nil

	case *sqlparser.OrExpr:
		expr := expr.(*sqlparser.OrExpr)

		lexpr, lambiguity, err := splitExprForUnionQuery(expr.Left, attrs, tableName)
		if err != nil {
			return nil, false, err
		}
		rexpr, rambiguity, err := splitExprForUnionQuery(expr.Right, attrs, tableName)
		if err != nil {
			return nil, false, err
		}

		if lambiguity && rambiguity {
			return nil, true, nil
		}
		if lambiguity {
			return rexpr, false, nil
		}
		if rambiguity {
			return lexpr, false, nil
		}
		return NewExprLogic(lexpr, types.OpLogicOR, rexpr), false, nil

	case *sqlparser.NotExpr:
		expr := expr.(*sqlparser.NotExpr)

		subexpr, ambiguity, err := splitExprForUnionQuery(expr.Expr, attrs, tableName)
		if err != nil {
			return nil, false, err
		}

		if ambiguity {
			return nil, true, nil
		}
		return NewExprLogic(nil, types.OpLogicNOT, subexpr), false, nil

	case *sqlparser.ComparisonExpr:
		expr := expr.(*sqlparser.ComparisonExpr)

		lexpr, lambiguity, err := splitExprForUnionQuery(expr.Left, attrs, tableName)
		if err != nil {
			return nil, false, err
		}
		rexpr, rambiguity, err := splitExprForUnionQuery(expr.Right, attrs, tableName)
		if err != nil {
			return nil, false, err
		}

		if lambiguity || rambiguity {
			return nil, true, nil
		}

		op, ok := compOpTrans[expr.Operator]
		if !ok {
			return nil, false, errorutil.ErrorOpNotFound
		}
		return NewExprComp(lexpr, op, rexpr), false, nil

	case *sqlparser.Literal:
		value := expr.(*sqlparser.Literal)
		if value == nil {
			ret := NewExprConst(types.NewValueFromEmpty())
			ret.IsNull = true
			return ret, false, nil
		}
		return NewExprConst(types.NewValueFromStr(string(value.Val))), false, nil

	case *sqlparser.ColName:
		col := expr.(*sqlparser.ColName)
		colTable := strings.ToLower(col.Qualifier.Name.CompliantName())
		colName := strings.ToLower(col.Name.CompliantName())

		attr, err := GetAttrFromList(attrs, colTable, colName)

		if err == errorutil.ErrorColNotFound && tableName != "" {
			return nil, true, nil
		}

		if err != nil {
			return nil, false, err
		}

		if tableName != "" && attr.RelName != tableName {
			return nil, true, nil
		}

		return NewExprAttr(*attr), false, nil

	default:
		fmt.Println(reflect.TypeOf(expr))
	}
	return nil, true, nil
}

func PrintExpr(expr *Expr) {
	switch expr.NodeType {
	case types.NodeArith:

	case types.NodeComp:
		PrintExpr(expr.Left)
		PrintExpr(expr.Right)
		fmt.Println("compare", expr.OpType)

	case types.NodeLogic:
		if expr.OpType == types.OpLogicNOT {
			PrintExpr(expr.Right)
			fmt.Println("not")
		} else {
			PrintExpr(expr.Left)
			PrintExpr(expr.Right)
			fmt.Println("logic", expr.OpType)
		}

	case types.NodeConst:
		fmt.Println(string(expr.Value.Value[:]))

	case types.NodeAttr:
		fmt.Println(expr.AttrInfo.AttrName)

	}
}

func SolveWhere(where *sqlparser.Where, attrs AttrInfoList, tableName string) (*Expr, error) {
	if where == nil {
		return NewExprConst(types.NewValueFromBool(true)), nil
	}
	result, ambiguity, err := splitExprForUnionQuery(where.Expr, attrs, tableName)
	if err != nil {
		return nil, err
	}
	if !ambiguity {
		// PrintExpr(result)
		return result, nil
	}
	return NewExprConst(types.NewValueFromBool(true)), nil
}
