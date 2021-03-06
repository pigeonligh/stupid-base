package parser

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

type AttrInfo struct {
	types.AttrInfo
	IsPrimary bool // used by system manager
	IndexName string
	FkName    string
	AttrName  string
	RelName   string
	Default   types.Value
}

type AttrInfoList []AttrInfo

type Expr struct {
	Left  *Expr
	Right *Expr

	NodeType types.NodeType
	OpType   types.OpType

	Value    types.Value // represent basic Value which can provide convert functions
	AttrInfo AttrInfo    // represent Value as attr in a db/table

	//
	IsNull       bool
	IsCalculated bool
}

func NewExprEmpty() *Expr {
	return &Expr{
		IsNull:       false,
		IsCalculated: false,
		Value:        types.Value{ValueType: types.NO_ATTR},
	}
}

// no type check will be conducted during construct
func NewExprCompQuickAttrCompValue(size, off int, compOp types.OpType, value types.Value) *Expr {
	node1l := NewExprAttr(AttrInfo{
		AttrInfo: types.AttrInfo{
			AttrSize:   size,
			AttrOffset: off,
			AttrType:   value.ValueType,
		},
	})
	node1r := NewExprConst(value)
	node1 := NewExprComp(node1l, compOp, node1r)
	return node1
}

// attr will always be appears with
func NewExprAttr(attr AttrInfo) *Expr {
	return &Expr{
		NodeType:     types.NodeAttr,
		AttrInfo:     attr,
		IsNull:       false,
		IsCalculated: false,
	}
}

func NewExprConst(value types.Value) *Expr {
	expr := Expr{
		NodeType:     types.NodeConst,
		OpType:       types.OpDefault,
		Value:        value,
		IsNull:       false,
		IsCalculated: true,
	}
	if value.ValueType == types.NO_ATTR {
		expr.IsNull = true
	}
	return &expr
}

func NewExprComp(l *Expr, op types.OpType, r *Expr) *Expr {
	if r.NodeType == types.NodeAttr {
		l, r = r, l
		switch op {
		case types.OpCompLT:
			op = types.OpCompGT
		case types.OpCompGT:
			op = types.OpCompLT
		case types.OpCompLE:
			op = types.OpCompGE
		case types.OpCompGE:
			op = types.OpCompLE
		}
	}

	return &Expr{
		Left:         l,
		Right:        r,
		NodeType:     types.NodeComp,
		OpType:       op,
		Value:        types.Value{},
		IsNull:       false,
		IsCalculated: false,
	}
}

func NewExprLogic(l *Expr, op types.OpType, r *Expr) *Expr {
	return &Expr{
		Left:         l,
		Right:        r,
		NodeType:     types.NodeLogic,
		OpType:       op,
		Value:        types.NewValueFromBool(false),
		IsNull:       false,
		IsCalculated: false,
	}
}

func (expr *Expr) GetBool() bool {
	return expr.Value.ToBool() && (types.IsOpComp(expr.OpType) || types.IsOpLogic(expr.OpType))
}

func (expr *Expr) isLogicComputable() bool {
	return expr.NodeType == types.NodeLogic || expr.NodeType == types.NodeComp
}

func (expr *Expr) Calculate(data []byte, relName string) error {
	if expr.IsCalculated {
		return nil
	}
	if expr.Left != nil {
		if err := expr.Left.Calculate(data, relName); err != nil {
			return err
		}
		/*
			if !expr.Left.IsCalculated {
				panic(0) // defense programming
			}
		*/
	}
	if expr.Right != nil {
		if err := expr.Right.Calculate(data, relName); err != nil {
			return err
		}
		/*
			if !expr.Right.IsCalculated {
				panic(0) // defense programming
			}
		*/
	}

	switch expr.NodeType {
	case types.NodeLogic:
		// And, or are binary operators, there must be left and right
		if expr.Left == nil && expr.Right == nil {
			return errorutil.ErrorExprBinaryOpWithNilChild
		}
		if expr.Left != nil {
			if !expr.Left.IsCalculated {
				return nil
			}
		}
		if expr.Right != nil {
			if !expr.Right.IsCalculated {
				return nil
			}
		}

		expr.IsCalculated = true
		switch expr.OpType {
		// child type will be guarantee in constructor
		case types.OpLogicAND, types.OpLogicOR:
			if expr.Left == nil || expr.Right == nil {
				return errorutil.ErrorExprBinaryOpWithNilChild
			}
			if !expr.Right.isLogicComputable() || !expr.Left.isLogicComputable() {
				return errorutil.ErrorExprIsNotLogicComputable
			}
			if expr.OpType == types.OpLogicAND {
				expr.Value.FromBool(expr.Left.GetBool() && expr.Right.GetBool())
			} else {
				expr.Value.FromBool(expr.Left.GetBool() || expr.Right.GetBool())
			}
		case types.OpLogicNOT:
			// not can only have non-nil right child
			if expr.Left != nil {
				return errorutil.ErrorExprUnaryOpWithNonNilLeftChild
			}
			if expr.Right == nil {
				return errorutil.ErrorExprUnaryOpWithNilRightChild
			}
			if !expr.Right.isLogicComputable() {
				return errorutil.ErrorExprIsNotLogicComputable
			}
			expr.Value.FromBool(!expr.Right.GetBool())
		default:
			return errorutil.ErrorExprNodeLogicWithNonLogicOp
		}
		return nil

	case types.NodeConst:
		expr.IsCalculated = true
		return nil
	case types.NodeComp:
		if expr.Right == nil || expr.Left == nil {
			return errorutil.ErrorExprInvalidComparison
		}
		if !expr.Left.IsCalculated || !expr.Right.IsCalculated {
			return nil
		}
		expr.IsCalculated = true
		if expr.OpType == types.OpCompIS || expr.OpType == types.OpCompISNOT {
			// A IS NULL
			// A IS NOT NULL
			is := expr.OpType == types.OpCompIS
			if expr.Left.IsNull && expr.Right.IsNull {
				expr.Value.FromBool(is)
			}
			// below comparison is meaningless?
			if (!expr.Left.IsNull && expr.Right.IsNull) || (expr.Left.IsNull && !expr.Right.IsNull) {
				if !expr.Right.IsNull {
					log.V(log.ExprLevel).Errorf("Right child is null, there must be violation on A IS NULL syntax")
					return errorutil.ErrorExprNodeCompViolateIsNullSyntax
				}
				expr.Value.FromBool(!is)
			}
			if !expr.Left.IsNull && !expr.Right.IsNull {
				log.V(log.ExprLevel).Errorf("Right child is null, there must be violation on A IS NULL syntax")
				return errorutil.ErrorExprNodeCompViolateIsNullSyntax
			}
		} else {
			if expr.Left.IsNull || expr.Right.IsNull {
				expr.IsNull = true
				expr.Value.FromBool(false)
			} else {
				if expr.OpType == types.OpCompLIKE || expr.OpType == types.OpCompNOTLIKE {
					regex := sqlparser.LikeToRegexp(expr.Right.Value.ToStr())
					left := expr.Left.Value.ToStr()
					if regex.Match([]byte(left)) && expr.OpType == types.OpCompLIKE {
						expr.Value.FromBool(true)
					} else {
						expr.Value.FromBool(false)
					}
					return nil
				}
				switch expr.OpType {
				case types.OpCompEQ:
					expr.Value.FromBool(expr.Left.Value.EQ(&expr.Right.Value))
				case types.OpCompLT:
					expr.Value.FromBool(expr.Left.Value.LT(&expr.Right.Value))
				case types.OpCompGT:
					expr.Value.FromBool(expr.Left.Value.GT(&expr.Right.Value))
				case types.OpCompLE:
					expr.Value.FromBool(expr.Left.Value.LE(&expr.Right.Value))
				case types.OpCompGE:
					expr.Value.FromBool(expr.Left.Value.GE(&expr.Right.Value))
				case types.OpCompNE:
					expr.Value.FromBool(expr.Left.Value.NE(&expr.Right.Value))
				default:
					log.V(log.ExprLevel).Warningf("Value comparison type not implemented %v\n", expr.OpType)
					expr.Value.FromBool(false)
				}
				// log.V(log.ExprLevel).Infof(
				// "Compare: left %v, right %v, res: %v",
				// expr.Left.Value.ToInt64(),
				// expr.Right.Value.ToInt64(),
				// expr.GetBool())
			}
		}
		return nil
	case types.NodeAttr:
		// this can be used for multiple tables join
		if expr.AttrInfo.RelName != "" && relName != "" {
			if expr.AttrInfo.RelName != relName {
				return nil
			}
		}

		expr.IsCalculated = true
		if expr.AttrInfo.NullAllowed {
			if data[expr.AttrInfo.AttrOffset+expr.AttrInfo.AttrSize] == 1 {
				expr.IsNull = true
				return nil
			}
		}
		switch expr.AttrInfo.AttrType {
		case types.INT:
			expr.Value.FromInt64(*(*int)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
		case types.FLOAT:
			expr.Value.FromFloat64(*(*float64)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
		case types.BOOL:
			expr.Value.FromBool(*(*bool)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
		case types.VARCHAR:
			expr.Value.FromStr(string(data[expr.AttrInfo.AttrOffset : expr.AttrInfo.AttrOffset+expr.AttrInfo.AttrSize]))
		case types.DATE:
			expr.Value.FromInt64(*(*int)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
			expr.Value.ValueType = types.DATE
		case types.NO_ATTR:
		default:
			log.V(log.ExprLevel).Warningf("data is not implemented\n")
		}
		// log.V(log.ExprLevel).Warningf("relationName: %v, TableName: %v\n", relationName, string(expr.AttrInfo.RelName[:]))
		return nil
	}
	panic(0) // return errorutil.ErrorExprNodeNotImplemented
}

func (expr *Expr) ResetCalculated() {
	if expr.Left != nil {
		expr.Left.ResetCalculated()
	}
	if expr.Right != nil {
		expr.Right.ResetCalculated()
	}
	expr.IsCalculated = false
}
