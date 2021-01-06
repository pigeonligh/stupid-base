package parser

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

type AttrInfo struct {
	types.AttrInfo

	AttrName [types.MaxNameSize]byte
	RelName  [types.MaxNameSize]byte // 24 * 2
	Default  types.Value
}

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

//
type AttrInfo4Expr struct {
	Off  int
	Len  int
	Nil  bool
	Type types.ValueType
}

// attr will always be appears with
func NewExprAttr(attr AttrInfo4Expr) *Expr {
	return &Expr{
		NodeType: types.NodeAttr,
		AttrInfo: AttrInfo{
			AttrInfo: types.AttrInfo{
				AttrSize:    attr.Len,
				AttrOffset:  attr.Off,
				AttrType:    attr.Type,
				NullAllowed: attr.Nil,
			},
		},
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
	return expr.OpType == types.NodeLogic || expr.OpType == types.NodeComp
}

func (expr *Expr) Calculate(data []byte) error {
	if expr.IsCalculated {
		return nil
	}
	if expr.Left != nil {
		if err := expr.Left.Calculate(data); err != nil {
			return err
		}
		if !expr.Left.IsCalculated {
			panic(0) // defense programming
		}
	}
	if expr.Right != nil {
		if err := expr.Right.Calculate(data); err != nil {
			return err
		}
		if !expr.Right.IsCalculated {
			panic(0) // defense programming
		}
	}

	switch expr.NodeType {
	case types.NodeLogic:
		// And, or are binary operators, there must be left and right
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
			return nil
		default:
			return errorutil.ErrorExprNodeLogicWithNonLogicOp
		}
	case types.NodeConst:
		expr.IsCalculated = true
		return nil
	case types.NodeComp:
		if expr.Right == nil || expr.Left == nil {
			return errorutil.ErrorExprInvalidComparison
		}

		if (!expr.Left.IsNull && !expr.Left.IsCalculated) || (!expr.Right.IsNull && !expr.Right.IsCalculated) {
			panic(0)
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
		expr.IsCalculated = true
		if expr.AttrInfo.NullAllowed {
			if data[expr.AttrInfo.AttrOffset+expr.AttrInfo.AttrSize] == 1 {
				expr.IsNull = true
				return nil
			}
		}
		switch expr.Value.ValueType {
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
