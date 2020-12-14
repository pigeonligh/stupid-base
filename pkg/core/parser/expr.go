package parser

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

type AttrInfo struct {
	AttrName      [types.MaxNameSize]byte
	RelName       [types.MaxNameSize]byte //24 * 2
	AttrSize      int                     // used by expr::NodeAttr
	AttrOffset    int                     // used by expr::NodeAttr
	AttrType      types.ValueType
	IndexNo       int       // used by system manager
	ConstraintRID types.RID // used by system manager
	NullAllowed   bool      // used by system manager
	IsPrimary     bool      // used by system manager
	AutoIncrement bool      // used for auto increasing
	Default       Value
}

type Expr struct {
	Left  *Expr
	Right *Expr

	NodeType types.NodeType
	OpType   types.OpType

	Value    Value    // represent basic Value which can provide convert functions
	AttrInfo AttrInfo // represent Value as attr in a db/table

	//
	IsNull       bool
	IsCalculated bool
}

func NewExprEmpty() *Expr {
	return &Expr{
		IsNull:       false,
		IsCalculated: false,
		Value:        Value{ValueType: types.NO_ATTR},
	}
}

func NewExprConst(value Value) *Expr {
	return &Expr{
		NodeType:     types.NodeConst,
		OpType:       types.OpDefault,
		Value:        value,
		AttrInfo:     AttrInfo{},
		IsNull:       false,
		IsCalculated: true,
	}
}

func NewExprComp(l *Expr, op types.OpType, r *Expr) *Expr {
	return &Expr{
		Left:         l,
		Right:        r,
		NodeType:     types.NodeComp,
		OpType:       op,
		Value:        Value{},
		IsNull:       false,
		IsCalculated: false,
	}
}

func (expr *Expr) CompIsTrue() bool {
	return expr.Value.ToBool() && types.IsOpComp(expr.OpType)
}

func (expr *Expr) Calculate(data []byte) error {
	if expr.IsCalculated {
		return nil
	}
	if expr.Left != nil {
		if err := expr.Left.Calculate(data); err != nil {
			return err
		}
	}
	if expr.Right != nil {
		if err := expr.Right.Calculate(data); err != nil {
			return err
		}
	}

	switch expr.NodeType {
	case types.NodeConst:
		expr.IsCalculated = true
		return nil
	case types.NodeComp:
		if expr.Right == nil || expr.Left == nil {
			return errorutil.ErrorExprInvalidComparison
		}

		if (!expr.Left.IsNull && !expr.Left.IsCalculated) || (!expr.Right.IsNull && !expr.Right.IsCalculated) {
			return errorutil.ErrorExprNonNullNotCalculated
		}
		expr.IsCalculated = true
		if expr.OpType == types.OpCompIS || expr.OpType == types.OpCompISNOT {
			is := expr.OpType == types.OpCompIS
			if expr.Left.IsNull && expr.Right.IsNull {
				expr.Value.FromBool(is)
			}
			if (!expr.Left.IsNull && expr.Right.IsNull) || (expr.Left.IsNull && !expr.Right.IsNull) {
				expr.Value.FromBool(!is)
			}
			if !expr.Left.IsNull && !expr.Right.IsNull {
				log.Warningf("Comparison on non-null and non-null Value") // TODO
				expr.Value.FromBool(is)
			}
		} else {
			if expr.Left.IsNull || expr.Right.IsNull {
				expr.IsNull = true
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
				//log.V(log.ExprLevel).Infof("Compare: left %v, right %v, res: %v", expr.Left.Value.ToInt64(), expr.Right.Value.ToInt64(), expr.CompIsTrue())

			}
		}
		return nil
	case types.NodeAttr:
		expr.IsCalculated = true
		//if data[expr.AttrInfo.AttrOffset- 1] == 0 {
		//	// TODO add null for attr
		//	expr.IsNull = true
		//	panic(0)
		//}
		expr.IsNull = false
		switch expr.Value.ValueType {
		case types.INT:
			expr.Value.FromInt64(*(*int)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
		case types.FLOAT:
			expr.Value.FromFloat64(*(*float64)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
		case types.BOOL:
			expr.Value.FromBool(*(*bool)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
		case types.STRING:
			fallthrough
		case types.VARCHAR:
			expr.Value.FromStr(string(data[expr.AttrInfo.AttrOffset : expr.AttrInfo.AttrOffset+expr.AttrInfo.AttrSize]))
		case types.DATE:
			expr.Value.FromInt64(*(*int)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
		case types.NO_ATTR:
		default:
			log.V(log.ExprLevel).Warningf("data is not implemented\n")
		}
		//log.V(log.ExprLevel).Warningf("relationName: %v, TableName: %v\n", relationName, string(expr.AttrInfo.RelName[:]))
		return nil
	}
	panic(0)
	return errorutil.ErrorExprNodeNotImplemented
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
