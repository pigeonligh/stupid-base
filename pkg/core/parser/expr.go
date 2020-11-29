package parser

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

type AttrInfo struct {
	AttrName   string
	TableName  string
	AttrOffset int
	NotNull    bool	// TODO
}

// Used by SM_Manager::CreateTable
//struct AttrInfo {
//char     *AttrName;           // Attribute name
//ValueType attrType;            // Type of attribute
//int      attrLength;          // Length of attribute
//};
//
//// Used by Printer class
//struct DataAttrInfo {
//char     relName[MAXNAME+1];  // Relation name
//char     AttrName[MAXNAME+1]; // Attribute name
//int      offset;              // Offset of attribute
//ValueType attrType;            // Type of attribute
//int      attrLength;          // Length of attribute
//int      indexNo;             // Attribute index number
//};

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

func NewExprEmpty() *Expr{
	return &Expr{
		IsNull: true,
		IsCalculated: false,
		Value: Value{ValueType: types.NO_ATTR},
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

func NewExpr(l *Expr, op types.OpType, r *Expr) *Expr{
	return &Expr{
		Left:         l,
		Right:        r,
		NodeType:     0,
		OpType:       op,
		Value:        Value{},
		IsNull:       false,
		IsCalculated: false,
	}
}

func (expr *Expr) CompIsTrue() bool {
	return expr.Value.toBool() && types.IsOpComp(expr.OpType)
}

func (expr *Expr)Calculate(data []byte, relationName string) error{
	if expr.IsCalculated {
		return nil
	}
	if expr.Left != nil {
		if err := expr.Left.Calculate(data, relationName); err != nil{
			return err
		}
	}
	if expr.Right != nil {
		if err := expr.Right.Calculate(data, relationName); err != nil {
			return err
		}
	}

	switch expr.NodeType {
	case types.NodeConst:
	case types.NodeComp:
		if expr.Right == nil || expr.Left == nil {
			return errorutil.ErrorExprInvalidComparison
		}

		if (!expr.Left.IsNull && !expr.Left.IsCalculated) || (!expr.Right.IsNull && !expr.Right.IsCalculated) {
			return errorutil.ErrorExprNonNullNotCalculated
		}
		expr.IsCalculated = true
		if expr.OpType == types.OpCompIS || expr.OpType == types.OpCompISNOT{
			is := expr.OpType == types.OpCompIS
			if expr.Left.IsNull && expr.Right.IsNull {
				expr.Value.fromBool(is)
			}
			if (!expr.Left.IsNull && expr.Right.IsNull) || (expr.Left.IsNull && !expr.Right.IsNull) {
				expr.Value.fromBool(!is)
			}
			if !expr.Left.IsNull && !expr.Right.IsNull {
				log.Warningf("Comparison on non-null and non-null Value") // TODO
				expr.Value.fromBool(is)
			}
		}else {
			if expr.Left.IsNull || expr.Right.IsNull {
				expr.IsNull = true
			}else {
				switch expr.OpType {
				case types.OpCompEQ:
					expr.Value.fromBool(expr.Left.Value.EQ(&expr.Right.Value))
				case types.OpCompLT:
					expr.Value.fromBool(expr.Left.Value.LT(&expr.Right.Value))
				case types.OpCompGT:
					expr.Value.fromBool(expr.Left.Value.GT(&expr.Right.Value))
				case types.OpCompLE:
					expr.Value.fromBool(expr.Left.Value.LE(&expr.Right.Value))
				case types.OpCompGE:
					expr.Value.fromBool(expr.Left.Value.GE(&expr.Right.Value))
				case types.OpCompNE:
					expr.Value.fromBool(expr.Left.Value.NE(&expr.Right.Value))
				default:
					log.V(log.ExprLevel).Warningf("Value comparison type not implemented %v\n", expr.OpType)
					expr.Value.fromBool(false)
				}
			}
			return nil
		}

	case types.NodeAttr:
		if len(relationName) == 0 || relationName == expr.AttrInfo.TableName {
			expr.IsCalculated = true
			//if data[expr.AttrInfo.AttrOffset- 1] == 0 {
			//	// TODO add null for attr
			//	expr.IsNull = true
			//	panic(0)
			//}
			expr.IsNull = false
			switch expr.Value.ValueType {
			case types.INT:
				expr.Value.fromInt64(*(*int)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
			case types.FLOAT:
				expr.Value.fromFloat64(*(*float64)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
			case types.BOOL:
				expr.Value.fromBool(*(*bool)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
			case types.STRING:
				fallthrough
			case types.VARCHAR:
				expr.Value.fromStr(string(data[expr.AttrInfo.AttrOffset : expr.AttrInfo.AttrOffset+expr.AttrInfo.attrLen]))
			case types.DATE:
				expr.Value.fromInt64(*(*int)(types.ByteSliceToPointerWithOffset(data, expr.AttrInfo.AttrOffset)))
			case types.NO_ATTR:
			default:
				log.V(log.ExprLevel).Warningf("data is not implemented\n")
			}

		}else {
			log.V(log.ExprLevel).Warningf("relationName: %v, TableName: %v\n", relationName, expr.AttrInfo.TableName)
		}
		return nil
	}
	panic(0)
	return errorutil.ErrorExprNodeNotImplemented
}

