package dbsys

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

func checkIfLogicOpIsAllAnd(expr *parser.Expr) bool {
	if expr == nil {
		return true
	}
	if types.IsOpLogic(expr.OpType) {
		if expr.OpType == types.OpDefault {
			return true
		}
		if expr.OpType != types.OpLogicAND {
			return false
		}
	}
	res := true
	res = res && checkIfLogicOpIsAllAnd(expr.Left)
	res = res && checkIfLogicOpIsAllAnd(expr.Right)
	return res
}

func getAttrCompConstNode(expr *parser.Expr) []*parser.Expr {
	if expr.Left == nil || expr.Right == nil {
		return []*parser.Expr{}
	}
	if expr.NodeType != types.NodeComp && expr.NodeType != types.NodeLogic {
		return []*parser.Expr{}
	} else {
		if expr.Left.NodeType == types.NodeAttr && expr.Right.NodeType == types.NodeConst {
			return []*parser.Expr{expr}
		}
	}
	res := []*parser.Expr{}

	res = append(res, getAttrCompConstNode(expr.Left)...)
	res = append(res, getAttrCompConstNode(expr.Right)...)
	return res
}

func (m *Manager) getIndexHintFromExpr(relName string, expr *parser.Expr) []*parser.Expr {
	if !checkIfLogicOpIsAllAnd(expr) {
		return []*parser.Expr{}
	}
	tmpList := getAttrCompConstNode(expr)
	if len(tmpList) == 0 {
		return []*parser.Expr{}
	}
	var retList []*parser.Expr
	attrInfoCollection := m.GetAttrInfoCollection(relName)
	for _, expr := range tmpList {
		if item, found := attrInfoCollection.IdxMap[expr.Left.AttrInfo.IndexName]; found {
			if len(item) > 0 && item[0] == expr.Left.AttrInfo.AttrName {
				retList = append(retList, expr)
			}
		}
	}
	return retList
}
