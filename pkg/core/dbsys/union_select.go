package dbsys

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

func (m *Manager) SelectTablesByWhereExpr(
	relNameList []string,
	attrNameList []string,
	expr sqlparser.Expr,
) (*TemporalTable, error) {
	/*
		for _, relName := range relNameList {
			attrs := db.sysManager.GetAttrInfoList(relName)
			where, err := solveWhere(stmt.Where, attrs, relName)
			if err != nil {
				return err
			}

			table, err := db.sysManager.SelectSingleTableByExpr(relName, nil, where, false)
			if err != nil {
				return err
			}

			tables = append(tables, table)
			allAttrs = append(allAttrs, attrs...)
		}*/

	return nil, nil
}
