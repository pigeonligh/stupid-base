package dbsys

import (
	"fmt"
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TT2Attrs(table *TemporalTable, addition types.CalculatedValuesType) []types.CalculatedValuesType {
	result := []types.CalculatedValuesType{}
	for _, rec := range table.rows {
		row := types.CalculatedValuesType{}
		for k, v := range addition {
			row[k] = v
		}
		for index, attrName := range table.attrs {
			attrTable := table.rels[index]

			var str string
			byteSlice := rec.Data[table.offs[index] : table.offs[index]+table.lens[index]]
			if table.nils[index] {
				if rec.Data[table.offs[index]+table.lens[index]] == 1 {
					// a single col always takes up (size + 1 bit)
					str = types.MagicNullString
				} else {
					str = data2StringByTypes(byteSlice, table.types[index])
				}
			} else {
				str = data2StringByTypes(byteSlice, table.types[index])
			}

			attr := types.SimpleAttr{
				TableName: attrTable,
				ColName:   attrName,
			}
			row[attr] = strings.Trim(str, "`'\"") // TODO
		}
		result = append(result, row)
	}
	return result
}

func (m *Manager) SelectTablesByWhereExpr(
	relNameList []string,
	attrNameList []string,
	expr *sqlparser.Where,
) (*TemporalTable, error) {
	calculatedValues := []types.CalculatedValuesType{
		make(types.CalculatedValuesType),
	}

	for _, relName := range relNameList {
		attrs := m.GetAttrInfoList(relName)

		newCalculatedValues := []types.CalculatedValuesType{}

		for _, cvs := range calculatedValues {
			where, err := parser.SolveWhere(expr, attrs, relName, cvs)
			if err != nil {
				return nil, err
			}

			table, err := m.SelectSingleTableByExpr(relName, nil, where, false)
			if err != nil {
				return nil, err
			}

			values := TT2Attrs(table, cvs)
			newCalculatedValues = append(newCalculatedValues, values...)

			// fmt.Println(index+1, len(newCalculatedValues))
		}

		calculatedValues = newCalculatedValues
		fmt.Println(len(calculatedValues))
	}

	fmt.Println("find", len(calculatedValues))
	return nil, nil
}
