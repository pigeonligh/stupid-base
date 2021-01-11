package dbsys

import (
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"

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
	attrTableList []string,
	attrNameList []string,
	expr *sqlparser.Where,
) (*TemporalTable, error) {
	allAttrs := AttrInfoList{}

	selectedAttrs := AttrInfoList{}

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

		}

		allAttrs = append(allAttrs, attrs...)
		calculatedValues = newCalculatedValues
	}

	if attrNameList == nil {
		selectedAttrs = append(selectedAttrs, allAttrs...)
	} else {
		for index, attrName := range attrNameList {
			attr, err := parser.GetAttrFromList(allAttrs, attrTableList[index], attrName)
			if err != nil || attr == nil {
				// panic(err)
				return nil, errorutil.ErrorColNotFound
			}
			selectedAttrs = append(selectedAttrs, *attr)
		}
	}

	offs := make([]int, 0)
	lens := make([]int, 0)
	rels := make([]string, 0)
	attrs := make([]string, 0)
	typs := make([]types.ValueType, 0)
	nils := make([]bool, 0)

	totLength := 0
	for _, attr := range selectedAttrs {
		offs = append(offs, attr.AttrOffset)
		lens = append(lens, attr.AttrSize)
		rels = append(rels, attr.RelName)
		attrs = append(attrs, attr.AttrName)
		typs = append(typs, attr.AttrType)
		nils = append(nils, attr.NullAllowed)
		totLength += attr.AttrSize + 1
	}

	rows := make([]*record.Record, 0)
	for _, row := range calculatedValues {
		tmpRec := record.Record{
			Rid:  types.RID{},
			Data: make([]byte, totLength),
		}

		for i := range selectedAttrs {
			rawVal, found := row[types.SimpleAttr{
				TableName: rels[i],
				ColName:   attrs[i],
			}]
			if !found {
				return nil, errorutil.ErrorColNotFound
			}

			if val, err := types.String2Value(rawVal, lens[i], typs[i]); err != nil {
				panic(0)
			} else {
				if val.ValueType == types.NO_ATTR {
					// null
					tmpRec.Data[offs[i]+lens[i]] = 1
				} else {
					copy(tmpRec.Data[offs[i]:offs[i]+lens[i]], val.Value[0:lens[i]])
				}
			}
		}
		rows = append(rows, &tmpRec)
	}

	tmpTable := &TemporalTable{
		rels:  rels,
		attrs: attrs,
		lens:  lens,
		offs:  offs,
		types: typs,
		nils:  nils,
		rows:  rows,
	}

	return tmpTable, nil
}
