package record

import (
	"github.com/pigeonligh/stupid-base/pkg/core/dsutil/bitset"
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
)



type FileScan struct {
	file 		*FileHandle
	cond 		*parser.Expr
	currentBitset	*bitset.Bitset
	currentBitData	[types.BitsetArrayMaxLength]uint32
	tableName 	string
	currentPage	types.PageNum
	close 		bool
}

func (f *FileScan) OpenScan(file *FileHandle, valueType types.ValueType, valueSize int, attrOffset int, compOp types.OpType, value parser.Value)  error{
	if !types.IsOpComp(compOp){
		return errorutil.ErrorRecordScanWithNonCompOp
	}

	var expr *parser.Expr = nil
	if compOp != types.OpDefault {
		left := parser.NewExprEmpty()
		left.AttrInfo.AttrOffset = attrOffset
		left.Value.ValueSize = valueSize
		left.Value.ValueType = valueType
		left.NodeType = types.NodeAttr

		var right *parser.Expr
		if value.ValueType != types.NO_ATTR {
			if value.ValueType != valueType {
				return errorutil.ErrorRecordScanValueTypeNotMatch
			}
			right = parser.NewExprConst(value)
		} else {
			right = parser.NewExprEmpty()
		}

		expr = parser.NewExpr(left, compOp, right)
	}
	f.file = file
	f.cond = expr
	f.close = false
	return nil
}

func (f* FileScan) GetNextRecord() (*Record, error) {
	for {
		var slot = bitset.BitsetFindNoRes
		if f.currentPage != 0 {
			slot = f.currentBitset.FindLowestOneBitIdx()
		}
		for slot == bitset.BitsetFindNoRes {
			f.currentPage += 1
			if f.currentPage >= f.file.header.Pages {
				f.close = true
				return nil, nil
			}
			pageHandle, err := f.file.storageFH.GetPage(f.currentPage)
			if err != nil {
				panic(0)
			}
			recordPage := (*types.RecordPage)(types.ByteSliceToPointer(pageHandle.Data))
			f.currentBitData = recordPage.BitsetData
			f.currentBitset  = bitset.NewBitset(&f.currentBitData, f.file.header.RecordPerPage)

			slot = f.currentBitset.FindLowestOneBitIdx()
			if err := f.file.storageFH.UnpinPage(f.currentPage); err != nil {
				panic(0)
			}
			record, err := f.file.GetRec(types.RID{
				Page: f.currentPage,
				Slot: slot,
			})
			if err != nil {
				panic(0)
			}
			f.currentBitset.Clean(slot)

			if f.cond != nil {
				err := f.cond.Calculate(record.Data, f.tableName)
				if err != nil {
					return nil, err
				}
				if f.cond.CompIsTrue() {
					break
				}
			}else {
				break
			}
		}
	}
}



//Expr *_condition = nullptr;
//MyBitset* _current_bitmap = nullptr;
//unsigned *_current_bitdata = nullptr;
//
//unsigned _current_page = 0;
//
//std::string _table_name;