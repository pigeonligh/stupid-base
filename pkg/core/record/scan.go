package record

import (
	"github.com/pigeonligh/stupid-base/pkg/core/dsutil/bitset"
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

type FileScan struct {
	file           *FileHandle
	cond           *parser.Expr
	currentBitset  *bitset.Bitset
	currentBitData [types.BitsetArrayMaxLength]uint32
	tableName      string
	currentPage    types.PageNum
	init           bool
}

func (f *FileScan) OpenFullScan(file *FileHandle) error {
	return f.OpenScan(file, parser.AttrInfo{}, types.OpDefault, types.NewValueFromEmpty())
}

func (f *FileScan) OpenScan(file *FileHandle, attr parser.AttrInfo, compOp types.OpType, value types.Value) error {
	if !types.IsOpComp(compOp) {
		return errorutil.ErrorRecordScanWithNonCompOp
	}

	var expr *parser.Expr = nil
	if compOp != types.OpDefault && attr.AttrType != types.NO_ATTR {
		if attr.AttrType != value.ValueType {
			return errorutil.ErrorRecordScanValueTypeNotMatch
		}
		left := parser.NewExprAttr(attr)
		right := parser.NewExprConst(value)
		expr = parser.NewExprComp(left, compOp, right)
	} else {
		log.V(log.RecordLevel).Infof("open scan with no comp or value type specified\n")
	}
	f.file = file
	f.cond = expr
	f.init = true
	return nil
}

func (f *FileScan) GetNextRecord() (*Record, error) {
	if !f.init {
		return nil, errorutil.ErrorRecordScanNotInit
	}
	for {
		if f.cond != nil {
			f.cond.ResetCalculated()
		}
		var slot = bitset.BitsetFindNoRes
		if f.currentPage != 0 {
			slot = f.currentBitset.FindLowestOneBitIdx()
		}
		for slot == bitset.BitsetFindNoRes {
			f.currentPage += 1
			if f.currentPage >= f.file.header.Pages {
				f.init = false
				return nil, nil
			}
			pageHandle, err := f.file.storageFH.GetPage(f.currentPage)
			if err != nil {
				log.V(log.RecordLevel).Errorf("GetPage(%v): %v", f.currentPage, err)
				panic(0)
			}
			recordPage := (*types.RecordPage)(types.ByteSliceToPointer(pageHandle.Data))
			f.currentBitData = recordPage.BitsetData
			f.currentBitset = bitset.NewBitset(&f.currentBitData, f.file.header.RecordPerPage)

			slot = f.currentBitset.FindLowestOneBitIdx()
			if err = f.file.storageFH.UnpinPage(f.currentPage); err != nil {
				panic(0)
			}
		}
		f.currentBitset.Clean(slot)
		if record, err := f.file.GetRec(types.RID{
			Page: f.currentPage,
			Slot: slot,
		}); err != nil {
			panic(0)
		} else {
			if f.cond != nil {
				err := f.cond.Calculate(record.Data)
				if err != nil {
					return nil, err
				}
				if f.cond.GetBool() {
					return record, nil
				}
			} else {
				return record, nil
			}
		}
	}
}
