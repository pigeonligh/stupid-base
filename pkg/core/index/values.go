package index

import (
	"github.com/pigeonligh/stupid-base/pkg/core/storage"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

type IndexedValue struct {
	types.IMValue

	index types.RID
}

func initValuePage(page *storage.PageHandle) types.RID {
	currentValuePage := (*types.IMValuePage)(types.ByteSliceToPointer(page.Data))
	for i := 0; i < int(types.IMValueItem); i++ {
		currentValuePage.Values[i].Next = types.MakeRID(page.Page, i-1)
	}
	return types.RID{
		Page: page.Page,
		Slot: types.IMValueItem - 1,
	}
}

func setValue(value *IndexedValue, data []byte) {
	currentValuePage := (*types.IMValuePage)(types.ByteSliceToPointer(data))
	slot := value.index.Slot

	currentValuePage.Values[slot].Row = value.Row
	currentValuePage.Values[slot].Next = value.Next
}

func getValue(index types.RID, data []byte) *IndexedValue {
	currentValuePage := (*types.IMValuePage)(types.ByteSliceToPointer(data))
	slot := index.Slot

	return &IndexedValue{
		IMValue: currentValuePage.Values[slot],
		index:   index,
	}
}
