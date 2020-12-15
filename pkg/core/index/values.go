package index

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

type IndexedValue struct {
	types.IMValue

	index types.RID
}

func initValuePage(data []byte) types.RID {
	// TODO
	return types.RID{}
}

func setValue(value *IndexedValue, data []byte) {
	// TODO
}

func getValue(index types.RID, data []byte) *IndexedValue {
	// TODO
	return nil
}
