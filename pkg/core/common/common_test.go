package common

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"testing"
)

func TestMyBitset(t *testing.T) {

	data := make([]byte, types.PageSize)

	contentSize := 50

	bitset := myBitset(types.ByteSlice2uint32ArrayPtr(data, 0), contentSize)

	for i := 0; i < contentSize; i += 2 {
		bitset.Set(i)
	}

	if res := bitset.FindLowestOneBitIdx(); res != 0 {
		t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", 0, res)
	}
	if res := bitset.FindLowestZeroBitIdx(); res != 1 {
		t.Errorf("FindLowestZeroBitIdx Error! Results should be %v but it's %v", 1, res)
	}

	for i := 0; i < contentSize; i += 2 {
		bitset.Clean(i)
	}
	for i := 31; i < contentSize; i += 2 {
		bitset.Set(i)
	}

	bitset.DebugBitset()

	if res := bitset.FindLowestOneBitIdx(); res != 31 {
		t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", 31, res)
	}
	if res := bitset.FindLowestZeroBitIdx(); res != 0 {
		t.Errorf("FindLowestZeroBitIdx Error! Results should be %v but it's %v", 0, res)
	}
	bitset.Clean(31)
	if res := bitset.FindLowestOneBitIdx(); res != 33 {
		t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", 33, res)
	}

	bitset.DebugBitset()

}
