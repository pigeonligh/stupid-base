package bitset

import (
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"testing"

	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

func TestMyBitset(t *testing.T) {

	log.SetDebugMode(true)
	log.SetLevel(log.BitsetLevel)
	data := make([]byte, types.PageSize)

	contentSize := 50

	page := (*types.BitsetPage)(types.ByteSliceToPointer(data))
	bitset := NewBitset(&page.Data, contentSize)

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
