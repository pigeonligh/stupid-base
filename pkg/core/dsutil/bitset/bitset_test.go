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

	contentSize := 126

	page := (*types.RecordPage)(types.ByteSliceToPointer(data))
	bitset := NewBitset(&page.BitsetData, contentSize)


	bitset.Set(48)
	if res := bitset.FindLowestOneBitIdx(); res != 48 {
		t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", 48, res)
	}
	bitset.Set(63)
	bitset.Set(64)
	if res := bitset.Set(126); res != BitsetOpFails {
		t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", -1, res)
	}
	bitset.Set(125)
	log.Debugf("Set: %v %v %v %v %v\n", 48, 63, 64, 126, 125)
	bitset.DebugBitset()


	for i := 0; i < contentSize; i += 1 {
		bitset.Clean(i)
	}

	if res := bitset.FindLowestOneBitIdx(); res != BitsetFindNoRes {
		t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", -1, res)
	}
	for i := contentSize - 1; i >= 0; i-- {
		bitset.Set(i)
		if res := bitset.FindLowestOneBitIdx(); res != i {
			t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", i, res)
		}
	}

	if res := bitset.FindLowestZeroBitIdx(); res != BitsetFindNoRes {
		t.Errorf("FindLowestZeroBitIdx Error! Results should be %v but it's %v", -1, res)
	}
	for i := contentSize - 1; i >= 0; i-- {
		bitset.Clean(i)
		if res := bitset.FindLowestZeroBitIdx(); res != i {
			t.Errorf("FindLowestZeroBitIdx Error! Results should be %v but it's %v", i, res)
		}
	}


}
