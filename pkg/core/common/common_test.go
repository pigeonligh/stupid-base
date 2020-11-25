package common

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

func TestMyBitset(t *testing.T) {

	data := make([]byte, types.PageSize)

	contentSize := 160

	data2 := ((*struct {
		pdata unsafe.Pointer
		len   int
		cap   int
	})(unsafe.Pointer(&data))).pdata

	data3 := (unsafe.Pointer)((uintptr)(data2) + 8)

	ptr := (*[1024]uint32)(data3)

	bitset := myBitset(ptr, contentSize)

	for i := 0; i < contentSize; i += 2 {
		bitset.Set(i)
	}

	if res := bitset.FindLowestOneBitIdx(); res != 0 {
		t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", 0, res)
	}
	if res := bitset.FindLowestZeroBitIdx(); res != 1 {
		t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", 1, res)
	}

	for i := 0; i < contentSize; i += 2 {
		bitset.Clean(i)
	}
	for i := 31; i < contentSize; i += 2 {
		bitset.Set(i)
	}

	fmt.Println(bitset.data)
	fmt.Println(data)

	if res := bitset.FindLowestOneBitIdx(); res != 31 {
		t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", 31, res)
	}
	if res := bitset.FindLowestZeroBitIdx(); res != 0 {
		t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", 0, res)
	}
	bitset.Clean(31)
	if res := bitset.FindLowestOneBitIdx(); res != 33 {
		t.Errorf("FindLowestOneBitIdx Error! Results should be %v but it's %v", 33, res)
	}

}
