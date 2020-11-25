package common

import (
	"testing"
	"unsafe"
)

func TestMyBitset(t *testing.T) {

	data := [4096]byte {}

	contentSize := 160

	ptr := (*[unsafe.Sizeof(data)]uint32)(unsafe.Pointer(&data))

	bitset := myBitset(ptr, contentSize)

	for i := 0; i < contentSize; i+=2 {
		bitset.Set(i)
	}

	if res := bitset.FindLowestOneBitIdx(); res != 0 {
		t.Error("FindLowestOneBitIdx Error! Results should be ", 0)
	}
	if res := bitset.FindLowestZeroBitIdx(); res != 1 {
		t.Error("FindLowestZeroBitIdx Error! Results should be ", 1)
	}

	for i := 0; i < contentSize; i+=2 {
		bitset.Clean(i)
	}
	for i := 31; i < contentSize; i+=2 {
		bitset.Set(i)
	}
	if res := bitset.FindLowestOneBitIdx(); res != 31 {
		t.Error("FindLowestOneBitIdx Error! Results should be ", 31)
	}
	if res := bitset.FindLowestZeroBitIdx(); res != 0 {
		t.Error("FindLowestOneBitIdx Error! Results should be ", 0)
	}
	bitset.Clean(31)
	if res := bitset.FindLowestOneBitIdx(); res != 32 {
		t.Error("FindLowestOneBitIdx Error! Results should be ", 32)
	}


}
