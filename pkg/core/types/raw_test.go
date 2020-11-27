/*
Copyright (c) 2020, pigeonligh.
*/

package types

import (
	"testing"
	"unsafe"
)

func TestRaw(t *testing.T) {
	data := make([]byte, PageSize)

	type TestStruct struct {
		a int
		b int
		c int
	}
	b := TestStruct{
		a: 0x1,
		b: 0x11,
		c: 0x111,
	}
	bSlice := PointerToByteSlice(unsafe.Pointer(&b), int(unsafe.Sizeof(TestStruct{})))
	copy(data, bSlice)
	t.Log(data)

	for i := 0; i < 12; i++ {
		data[i] = byte(i)
	}
	page := (*RecordPage)(ByteSliceToPointer(data))
	t.Log(page.NextFree)
	t.Log(page.Data[0])

	// t.Error("display")
}
