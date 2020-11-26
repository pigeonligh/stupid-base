package types

import "unsafe"

type RawSlice struct {
	pdata unsafe.Pointer
	len   int
	cap   int
}
type RawSlicePtr = *RawSlice

// ByteSlice2UInt32ArrayPtr to regard the page data([]byte) as a array of uint32
// while index out of range may occurs here, when offset dose not equal 0, res[127] would be definitely out of range
func ByteSlice2uint32ArrayPtr(data []byte, off int) *[1024]uint32{
	ptr := RawSlicePtr(unsafe.Pointer(&data)).pdata
	ptr2 := (unsafe.Pointer)((uintptr)(ptr) + uintptr(off))
	return (*[1024]uint32)(ptr2)
}


