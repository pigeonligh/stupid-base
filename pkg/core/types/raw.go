package types

import "unsafe"

type rawSlice struct {
	pdata unsafe.Pointer
	len   int
	cap   int
}
type rawSlicePtr = *rawSlice

// ByteSlice2uint32ArrayPtr to regard the page data([]byte) as a array of uint32
// while index out of range may occurs here, when offset dose not equal 0, res[127] would be definitely out of range
func ByteSlice2uint32ArrayPtr(data []byte, off int) *[BitsetArrayMaxLength]uint32 {
	ptr := rawSlicePtr(unsafe.Pointer(&data)).pdata
	ptr2 := (unsafe.Pointer)((uintptr)(ptr) + uintptr(off))
	return (*[BitsetArrayMaxLength]uint32)(ptr2)
}

// ByteSliceToPointer regard the page data([]byte) as Pointer
func ByteSliceToPointer(data []byte) unsafe.Pointer {
	return ByteSliceToPointerWithOffset(data, 0)
}

// ByteSliceToPointerWithOffset regard the page data([]byte) as Pointer
func ByteSliceToPointerWithOffset(data []byte, offset int) unsafe.Pointer {
	ptr := rawSlicePtr(unsafe.Pointer(&data)).pdata
	return (unsafe.Pointer)((uintptr)(ptr) + uintptr(offset))
}

// PointerToByteSlice, convert from Pointer to byte, len must be specified (cap equals len)
func PointerToByteSlice(ptr unsafe.Pointer, len int) []byte {
	raw := &rawSlice{
		pdata: ptr,
		len:   len,
		cap:   len,
	}
	return *(*[]byte)(unsafe.Pointer(raw))
}
