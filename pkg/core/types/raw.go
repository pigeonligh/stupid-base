package types

import "unsafe"

type rawSlice struct {
	pdata unsafe.Pointer
	len   int
	cap   int
}
type rawSlicePtr = *rawSlice

// ByteSliceToPointer regard the page data([]byte) as Pointer
func ByteSliceToPointer(data []byte) unsafe.Pointer {
	return ByteSliceToPointerWithOffset(data, 0)
}

// ByteSliceToPointerWithOffset regard the page data([]byte) as Pointer
func ByteSliceToPointerWithOffset(data []byte, offset int) unsafe.Pointer {
	ptr := rawSlicePtr(unsafe.Pointer(&data)).pdata
	return (unsafe.Pointer)((uintptr)(ptr) + uintptr(offset))
}

// PointerToByteSlice converts object from Pointer to byte, len must be specified (cap equals len)
func PointerToByteSlice(ptr unsafe.Pointer, len int) []byte {
	raw := &rawSlice{
		pdata: ptr,
		len:   len,
		cap:   len,
	}
	return *(*[]byte)(unsafe.Pointer(raw))
}
