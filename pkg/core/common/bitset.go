package common

import (
	"math"
	"unsafe"
)

const (
	UnsignedMax = 0xffffffff
	BitsetFindNoRes = -1
	BitsetOpFails = -1
)

type MyBitset struct {
	data []uint32
	length int	// length of the array
	size int	// how many contents are there
}

func myBitset(data []uint32, contentSize int) *MyBitset {
	bitset := MyBitset{
		data:   data,
		length: int(math.Ceil(float64(contentSize) / 32)),
		size:   contentSize,
	}

	paddingBitsNum := bitset.length * 32 - bitset.size

	println(paddingBitsNum)
	println(bitset.length)
	println(unsafe.Sizeof(bitset.data))
	println((bitset.data)[0])

	for i := 0; i < paddingBitsNum; i++ {
		bitset.data[bitset.length - 1] |= 1 << (31 - i)
	}

	return &bitset
}

func (b *MyBitset) FindLowestZeroBitIdx() int{
	for i := 0; i < b.length; i++ {
		if b.data[i] != UnsignedMax {
			return i * 32 + int(math.Log2(float64(^b.data[i] & (b.data[i]+1))))
		}
	}
	return BitsetFindNoRes
}


func (b *MyBitset) FindLowestOneBitIdx() int{
	for i := 0; i < b.length; i++ {
		if b.data[i] != 0 {
			idx := i * 32 + int(math.Log2(float64(-b.data[i] & b.data[i])))
			if idx > b.size {
				return BitsetFindNoRes
			}else {
				return idx
			}
		}
	}
	return BitsetFindNoRes
}

func (b *MyBitset) IsOccupied(idx int) bool {
	var is, r = false, uint32(1 << (idx & 31))
	if r == (b.data[idx >> 5] & r) {
		is = true
	}
	return is
}

func (b *MyBitset) Set(idx int) int{
	if idx < 0 || idx >= b.size {
		return BitsetOpFails
	}
	b.data[idx >> 5] |= 1 << (idx & 31)
	return 0
}

func (b *MyBitset) Clean(idx int) int{
	if idx < 0 || idx >= b.size {
		return BitsetOpFails
	}
	b.data[idx >> 5] &= ^(1 << (idx & 31))
	return 0
}