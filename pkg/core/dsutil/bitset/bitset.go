package bitset

import (
	"math"

	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

const (
	UnsignedMax     = ^uint32(0)
	BitsetFindNoRes = -1
	BitsetOpFails   = -1
)

type Bitset struct {
	data   *[types.BitsetDataSize]uint32
	length int // length of the array
	size   int // how many contents are there
}

func NewBitset(data *[types.BitsetDataSize]uint32, contentSize int) *Bitset {
	bitset := Bitset{
		data:   data,
		length: int(math.Ceil(float64(contentSize) / 32)),
		size:   contentSize,
	}

	paddingBitsNum := bitset.length*32 - bitset.size

	for i := 0; i < paddingBitsNum; i++ {
		bitset.data[bitset.length-1] |= 1 << (31 - i)
	}

	return &bitset
}

func (b *Bitset) FindLowestZeroBitIdx() int {
	for i := 0; i < b.length; i++ {
		if b.data[i] != UnsignedMax {
			return i*32 + int(math.Log2(float64(^b.data[i]&(b.data[i]+1))))
		}
	}
	return BitsetFindNoRes
}

func (b *Bitset) FindLowestOneBitIdx() int {
	for i := 0; i < b.length; i++ {
		if b.data[i] != 0 {
			idx := i*32 + int(math.Log2(float64(-b.data[i]&b.data[i])))
			if idx > b.size {
				return BitsetFindNoRes
			} else {
				return idx
			}
		}
	}
	return BitsetFindNoRes
}

func (b *Bitset) IsOccupied(idx int) bool {
	var is, r = false, uint32(1 << (idx & 31))
	if r == (b.data[idx>>5] & r) {
		is = true
	}
	return is
}

func (b *Bitset) Set(idx int) int {
	if idx < 0 || idx >= b.size {
		return BitsetOpFails
	}
	b.data[idx>>5] |= 1 << (idx & 31)
	return 0
}

func (b *Bitset) Clean(idx int) int {
	if idx < 0 || idx >= b.size {
		return BitsetOpFails
	}
	b.data[idx>>5] &= ^(1 << (idx & 31))
	return 0
}

func (b *Bitset) DebugBitset() {
	println("------------------------------------------------------------------------------------")
	println("Bitmap size of content: ", b.size)
	println("Bitmap arr length: ", b.length)
	println("Bitmap padding bits num: ", b.length*32-b.size)
	var b2i = map[bool]int8{false: 0, true: 1}
	for i := 0; i < b.size; i++ {
		if i%32 == 0 && i != 0 {
			print("-")
		}
		print(b2i[b.IsOccupied(i)])
	}
	print("$")
	for i := b.size; i < b.length*32; i++ {
		print(b2i[b.IsOccupied(i)])
	}
	println()
	println("------------------------------------------------------------------------------------")
}
