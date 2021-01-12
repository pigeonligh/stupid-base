package bitset

import (
	"math"
	"strings"

	log "github.com/pigeonligh/stupid-base/pkg/logutil"

	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

const (
	UnsignedMax     = ^uint32(0)
	BitsetFindNoRes = -1
	BitsetOpFails   = -1
)

type Bitset struct {
	data *[types.BitsetArrayMaxLength]uint32
	size int // how many records are there (how many real occupied bytes)
}

func NewBitset(data *[types.BitsetArrayMaxLength]uint32, contentNums int) *Bitset {
	bitset := Bitset{
		data: data,
		size: contentNums,
	}

	for i := 0; i < len(data); i++ {
		paddingBitsNum := (i+1)*32 - bitset.size
		if paddingBitsNum >= 32 {
			bitset.data[i] = UnsignedMax
		} else {
			for j := 0; j < paddingBitsNum; j++ {
				bitset.data[i] |= 1 << (31 - j)
			}
		}
	}
	return &bitset
}

func (b *Bitset) FindLowestZeroBitIdx() int {
	for i := 0; i < len(b.data); i++ {
		if b.data[i] != UnsignedMax {
			return i*32 + int(math.Log2(float64(^b.data[i]&(b.data[i]+1))))
		}
	}
	return BitsetFindNoRes
}

func (b *Bitset) FindLowestOneBitIdx() int {
	for i := 0; i < len(b.data); i++ {
		if b.data[i] != 0 {
			idx := i*32 + int(math.Log2(float64(-b.data[i]&b.data[i])))
			if idx >= b.size {
				return BitsetFindNoRes
			}
			return idx
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
	log.Debugf("Bitmap size of content: %v\n", b.size)
	log.Debugf("Bitmap arr length: %v\n", len(b.data))
	log.Debugf("Bitmap padding bits num: %v\n", len(b.data)*32-b.size)

	var sb strings.Builder

	// var b2i = map[bool]int8{false: 0, true: 1}
	var b2c = map[bool]string{false: "0", true: "1"}

	for i := 0; i < b.size; i++ {
		if i%32 == 0 && i != 0 {
			sb.Write([]byte("-"))
		}
		sb.Write([]byte(b2c[b.IsOccupied(i)]))
	}
	sb.Write([]byte("$"))
	for i := b.size; i < len(b.data)*32; i++ {
		sb.Write([]byte(b2c[b.IsOccupied(i)]))
	}
	log.Debugf(sb.String())
}
