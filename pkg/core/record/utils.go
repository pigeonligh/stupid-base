package record

import (
	"bytes"
	"math"
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

func recordPerPage(recordSize int) int {
	// ceil(x/32) + x * record_size + 8 = PF_PAGE_SIZE(4092)
	// return int(math.Floor(float64(32*types.PageSize) / float64(32*recordSize+1)))
	return int(math.Floor(float64(types.PageSize-types.PageHeaderSize-types.BitsetByteMaxSize) / float64(recordSize)))
}

/*
func bitMapSize(recordPerPage int) int {
	return int(math.Ceil(float64(recordPerPage)/32.0) * 4)
}
*/

func Data2IntWithOffset(data []byte, off int) int {
	return *(*int)(types.ByteSliceToPointerWithOffset(data, off))
}

func Data2FloatWithOffset(data []byte, off int) float64 {
	return *(*float64)(types.ByteSliceToPointerWithOffset(data, off))
}

func Data2TrimmedStringWithOffset(data []byte, off int, size ...int) string {
	if len(size) == 0 {
		return strings.TrimSpace(string(bytes.Trim(data[off:], string(byte(0)))))
	}
	return strings.TrimSpace(string(bytes.Trim(data[off:off+size[0]], string(byte(0)))))
}
