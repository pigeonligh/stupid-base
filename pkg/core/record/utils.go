package record

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"math"
)

func recordPerPage(recordSize uint32) uint32{
	//ceil(x/32) + x * record_size + 8 = PF_PAGE_SIZE(4092)
	return uint32(math.Floor(float64(32 * types.PageSize) / float64(32 * recordSize + 1)))
}

func bitMapSize(recordPerPage uint32) uint32{
	return uint32(math.Ceil(float64(recordPerPage) / 32.0) * 4)
}