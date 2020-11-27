package record

import (
	"math"

	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

func recordPerPage(recordSize int) int {
	//ceil(x/32) + x * record_size + 8 = PF_PAGE_SIZE(4092)
	return int(math.Floor(float64(32*types.PageSize) / float64(32*recordSize+1)))
}

func bitMapSize(recordPerPage int) int {
	return int(math.Ceil(float64(recordPerPage)/32.0) * 4)
}
