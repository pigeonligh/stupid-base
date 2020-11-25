package record

import "github.com/pigeonligh/stupid-base/pkg/core/types"

type Header struct {
	RecordSize uint32
	RecordNum uint32
	RecordPerPage uint32
	FirstSparePage uint32
	PageNum types.PageNum
	SlotMapSize uint32
	SizeOfHeader uint32
}

