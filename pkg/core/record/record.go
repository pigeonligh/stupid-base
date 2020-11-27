package record

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
)

type Record struct {
	Rid  types.RID
	Data []byte
	size int
}

func NewEmptyRecord() Record {
	return Record{
		Rid:  types.RID{},
		Data: nil,
		size: 0,
	}
}

func NewRecord(rid types.RID, data []byte, size int) (Record, error) {
	if len(data) != size {
		return NewEmptyRecord(), errorutil.ErrorRecordLengthNotMatch
	}
	copiedData := make([]byte, size)
	copy(copiedData, data)
	return Record{
		Rid:  rid,
		Data: copiedData,
		size: size,
	}, nil
}
