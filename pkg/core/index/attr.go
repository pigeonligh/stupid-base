package index

import "github.com/pigeonligh/stupid-base/pkg/core/types"

type OffsetPair struct {
}

type AttrDefine struct {
	offsets []OffsetPair
}

func NewAttr() *AttrDefine {
	return &AttrDefine{}
}

func loadAttrFromHeader(header *types.IndexHeaderPage) *AttrDefine {
	// TODO
	return &AttrDefine{
		offsets: []OffsetPair{},
	}
}

func (attr *AttrDefine) writeAttrToHeader(header *types.IndexHeaderPage) {
	// TODO
}

func (attr *AttrDefine) dataToAttrs(rid types.RID, data []byte) []byte {
	// TODO
	return []byte{}
}

func (attr *AttrDefine) AddOffsetPair(offset OffsetPair) {
	attr.offsets = append(attr.offsets, offset)
}
