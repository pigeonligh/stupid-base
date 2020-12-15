package index

import "github.com/pigeonligh/stupid-base/pkg/core/types"

type SingleAttr struct {
}

type AttrDefine struct {
	offsets []SingleAttr
}

func NewAttr() *AttrDefine {
	return &AttrDefine{}
}

func loadAttrFromHeader(header *types.IndexHeaderPage) *AttrDefine {
	// TODO
	return &AttrDefine{
		offsets: []SingleAttr{},
	}
}

func (attr *AttrDefine) writeAttrToHeader(header *types.IndexHeaderPage) {
	// TODO
}

func (attr *AttrDefine) dataToAttrs(rid types.RID, data []byte) []byte {
	// TODO
	return []byte{}
}

func (attr *AttrDefine) AddSingleAttr(offset SingleAttr) {
	attr.offsets = append(attr.offsets, offset)
}
