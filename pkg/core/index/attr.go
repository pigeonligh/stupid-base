package index

import "github.com/pigeonligh/stupid-base/pkg/core/types"

type SingleAttr struct {
	types.AttrInfo
}

type AttrDefine struct {
	attrs []SingleAttr
}

func NewAttr() *AttrDefine {
	return &AttrDefine{}
}

func loadAttrFromHeader(header *types.IndexHeaderPage) *AttrDefine {
	attrs := []SingleAttr{}
	for i := 0; i < header.AttrSize; i++ {
		attrs = append(attrs, SingleAttr{AttrInfo: header.Attrs[i]})
	}
	return &AttrDefine{
		attrs: attrs,
	}
}

func (attr *AttrDefine) writeAttrToHeader(header *types.IndexHeaderPage) {
	header.AttrSize = len(attr.attrs)
	for i := 0; i < header.AttrSize; i++ {
		header.Attrs[i] = attr.attrs[i].AttrInfo
	}
}

func (attr *AttrDefine) dataToAttrs(rid types.RID, data []byte) []byte {
	result := []byte{}
	size := len(attr.attrs)
	for i := 0; i < size; i++ {
		sa := attr.attrs[i]
		tmpSlice := data[sa.AttrOffset : sa.AttrOffset+sa.AttrSize]
		result = append(result, tmpSlice...)
		// TODO: need some discuss
	}
	return result
}

func (attr *AttrDefine) AddSingleAttr(sa SingleAttr) {
	attr.attrs = append(attr.attrs, sa)
}
