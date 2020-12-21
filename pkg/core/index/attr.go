package index

import "github.com/pigeonligh/stupid-base/pkg/core/types"

type SingleAttr struct {
	types.AttrInfo
}

type AttrDefine struct {
	attrs   []SingleAttr
	nullPos []int
}

func NewAttr() *AttrDefine {
	return &AttrDefine{
		attrs:   []SingleAttr{},
		nullPos: []int{},
	}
}

func loadAttrFromHeader(header *types.IndexHeaderPage) *AttrDefine {
	attrs := []SingleAttr{}
	nullPos := []int{}
	pos := 0
	for i := 0; i < header.AttrSize; i++ {
		attrs = append(attrs, SingleAttr{AttrInfo: header.Attrs[i]})
		pos += header.Attrs[i].AttrSize
		if header.Attrs[i].NullAllowed {
			nullPos = append(nullPos, pos)
			pos++
		}
	}
	return &AttrDefine{
		attrs:   attrs,
		nullPos: nullPos,
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
		attrsize := sa.AttrSize
		if sa.NullAllowed {
			attrsize++
		}
		tmpSlice := data[sa.AttrOffset : sa.AttrOffset+attrsize]
		result = append(result, tmpSlice...)
	}
	return result
}

func (attr *AttrDefine) AddSingleAttr(sa SingleAttr) {
	attr.attrs = append(attr.attrs, sa)
}

func (attr *AttrDefine) HasNull(attrData []byte) bool {
	for _, pos := range attr.nullPos {
		if attrData[pos] > 0 {
			return true
		}
	}
	return false
}
