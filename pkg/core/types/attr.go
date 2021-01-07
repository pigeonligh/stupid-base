package types

type AttrInfo struct {
	AttrSize    int // used by expr::NodeAttr
	AttrOffset  int // used by expr::NodeAttr
	AttrType    ValueType
	NullAllowed bool // used by system manager
}

type AttrSet struct {
	attrs   []AttrInfo
	nullPos []int
}

func NewAttrSet() *AttrSet {
	return &AttrSet{
		attrs:   []AttrInfo{},
		nullPos: []int{},
	}
}

func LoadAttrFromHeader(header *IndexHeaderPage) *AttrSet {
	attrs := []AttrInfo{}
	nullPos := []int{}
	pos := 0
	for i := 0; i < header.AttrSize; i++ {
		attrs = append(attrs, header.Attrs[i])
		pos += header.Attrs[i].AttrSize
		if header.Attrs[i].NullAllowed {
			nullPos = append(nullPos, pos)
			pos++
		}
	}
	return &AttrSet{
		attrs:   attrs,
		nullPos: nullPos,
	}
}

func (attr *AttrSet) WriteAttrToHeader(header *IndexHeaderPage) {
	header.AttrSize = len(attr.attrs)
	for i := 0; i < header.AttrSize; i++ {
		header.Attrs[i] = attr.attrs[i]
	}
}

func (attr *AttrSet) DataToAttrs(rid RID, data []byte) []byte {
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

func (attr *AttrSet) AddSingleAttr(ai AttrInfo) {
	attr.attrs = append(attr.attrs, ai)
	if ai.NullAllowed {
		attr.nullPos = append(attr.nullPos, ai.AttrOffset+ai.AttrSize)
	}
}

func (attr *AttrSet) HasNull(attrData []byte) bool {
	for _, pos := range attr.nullPos {
		if attrData[pos] > 0 {
			return true
		}
	}
	return false
}
