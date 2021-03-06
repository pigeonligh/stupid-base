package types

import "unsafe"

const (
	// NodePageHeaderSize is the size of node page header
	NodePageHeaderSize = int(unsafe.Sizeof(IMNodePageHeader{}))

	// NodePageSize is the size of node page data
	NodePageSize = PageDataSize - NodePageHeaderSize

	// NodeMaxItem is the max number of node items
	NodeMaxItemNum = 32 // NodePageSize / 2 / int(unsafe.Sizeof(RID{}))

	// NodeMinItem is the min number of node items
	NodeMinItem = NodeMaxItemNum / 2

	IMValuePageHeaderSize = 0

	IMValuePageSize = PageDataSize - IMValuePageHeaderSize

	IMValueItem = IMValuePageSize / int(unsafe.Sizeof(IMValue{}))

	IMAttrSize = (PageDataSize - int(unsafe.Sizeof(IndexHeaderPageWithoutAttr{})))
	IMAttrItem = IMAttrSize / int(unsafe.Sizeof(AttrInfo{}))
)

type IndexHeaderPageWithoutAttr struct {
	FileHeaderPage

	FirstFreeValue RID
	RootPage       PageNum

	AttrSize int
}

type IndexHeaderPage struct {
	IndexHeaderPageWithoutAttr

	Attrs [IMAttrItem]AttrInfo
}

type IMValue struct {
	Row  RID
	Next RID
}

type IMValuePage struct {
	PageHeader

	Values [IMValueItem]IMValue
}

type IMNodePageHeader struct {
	IsLeaf   bool
	Size     int
	Capacity int

	Index     PageNum
	NextIndex PageNum
	PrevIndex PageNum
}

type IMNodePage struct {
	PageHeader

	IMNodePageHeader

	Keys    [NodeMaxItemNum]RID
	Indexes [NodeMaxItemNum]RID
}
