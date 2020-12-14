package types

import "unsafe"

const (
	// NodePageHeaderSize is the size of node page header
	NodePageHeaderSize = 0

	// NodePageSize is the size of node page data
	NodePageSize = PageDataSize - NodePageHeaderSize

	// NodeMaxItem is the max number of node items
	NodeMaxItem = NodePageSize / 2 / int(unsafe.Sizeof(RID{}))

	// NodeMinItem is the min number of node items
	NodeMinItem = NodeMaxItem / 2
)

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

	Keys    [NodeMaxItem]RID
	Indexes [NodeMaxItem]RID
}

type IMValue struct {
	Row  RID
	Next RID
}
