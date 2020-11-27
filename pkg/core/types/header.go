/*
Copyright (c) 2020, pigeonligh.
*/

package types

// FileHeaderPage is header structure for file header pages
type FileHeaderPage struct {
	FirstFree int
	Pages     int
}

// RecordHeaderPage is file header page structure for record
// Pages marks for how many pages are there
// FirstFree marks the page has free slot
type RecordHeaderPage struct {
	FileHeaderPage
	RecordSize    int
	RecordNum     int
	RecordPerPage int
}

const (
	// BitsetDataSize is the data size of bitset
	BitsetArrayMaxLength = 32
	BitsetByteMaxSize    = 128
)

// PageHeader is header structure for pages
type PageHeader struct {
	NextFree int
}

// BitsetPageHeader is page structure for bitset
type RecordPage struct {
	PageHeader
	BitsetData [BitsetArrayMaxLength]uint32
	Data       [PageSize - PageHeaderSize - BitsetByteMaxSize]byte
}
