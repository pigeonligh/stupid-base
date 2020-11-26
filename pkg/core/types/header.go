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
type RecordHeaderPage struct {
	FileHeaderPage

	RecordSize    uint32
	RecordNum     uint32
	RecordPerPage uint32
	SlotMapSize   uint32
	SizeOfHeader  uint32
}
