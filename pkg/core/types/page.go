/*
Copyright (c) 2020, pigeonligh.
*/

package types

const (
	BitsetDataSize = PageDataSize / 4
)

// PageHeader is header structure for pages
type PageHeader struct {
	NextFree int
}

// BitsetPage is page structure for bitset
type BitsetPage struct {
	PageHeader
	Data [BitsetDataSize]uint32
}
