/*
Copyright (c) 2020, pigeonligh.
*/

package types

// PageHeader is header structure for pages
type PageHeader struct {
	NextFree int
}

// BitsetPage is page structure for bitset
type BitsetPage struct {
	PageHeader
	Data [PageDataSize / 4]uint32
}
