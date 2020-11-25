/*
Copyright (c) 2020, pigeonligh.
*/

package storage

import "strconv"

// PageNum uniquely identifies a page in a file
type PageNum int

const (
	// PageSize is the size of a page
	PageSize = 4096

	// PageDataSize is the size of a page without header
	PageDataSize = PageSize - strconv.IntSize
)
