/*
Copyright (c) 2020, pigeonligh.
*/

package storage

import "strconv"

// PageNum uniquely identifies a page in a file
type PageNum = int64

const (
	// FileHeaderSize justifies the file header to the length of one page
	FileHeaderSize = 4096

	// PageSize is the size of a page
	PageSize = 4096

	// PageDataSize is the size of a page without header
	PageDataSize = PageSize - strconv.IntSize

	// AllPageNum is defined and used by the ForcePages method defined in RM and PF layers
	AllPageNum = -1
)
