/*
Copyright (c) 2020, pigeonligh.
*/

package types

import (
	"os"
	"unsafe"
)

const (
	// FileHeaderSize justifies the file header to the length of one page
	FileHeaderSize = 4096

	// PageSize is the size of a page
	PageSize = 4096

	// PageHeaderSize is the size of a page header
	PageHeaderSize = int(unsafe.Sizeof(PageHeader{}))

	// PageDataSize is the size of a page data
	PageDataSize = PageSize - PageHeaderSize

	// AllPageNum is defined and used by the ForcePages method defined in RM and PF layers
	AllPageNum = -1

	// InvalidPageNum is defined for invalid page
	InvalidPageNum = -1
)

// PageNum uniquely identifies a page in a file
type PageNum = int

// PageData is type of data
type PageData = []byte

// PageID saves a page's fd and PageNum
type PageID struct {
	File *os.File
	Page PageNum
}
