/*
Copyright (c) 2020, pigeonligh.
*/

package buffer

import (
	"github.com/pigeonligh/stupid-base/pkg/database/types"
)

// PageDescriptor contains data about a page in the buffer
type PageDescriptor struct {
	types.PageID

	previous int
	next     int
	linked   bool

	pinCount int
	dirty    bool

	data types.PageData
}

// NewDescriptor returns a page descriptor
func NewDescriptor(pageSize int, index int) *PageDescriptor {
	return &PageDescriptor{
		previous: index - 1,
		next:     index + 1,
		linked:   true,

		pinCount: 1,
		dirty:    false,

		data: make(types.PageData, pageSize),
	}
}
