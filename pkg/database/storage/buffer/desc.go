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

	Dirty bool

	Data types.PageData
}

// NewDescriptor returns a page descriptor
func NewDescriptor(pageSize int, index int) *PageDescriptor {
	return &PageDescriptor{
		previous: index - 1,
		next:     index + 1,
		linked:   true,

		Data: make(types.PageData, pageSize),
	}
}
