/*
Copyright (c) 2020, pigeonligh.
*/

package buffer

import (
	"github.com/pigeonligh/stupid-base/pkg/database/storage"
)

// PageDescriptor contains data about a page in the buffer
type PageDescriptor struct {
	storage.PageID

	Previous int
	Next     int
	Linked   bool

	Dirty bool

	Data storage.PageData
}

// NewDescriptor returns a page descriptor
func NewDescriptor(pageSize int, index int) *PageDescriptor {
	return &PageDescriptor{
		Previous: index - 1,
		Next:     index + 1,
		Linked:   true,

		Data: make(storage.PageData, pageSize),
	}
}
