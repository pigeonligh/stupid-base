/*
Copyright (c) 2020, pigeonligh.
*/

package buffer

import (
	"os"

	"github.com/pigeonligh/stupid-base/pkg/database/storage"
)

// TypeData is type of data
type TypeData = []byte

// PageDescriptor contains data about a page in the buffer
type PageDescriptor struct {
	File *os.File

	Previous int
	Next     int

	Dirty    int
	PinCount int

	Page storage.PageNum

	Data TypeData
}

// NewDescriptor returns a page descriptor
func NewDescriptor(pageSize int, index int) *PageDescriptor {
	return &PageDescriptor{
		Previous: index - 1,
		Next:     index + 1,

		Data: make(TypeData, pageSize),
	}
}
