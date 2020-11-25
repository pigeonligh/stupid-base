/*
Copyright (c) 2020, pigeonligh.
*/

package storage

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

const (
	// InvalidPage is the invalid page number
	InvalidPage = -1
)

// PageHandle is PF page interface
type PageHandle struct {
	Page types.PageNum
	Data types.PageData
}

// NewPageHandle returns a page handle
func NewPageHandle() *PageHandle {
	return &PageHandle{
		Page: InvalidPage,
		Data: nil,
	}
}
