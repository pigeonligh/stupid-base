/*
Copyright (c) 2020, pigeonligh.
*/

package storage

import (
	"errors"

	"github.com/pigeonligh/stupid-base/pkg/database/errormsg"
	"github.com/pigeonligh/stupid-base/pkg/database/types"
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

// GetData accesses the contents of a page
func (ph *PageHandle) GetData() (types.PageData, error) {
	if ph.Data == nil {
		return nil, errors.New(errormsg.ErrorPageUnPinned)
	}
	return ph.Data, nil
}

// GetPageNumber accesses the page number
func (ph *PageHandle) GetPageNumber() (int, error) {
	if ph.Data == nil {
		return -1, errors.New(errormsg.ErrorPageUnPinned)
	}
	return ph.Page, nil
}
