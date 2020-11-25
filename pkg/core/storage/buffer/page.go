/*
Copyright (c) 2020, pigeonligh.
*/

package buffer

import (
	"github.com/pigeonligh/stupid-base/pkg/core/errormsg"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

func (mg *Manager) readPage(pageID types.PageID) (types.PageData, error) {
	offset := int64(pageID.Page)*int64(mg.pageSize) + types.FileHeaderSize
	file := pageID.File
	data := make(types.PageData, mg.pageSize)
	n, err := file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}
	if n != mg.pageSize {
		return nil, errormsg.ErrorImcompleteRead
	}
	return data, nil
}

func (mg *Manager) writePage(pageID types.PageID, data types.PageData) error {
	offset := int64(pageID.Page)*int64(mg.pageSize) + types.FileHeaderSize
	file := pageID.File
	n, err := file.WriteAt(data, offset)
	if err != nil {
		return err
	}
	if n != mg.pageSize {
		return errormsg.ErrorImcompleteWrite
	}
	return nil
}

func (mg *Manager) initPageDesc(pageID types.PageID, slot int) {
	mg.slots[pageID] = slot

	page := mg.buffers[slot]
	page.PageID = pageID
	page.dirty = false
	page.pinCount = 1
}

func (mg *Manager) clearDirty(slot int) error {
	page := mg.buffers[slot]
	if page.dirty {
		if err := mg.writePage(page.PageID, page.data); err != nil {
			return err
		}
		page.dirty = false
	}
	return nil
}
