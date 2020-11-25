/*
Copyright (c) 2020, pigeonligh.
*/

package buffer

import (
	"errors"

	"github.com/pigeonligh/stupid-base/pkg/database/errormsg"
	"github.com/pigeonligh/stupid-base/pkg/database/types"
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
		return nil, errors.New(errormsg.ErrorImcompleteRead)
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
		return errors.New(errormsg.ErrorImcompleteWrite)
	}
	return nil
}

func (mg *Manager) initPageDesc(pageID types.PageID, slot int) {
	mg.slots[pageID] = slot

	mg.buffers[slot].PageID = pageID
	mg.buffers[slot].Dirty = false
}

func (mg *Manager) clearDirty(slot int) error {
	page := mg.buffers[slot]
	if page.Dirty {
		if err := mg.writePage(page.PageID, page.Data); err != nil {
			return err
		}
		page.Dirty = false
	}
	return nil
}
