/*
Copyright (c) 2020, pigeonligh.
*/

package buffer

import (
	"errors"

	"github.com/pigeonligh/stupid-base/pkg/database/storage"
)

func (mg *Manager) readPage(pageID storage.PageID) (storage.PageData, error) {
	offset := int64(pageID.Page)*int64(mg.PageSize) + storage.FileHeaderSize
	file := pageID.File
	data := make(storage.PageData, mg.PageSize)
	n, err := file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}
	if n != mg.PageSize {
		return nil, errors.New(storage.ErrorImcompleteRead)
	}
	return data, nil
}

func (mg *Manager) writePage(pageID storage.PageID, data storage.PageData) error {
	offset := int64(pageID.Page)*int64(mg.PageSize) + storage.FileHeaderSize
	file := pageID.File
	n, err := file.WriteAt(data, offset)
	if err != nil {
		return err
	}
	if n != mg.PageSize {
		return errors.New(storage.ErrorImcompleteWrite)
	}
	return nil
}

func (mg *Manager) initPageDesc(pageID storage.PageID, slot int) {
	mg.Slots[pageID] = slot

	mg.Buffers[slot].PageID = pageID
	mg.Buffers[slot].Dirty = false
}

func (mg *Manager) clearDirty(slot int) error {
	page := mg.Buffers[slot]
	if page.Dirty {
		if err := mg.writePage(page.PageID, page.Data); err != nil {
			return err
		}
		page.Dirty = false
	}
	return nil
}
