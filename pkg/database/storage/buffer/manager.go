/*
Copyright (c) 2020, pigeonligh.
*/

package buffer

import (
	"errors"
	"os"

	"github.com/pigeonligh/stupid-base/pkg/database/storage"
)

// Manager is the manager for page buffer
type Manager struct {
	// info on buffer pages
	Buffers []*PageDescriptor
	Slots   map[storage.PageID]int

	// pages in the buffer
	Pages int
	// size of pages in the buffer
	PageSize int

	FirstUsed int
	LastUsed  int
	FirstFree int
}

// NewManager returns a manager
func NewManager(numPages, pageSize int) *Manager {
	buffers := make([]*PageDescriptor, numPages)
	for i := 0; i < numPages; i++ {
		buffers[i] = NewDescriptor(pageSize, i)
	}
	buffers[0].Previous = InvalidSlot
	buffers[numPages-1].Next = InvalidSlot

	mg := &Manager{
		Buffers: buffers,
		Slots:   make(map[storage.PageID]int, numPages),

		Pages:    numPages,
		PageSize: pageSize,

		FirstUsed: InvalidSlot,
		LastUsed:  InvalidSlot,
		FirstFree: 0,
	}
	return mg
}

// GetPage gets a pointer to a page pinned in the buffer.
func (mg *Manager) GetPage(id storage.PageID) (storage.PageData, error) {
	var err error
	slot, found := mg.Slots[id]
	if found {
		mg.linkUsed(slot)
	} else {
		if slot, err = mg.allocSlot(); err != nil {
			return nil, err
		}
		if mg.Buffers[slot].Data, err = mg.readPage(id); err != nil {
			mg.linkFree(slot)
			return nil, err
		}
		mg.initPageDesc(id, slot)
	}

	return mg.Buffers[slot].Data, nil
}

// AllocatePage allocates a new page in the buffer
func (mg *Manager) AllocatePage(id storage.PageID) (storage.PageData, error) {
	if _, found := mg.Slots[id]; found {
		return nil, errors.New(storage.ErrorPageInBuffer)
	}
	var err error
	var slot int
	if slot, err = mg.allocSlot(); err != nil {
		return nil, err
	}
	mg.initPageDesc(id, slot)

	return mg.Buffers[slot].Data, nil
}

// MarkDirty marks page dirty
func (mg *Manager) MarkDirty(id storage.PageID) error {
	slot, found := mg.Slots[id]
	if !found {
		return errors.New(storage.ErrorPageNotInBuffer)
	}
	mg.Buffers[slot].Dirty = true
	mg.linkUsed(slot)
	return nil
}

// UnpinPage unpins a page so that it can be discarded from the buffer.
func (mg *Manager) UnpinPage(id storage.PageID) error {
	slot, found := mg.Slots[id]
	if !found {
		return errors.New(storage.ErrorPageNotInBuffer)
	}
	mg.linkUsed(slot)
	return nil
}

// FlushPages flushes pages for file
func (mg *Manager) FlushPages(file *os.File) error {
	for slot := mg.FirstUsed; slot != InvalidSlot; slot = mg.Buffers[slot].Next {
		page := mg.Buffers[slot]
		if file == page.File {
			if err := mg.clearDirty(slot); err != nil {
				return err
			}
			delete(mg.Slots, page.PageID)
			mg.linkFree(slot)
		}
	}
	return nil
}

// ForcePages forces a page to disk
func (mg *Manager) ForcePages(id storage.PageID) error {
	for slot := mg.FirstUsed; slot != InvalidSlot; slot = mg.Buffers[slot].Next {
		page := mg.Buffers[slot]
		if id == page.PageID && id.Page == storage.AllPageNum {
			if err := mg.clearDirty(slot); err != nil {
				return err
			}
		}
	}
	return nil
}

// ClearBuffer removes all entries from the Buffer Manager
func (mg *Manager) ClearBuffer() error {
	for slot := mg.FirstUsed; slot != InvalidSlot; slot = mg.Buffers[slot].Next {
		page := mg.Buffers[slot]
		delete(mg.Slots, page.PageID)
		mg.linkFree(slot)
	}
	return nil
}

// PrintBuffer displays all entries in the buffer
func (mg *Manager) PrintBuffer() {
	// TODO
}

// ResizeBuffer attempts to resize the buffer to the new size
func (mg *Manager) ResizeBuffer(newSize int) error {
	return errors.New(storage.ErrorNotImplemented)
}

// GetBlockSize returns the size of the block that can be allocated
func (mg *Manager) GetBlockSize() int {
	return mg.PageSize
}

// AllocateBlock allocates a memory chunk that lives in buffer manager
func (mg *Manager) AllocateBlock() (storage.PageID, storage.PageData, error) {
	slot, err := mg.allocSlot()
	if err != nil {
		return storage.PageID{File: nil, Page: -1}, nil, err
	}
	pageID := storage.PageID{
		File: nil,
		Page: 0, // need to create
	}
	mg.initPageDesc(pageID, slot)
	return pageID, mg.Buffers[slot].Data, nil
}

// DisposeBlock disposes of a memory chunk managed by the buffer manager
func (mg *Manager) DisposeBlock(storage.PageID) error {
	// TODO: maybe nothing is needed to do
	return nil
}
