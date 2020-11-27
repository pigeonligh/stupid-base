/*
Copyright (c) 2020, pigeonligh.
*/

package buffer

import (
	"os"

	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
)

// Manager is the manager for page buffer
type Manager struct {
	// info on buffer pages
	buffers []*PageDescriptor
	slots   map[types.PageID]int

	// pages in the buffer
	pages int
	// size of pages in the buffer
	pageSize int

	firstUsed int
	lastUsed  int
	firstFree int
}

// NewManager returns a manager
func NewManager(numPages, pageSize int) *Manager {
	buffers := make([]*PageDescriptor, numPages)
	for i := 0; i < numPages; i++ {
		buffers[i] = NewDescriptor(pageSize, i)
	}
	buffers[0].previous = InvalidSlot
	buffers[numPages-1].next = InvalidSlot

	mg := &Manager{
		buffers: buffers,
		slots:   make(map[types.PageID]int, numPages),

		pages:    numPages,
		pageSize: pageSize,

		firstUsed: InvalidSlot,
		lastUsed:  InvalidSlot,
		firstFree: 0,
	}
	return mg
}

// GetPage gets a pointer to a page pinned in the buffer.
func (mg *Manager) GetPage(id types.PageID) (types.PageData, error) {
	var err error
	slot, found := mg.slots[id]
	if found {
		mg.buffers[slot].pinCount++
		mg.linkUsed(slot)
	} else {
		if slot, err = mg.allocSlot(); err != nil {
			return nil, err
		}
		if mg.buffers[slot].data, err = mg.readPage(id); err != nil {
			mg.linkFree(slot)
			return nil, err
		}
		mg.initPageDesc(id, slot)
	}

	return mg.buffers[slot].data, nil
}

// AllocatePage allocates a new page in the buffer
func (mg *Manager) AllocatePage(id types.PageID) (types.PageData, error) {
	if _, found := mg.slots[id]; found {
		return nil, errorutil.ErrorPageInBuffer
	}
	var err error
	var slot int
	if slot, err = mg.allocSlot(); err != nil {
		return nil, err
	}
	mg.initPageDesc(id, slot)

	return mg.buffers[slot].data, nil
}

// MarkDirty marks page dirty
func (mg *Manager) MarkDirty(id types.PageID) error {
	slot, found := mg.slots[id]
	if !found {
		return errorutil.ErrorPageNotInBuffer
	}
	if mg.buffers[slot].pinCount == 0 {
		return errorutil.ErrorPageUnPinned
	}
	mg.buffers[slot].dirty = true
	mg.linkUsed(slot)
	return nil
}

// UnpinPage unpins a page so that it can be discarded from the buffer
func (mg *Manager) UnpinPage(id types.PageID) error {
	slot, found := mg.slots[id]
	if !found {
		return errorutil.ErrorPageNotInBuffer
	}
	page := mg.buffers[slot]
	if page.pinCount == 0 {
		return errorutil.ErrorPageUnPinned
	}
	page.pinCount--
	if page.pinCount == 0 {
		mg.linkUsed(slot)
	}
	return nil
}

// FlushPages flushes pages for file
func (mg *Manager) FlushPages(file *os.File) error {
	var next int
	for slot := mg.firstUsed; slot != InvalidSlot; slot = next {
		next = mg.buffers[slot].next
		page := mg.buffers[slot]
		if file == page.File {
			if page.pinCount > 0 {
				// TODO: Warn
			} else {
				if err := mg.clearDirty(slot); err != nil {
					return err
				}
				delete(mg.slots, page.PageID)
				mg.linkFree(slot)
			}
		}
	}
	return nil
}

// ForcePage forces a page to disk
func (mg *Manager) ForcePage(id types.PageID) error {
	var next int
	for slot := mg.firstUsed; slot != InvalidSlot; slot = next {
		next = mg.buffers[slot].next
		page := mg.buffers[slot]
		if id == page.PageID {
			if err := mg.clearDirty(slot); err != nil {
				return err
			}
		}
	}
	return nil
}

// ClearBuffer removes all entries from the Buffer Manager
func (mg *Manager) ClearBuffer() error {
	var next int
	for slot := mg.firstUsed; slot != InvalidSlot; slot = next {
		next = mg.buffers[slot].next
		page := mg.buffers[slot]
		if page.pinCount == 0 {
			delete(mg.slots, page.PageID)
			mg.linkFree(slot)
		}
	}
	return nil
}

// PrintBuffer displays all entries in the buffer
func (mg *Manager) PrintBuffer() {
	// TODO: print buffer
}

// ResizeBuffer attempts to resize the buffer to the new size
func (mg *Manager) ResizeBuffer(newSize int) error {
	return errorutil.ErrorNotImplemented
}

// GetBlockSize returns the size of the block that can be allocated
func (mg *Manager) GetBlockSize() int {
	return mg.pageSize
}

// AllocateBlock allocates a memory chunk that lives in buffer manager
func (mg *Manager) AllocateBlock() (types.PageID, types.PageData, error) {
	slot, err := mg.allocSlot()
	if err != nil {
		return types.PageID{File: nil, Page: -1}, nil, err
	}
	pageID := types.PageID{
		File: nil,
		Page: 0, // need to create
	}
	mg.initPageDesc(pageID, slot)
	return pageID, mg.buffers[slot].data, nil
}

// DisposeBlock disposes of a memory chunk managed by the buffer manager
func (mg *Manager) DisposeBlock(types.PageID) error {
	// TODO: maybe nothing is needed to do
	return nil
}
