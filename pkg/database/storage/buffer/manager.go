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

	// MRU page slot
	First int
	// LRU page slot
	Last int
	// Head of free list
	Free int
}

// NewManager returns a manager
func NewManager(numPages, pageSize int) *Manager {
	buffers := []*PageDescriptor{}
	for i := 0; i < numPages; i++ {
		desc := NewDescriptor(pageSize, i)
		buffers = append(buffers, desc)
	}
	buffers[0].Previous = InvalidSlot
	buffers[numPages-1].Next = InvalidSlot

	mg := &Manager{
		Buffers: buffers,
		Slots:   map[storage.PageID]int{},

		Pages:    numPages,
		PageSize: pageSize,

		First: InvalidSlot,
		Last:  InvalidSlot,
		Free:  0,
	}
	return mg
}

// GetPage gets a pointer to a page pinned in the buffer.
func (mg *Manager) GetPage(
	file *os.File, pageNum storage.PageNum, multiplePins int,
) (*TypeData, error) {
	id := storage.PageID{
		File: file,
		Page: pageNum,
	}
	var err error
	slot, found := mg.Slots[id]
	if found {
		if multiplePins == 0 && mg.Buffers[slot].PinCount > 0 {
			return nil, errors.New(storage.ErrorPagePinned)
		}
		mg.Buffers[slot].PinCount++

		if err = mg.unlink(slot); err != nil {
			return nil, err
		}
		if err = mg.linkhead(slot); err != nil {
			return nil, err
		}
	} else {
		slot, err = mg.alloc()
		if err != nil {
			return nil, err
		}

	}

	return nil, nil
}

func (mg *Manager) unlink(slot int) error {
	return nil
}

func (mg *Manager) linkhead(slot int) error {
	return nil
}

func (mg *Manager) alloc() (int, error) {
	return -1, nil
}
