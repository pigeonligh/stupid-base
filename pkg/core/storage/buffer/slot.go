/*
Copyright (c) 2020, pigeonligh.
*/

package buffer

import (
	"github.com/pigeonligh/stupid-base/pkg/core/errormsg"
)

// InvalidSlot is the invalid slot
const InvalidSlot = -1

func (mg *Manager) setLink(slot, prev, next int) {
	page := mg.buffers[slot]
	if page.linked {
		mg.setUnlink(slot)
	}
	page.previous = prev
	page.next = next
	page.linked = true

	if prev != InvalidSlot {
		mg.buffers[prev].next = slot
	}
	if next != InvalidSlot {
		mg.buffers[next].previous = slot
	}
}

func (mg *Manager) setUnlink(slot int) {
	page := mg.buffers[slot]
	if !page.linked {
		return
	}

	if mg.firstUsed == slot {
		mg.firstUsed = page.next
	}
	if mg.firstFree == slot {
		mg.firstFree = page.next
	}
	if mg.lastUsed == slot {
		mg.lastUsed = page.previous
	}

	if page.next != InvalidSlot {
		mg.buffers[page.next].previous = page.previous
	}
	if page.previous != InvalidSlot {
		mg.buffers[page.previous].next = page.next
	}

	page.previous = InvalidSlot
	page.next = InvalidSlot
	page.linked = false
}

func (mg *Manager) linkFree(slot int) {
	page := mg.buffers[slot]
	if page.linked {
		mg.setUnlink(slot)
	}
	mg.setLink(slot, InvalidSlot, mg.firstFree)
	mg.firstFree = slot
}

func (mg *Manager) linkUsed(slot int) {
	page := mg.buffers[slot]
	if page.linked {
		mg.setUnlink(slot)
	}
	mg.setLink(slot, InvalidSlot, mg.firstUsed)
	mg.firstUsed = slot
	if mg.lastUsed == InvalidSlot {
		mg.lastUsed = slot
	}
}

func (mg *Manager) allocSlot() (int, error) {
	if mg.firstFree == InvalidSlot {
		slot := mg.lastUsed
		for slot != InvalidSlot && mg.buffers[slot].pinCount > 0 {
			slot = mg.buffers[slot].previous
		}
		if slot == InvalidSlot {
			return -1, errormsg.ErrorBufferFull
		}
		if err := mg.clearDirty(slot); err != nil {
			return -1, err
		}
		delete(mg.slots, mg.buffers[slot].PageID)
		mg.linkFree(slot)
	}

	slot := mg.firstFree
	mg.linkUsed(slot)

	return slot, nil
}
