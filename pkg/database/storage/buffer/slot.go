/*
Copyright (c) 2020, pigeonligh.
*/

package buffer

// InvalidSlot is the invalid slot
const InvalidSlot = -1

func (mg *Manager) setLink(slot, prev, next int) {
	page := mg.Buffers[slot]
	if page.Linked {
		mg.setUnlink(slot)
	}
	page.Previous = prev
	page.Next = next
	page.Linked = true

	if prev != InvalidSlot {
		mg.Buffers[prev].Next = slot
	}
	if next != InvalidSlot {
		mg.Buffers[next].Previous = slot
	}
}

func (mg *Manager) setUnlink(slot int) {
	page := mg.Buffers[slot]
	if !page.Linked {
		return
	}

	if mg.FirstUsed == slot {
		mg.FirstUsed = page.Next
	}
	if mg.FirstFree == slot {
		mg.FirstFree = page.Next
	}
	if mg.LastUsed == slot {
		mg.LastUsed = page.Previous
	}

	if page.Next != InvalidSlot {
		mg.Buffers[page.Next].Previous = page.Previous
	}
	if page.Previous != InvalidSlot {
		mg.Buffers[page.Previous].Next = page.Next
	}

	page.Previous = InvalidSlot
	page.Next = InvalidSlot
	page.Linked = false
}

func (mg *Manager) linkFree(slot int) {
	mg.setLink(slot, InvalidSlot, mg.FirstFree)
	mg.FirstFree = slot
}

func (mg *Manager) linkUsed(slot int) {
	mg.setLink(slot, InvalidSlot, mg.FirstUsed)
	mg.FirstUsed = slot
	if mg.LastUsed == InvalidSlot {
		mg.LastUsed = slot
	}
}

func (mg *Manager) allocSlot() (int, error) {
	if mg.FirstFree == InvalidSlot {
		slot := mg.LastUsed
		if err := mg.clearDirty(slot); err != nil {
			return -1, err
		}
		delete(mg.Slots, mg.Buffers[slot].PageID)
		mg.linkFree(slot)
	}

	slot := mg.FirstFree
	mg.linkUsed(slot)

	return slot, nil
}
