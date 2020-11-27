package types

type SlotNum = int
type RID struct {
	Page PageNum
	Slot SlotNum
}

func (r *RID) Clone() *RID {
	return &RID{Page: r.Page, Slot: r.Slot}
}
