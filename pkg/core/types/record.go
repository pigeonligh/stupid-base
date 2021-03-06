package types

type SlotNum = int

type RID struct {
	Page PageNum
	Slot SlotNum
}

func (r *RID) Clone() *RID {
	return &RID{Page: r.Page, Slot: r.Slot}
}

func (r *RID) IsValid() bool {
	return r.Slot >= 0 && r.Page > 0
}

func (r *RID) Equal(rid *RID) bool {
	return r.Page == rid.Page && r.Slot == rid.Slot
}

func MakeRID(page PageNum, slot SlotNum) RID {
	return RID{
		Page: page,
		Slot: slot,
	}
}
