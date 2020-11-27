package types

type SlotNum = int
type RID struct {
	Page PageNum
	Slot SlotNum
}

func (r *RID) IsValid() bool {
	return r.Slot >= 0 && r.Page > 0
}
