package index

import (
	"github.com/pigeonligh/stupid-base/pkg/core/dsutil/bptree"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

type Scaner struct {
	handle *FileHandle
	compOp types.OpType
	attr   []byte

	now *bptree.Iterator
	end *bptree.Iterator

	forceGet bool
}

func NewScaner(handle *FileHandle, compOp types.OpType, attr []byte) (*Scaner, error) {
	scaner := &Scaner{
		handle:   handle,
		compOp:   compOp,
		attr:     attr,
		forceGet: false,
	}
	var err error
	switch compOp {
	case types.OpCompEQ:
		if scaner.now, err = handle.tree.LowerBound(attr); err != nil {
			return nil, err
		}
		if scaner.end, err = handle.tree.UpperBound(attr); err != nil {
			return nil, err
		}
	case types.OpCompLT:
		if scaner.now, err = handle.tree.Begin(); err != nil {
			return nil, err
		}
		if scaner.end, err = handle.tree.LowerBound(attr); err != nil {
			return nil, err
		}
	case types.OpCompLE:
		if scaner.now, err = handle.tree.Begin(); err != nil {
			return nil, err
		}
		if scaner.end, err = handle.tree.UpperBound(attr); err != nil {
			return nil, err
		}
	case types.OpCompGT:
		if scaner.now, err = handle.tree.UpperBound(attr); err != nil {
			return nil, err
		}
		if scaner.end, err = handle.tree.End(); err != nil {
			return nil, err
		}
	case types.OpCompGE:
		if scaner.now, err = handle.tree.LowerBound(attr); err != nil {
			return nil, err
		}
		if scaner.end, err = handle.tree.End(); err != nil {
			return nil, err
		}
	case types.OpCompNE:
		if scaner.now, err = handle.tree.UpperBound(attr); err != nil {
			return nil, err
		}
		if scaner.end, err = handle.tree.LowerBound(attr); err != nil {
			return nil, err
		}
		scaner.forceGet = true
		if scaner.now.End() {
			if scaner.now, err = handle.tree.Begin(); err != nil {
				return nil, err
			}
			scaner.forceGet = false
		}
	}
	return scaner, nil
}

func (sc *Scaner) Close() {
	// TODO
}

func (sc *Scaner) GetNextEntry() (types.RID, error) {
	if sc.forceGet || !sc.now.EqualTo(sc.end) {
		sc.forceGet = false
		rid, err := sc.now.Get()
		if err != nil {
			return types.RID{}, err
		}
		if err = sc.now.Next(); err != nil {
			return types.RID{}, err
		}
		return rid, nil
	}
	return types.RID{}, nil
}