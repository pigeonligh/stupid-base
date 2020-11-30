/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

// Iterator is the iterator for bptree values
type Iterator struct {
	operator *Operator

	nodeIndex  types.PageNum
	nodePos    int
	valueIndex types.RID

	node *TreeNode

	ended bool
}

func endIterator() *Iterator {
	return &Iterator{ended: true}
}

func newIterator(oper *Operator, nodeIndex types.PageNum, nodePos int) *Iterator {
	return &Iterator{}
}

func (iter *Iterator) prepareValue() {
	iter.valueIndex = iter.node.indexes[iter.nodePos]
	// TODO: prepare value
}

// EqualTo checks the two Iterators are equal or not
func (iter *Iterator) EqualTo(target *Iterator) bool {
	if iter.ended && target.ended {
		return true
	}
	if iter.ended != target.ended {
		return false
	}
	if iter.nodeIndex != target.nodeIndex || iter.nodePos != target.nodePos {
		return false
	}
	if iter.valueIndex != target.valueIndex {
		return false
	}
	return true
}

// Next moves the iterator's pointer to next value
func (iter *Iterator) Next() error {
	if iter.ended {
		return nil
	}
	// TODO
	return nil
}

// Get returns the value of the iterator
func (iter *Iterator) Get() (types.RID, error) {
	return types.RID{}, nil
}
