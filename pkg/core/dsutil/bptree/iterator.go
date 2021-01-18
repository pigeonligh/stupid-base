/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

// Iterator is the iterator for bptree values
type Iterator struct {
	operator Operator

	nodeIndex  types.PageNum
	nodePos    int
	valueIndex types.RID

	ended bool
}

func endIterator() *Iterator {
	return &Iterator{ended: true}
}

func newIterator(oper Operator, nodeIndex types.PageNum, nodePos int) *Iterator {
	node, err := oper.LoadNode(nodeIndex)
	if err != nil {
		// TODO: Warning
		return endIterator()
	}
	if nodePos >= node.Size {
		// TODO: Warning
		return endIterator()
	}
	return &Iterator{
		operator:   oper,
		nodeIndex:  nodeIndex,
		nodePos:    nodePos,
		valueIndex: node.Indexes[nodePos],
		ended:      false,
	}
}

func (iter *Iterator) getNode() (*TreeNode, error) {
	return iter.operator.LoadNode(iter.nodeIndex)
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
	nowValue, err := iter.get()
	if err != nil {
		return err
	}
	nextIndex := nowValue.Next
	if !nextIndex.IsValid() {
		node, err := iter.getNode()
		if err != nil {
			return err
		}
		iter.nodePos++
		if iter.nodePos == node.Size {
			iter.nodeIndex = node.NextIndex
			iter.nodePos = 0
			node, err = iter.getNode()
			if err != nil {
				return err
			}
			if node == nil {
				iter.ended = true
				return nil
			}
		}
		nextIndex = node.Indexes[iter.nodePos]
	}
	iter.valueIndex = nextIndex
	return nil
}

// Get returns the value of the iterator
func (iter *Iterator) Get() (types.RID, error) {
	if iter.ended {
		return types.RID{}, nil
	}
	nowValue, err := iter.get()
	if err != nil {
		return types.RID{}, err
	}
	return nowValue.Row, nil
}

func (iter *Iterator) get() (*types.IMValue, error) {
	return iter.operator.LoadValue(iter.valueIndex)
}

func (iter *Iterator) End() bool {
	return iter.ended
}
