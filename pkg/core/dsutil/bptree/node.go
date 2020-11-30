/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
)

// TreeNode is node for bptree
type TreeNode struct {
	isLeaf   bool
	size     int
	capacity int

	index     types.PageNum
	nextIndex types.PageNum
	prevIndex types.PageNum

	keys    [types.NodeMaxItem]types.RID
	indexes [types.NodeMaxItem]types.RID
}

// NewTreeNode returns a tree node
func NewTreeNode(index types.PageNum, capacity int) *TreeNode {
	return &TreeNode{
		index:     index,
		nextIndex: types.InvalidPageNum,
		prevIndex: types.InvalidPageNum,
		size:      0,
		capacity:  capacity,
	}
}

// Close should be called when node is deleted
func (tn *TreeNode) Close() {
	tn.index = types.InvalidPageNum
	tn.nextIndex = types.InvalidPageNum
	tn.prevIndex = types.InvalidPageNum
	for i := 0; i < tn.size; i++ {
		tn.keys[i] = types.RID{}
		tn.indexes[i] = types.RID{}
	}
}

func (tn *TreeNode) getChild(pos int, oper *Operator) (*TreeNode, error) {
	if pos < 0 || pos >= tn.size {
		return nil, errorutil.ErrorBpTreeNodeOutOfBound
	}
	return (*oper).LoadNode(tn.indexes[pos].Page)
}

func (tn *TreeNode) updateKey(pos int, node *TreeNode) error {
	if pos < 0 || pos >= tn.size {
		return errorutil.ErrorBpTreeNodeOutOfBound
	}
	tn.keys[pos] = node.keys[0]
	return nil
}

func (tn *TreeNode) insertData(pos int, key, index types.RID) error {
	if pos < 0 || pos > tn.size {
		return errorutil.ErrorBpTreeNodeOutOfBound
	}
	for i := tn.size; i > pos; i-- {
		tn.keys[i] = tn.keys[i-1]
		tn.indexes[i] = tn.indexes[i-1]
	}
	tn.keys[pos] = key
	tn.indexes[pos] = index
	tn.size++
	return nil
}

func (tn *TreeNode) eraseData(pos int) error {
	if pos < 0 || pos >= tn.size {
		return errorutil.ErrorBpTreeNodeOutOfBound
	}
	for i := pos; i < tn.size; i++ {
		tn.keys[i] = tn.keys[i+1]
		tn.indexes[i] = tn.indexes[i+1]
	}
	tn.size--
	return nil
}
