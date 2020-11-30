/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"unsafe"

	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
)

const (
	// NodePageHeaderSize is the size of node page header
	NodePageHeaderSize = 0

	// NodePageSize is the size of node page data
	NodePageSize = types.PageDataSize - NodePageHeaderSize

	// NodeMaxItem is the max number of node items
	NodeMaxItem = NodePageSize / 2 / unsafe.Sizeof(types.RID{})

	// NodeMinItem is the min number of node items
	NodeMinItem = NodeMaxItem / 2
)

// TreeNode is node for bptree
type TreeNode struct {
	isLeaf   bool
	size     int
	capacity int

	index     types.PageNum
	nextIndex types.PageNum

	keys    [NodeMaxItem]types.RID
	indexes [NodeMaxItem]types.RID

	children [NodeMaxItem]*TreeNode
}

// NewTreeNode returns a tree node
func NewTreeNode(index types.PageNum, capacity int) *TreeNode {
	return &TreeNode{
		index:     index,
		nextIndex: types.InvalidPageNum,
		size:      0,
		capacity:  capacity,
	}
}

// Close should be called when node is deleted
func (tn *TreeNode) Close() {
	tn.index = types.InvalidPageNum
	tn.nextIndex = types.InvalidPageNum
	for i := 0; i < tn.size; i++ {
		tn.keys[i] = types.RID{}
		tn.indexes[i] = types.RID{}
		tn.children[i] = nil
	}
}

func (tn *TreeNode) updateKeyByChildren(index int) error {
	if index < 0 || index >= tn.size {
		return errorutil.ErrorBpTreeNodeOutOfBound
	}
	if tn.children[index] == nil {
		return errorutil.ErrorBpTreeNodeChildrenNotFound
	}
	tn.keys[index] = tn.children[index].keys[0]
	return nil
}

func (tn *TreeNode) prepareNode(index int, oper *Operator) error {
	if index < 0 || index >= tn.size {
		return errorutil.ErrorBpTreeNodeOutOfBound
	}
	if tn.children[index] == nil {
		var err error
		tn.children[index], err = (*oper).LoadNode(tn.indexes[index].Page)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tn *TreeNode) insertData(pos int, key, index types.RID, child *TreeNode) error {
	if pos < 0 || pos > tn.size {
		return errorutil.ErrorBpTreeNodeOutOfBound
	}
	for i := tn.size; i > pos; i-- {
		tn.keys[i] = tn.keys[i-1]
		tn.indexes[i] = tn.indexes[i-1]
		tn.children[i] = tn.children[i-1]
	}
	tn.keys[pos] = key
	tn.indexes[pos] = index
	tn.children[pos] = child
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
		tn.children[i] = tn.children[i+1]
	}
	tn.size--
	return nil
}