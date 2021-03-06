/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

// TreeNode is node for bptree
type TreeNode struct {
	types.IMNodePage
}

// NewTreeNode returns a tree node
func NewTreeNode(index types.PageNum, capacity int) *TreeNode {
	return &TreeNode{
		IMNodePage: types.IMNodePage{
			IMNodePageHeader: types.IMNodePageHeader{
				Index:     index,
				NextIndex: types.InvalidPageNum,
				PrevIndex: types.InvalidPageNum,
				Size:      0,
				Capacity:  capacity,
			},
		},
	}
}

// NewTreeNodeByData returns a tree node
func NewTreeNodeByData(data []byte) (*TreeNode, error) {
	currentNodePage := (*types.IMNodePage)(types.ByteSliceToPointer(data))
	return &TreeNode{
		IMNodePage: *currentNodePage,
	}, nil
}

func InitTreeNode(index types.PageNum, node *TreeNode, isLeaf bool) {
	node.IsLeaf = isLeaf
	node.Size = 0
	node.Capacity = types.NodeMaxItemNum

	node.Index = index
	node.NextIndex = types.InvalidPageNum
	node.PrevIndex = types.InvalidPageNum
}

// Close should be called when node is deleted
func (tn *TreeNode) Close() {
	tn.Index = types.InvalidPageNum
	tn.NextIndex = types.InvalidPageNum
	tn.PrevIndex = types.InvalidPageNum
	for i := 0; i < tn.Size; i++ {
		tn.Keys[i] = types.RID{}
		tn.Indexes[i] = types.RID{}
	}
}

func (tn *TreeNode) getChild(pos int, oper Operator) (*TreeNode, error) {
	if pos < 0 || pos >= tn.Size {
		return nil, errorutil.ErrorBpTreeNodeOutOfBound
	}
	log.V(log.BptreeLevel).Debugf("get child %v %v %v", pos, tn.Indexes[pos].Page, tn.Keys[pos])
	return oper.LoadNode(tn.Indexes[pos].Page)
}

func (tn *TreeNode) updateKey(pos int, node *TreeNode) error {
	if pos < 0 || pos >= tn.Size {
		return errorutil.ErrorBpTreeNodeOutOfBound
	}
	tn.Keys[pos] = node.Keys[0]
	return nil
}

func (tn *TreeNode) insertData(pos int, key, index types.RID) error {
	if pos < 0 || pos > tn.Size {
		return errorutil.ErrorBpTreeNodeOutOfBound
	}
	for i := tn.Size; i > pos; i-- {
		tn.Keys[i] = tn.Keys[i-1]
		tn.Indexes[i] = tn.Indexes[i-1]
	}
	tn.Keys[pos] = key
	tn.Indexes[pos] = index
	tn.Size++
	return nil
}

func (tn *TreeNode) eraseData(pos int) error {
	if pos < 0 || pos >= tn.Size {
		return errorutil.ErrorBpTreeNodeOutOfBound
	}
	for i := pos; i < tn.Size; i++ {
		tn.Keys[i] = tn.Keys[i+1]
		tn.Indexes[i] = tn.Indexes[i+1]
	}
	tn.Size--
	return nil
}
