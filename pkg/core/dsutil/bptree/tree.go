/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

// BpTree is structure of bptree
type BpTree struct {
	operator *Operator
	root     *TreeNode
}

// NewBpTree returns a bptree by an operator
func NewBpTree(oper *Operator) (*BpTree, error) {
	root, err := (*oper).LoadRoot()
	if err != nil {
		return nil, err
	}
	return &BpTree{
		operator: oper,
		root:     root,
	}, nil
}

// Close closes the bptree
func (t *BpTree) Close() {
	// TODO
}

// Insert adds a row into bptree
func (t *BpTree) Insert(row *types.RID) error {
	oldRoot := t.root
	newNode, err := t.insert(oldRoot, row)
	if err != nil {
		return err
	}
	if newNode != nil {
		var newRoot *TreeNode
		if t.root == nil {
			newRoot = newNode
		} else {
			newRoot, err = (*t.operator).NewNode(false)
			if err != nil {
				return err
			}
			newRoot.insertData(0, oldRoot.keys[0], types.MakeRID(oldRoot.index, -1))
			newRoot.insertData(0, newNode.keys[0], types.MakeRID(newNode.index, -1))
		}
		err = t.updateRoot(newRoot)
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete deletes a row from bptree
func (t *BpTree) Delete(row *types.RID) error {
	deleteNode, err := t.erase(t.root, row)
	if err != nil {
		return err
	}
	if deleteNode {
		return t.updateRoot(nil)
	}
	return nil
}

// LowerBound get the iterator of the first row >= key
func (t *BpTree) LowerBound(key []byte) (*Iterator, error) {
	nodeIndex, nodePos, err := t.query(t.root, key, true)
	if err != nil {
		return nil, err
	}
	if nodeIndex != types.InvalidPageNum {
		return newIterator(t.operator, nodeIndex, nodePos), nil
	}
	return endIterator(), nil
}

// UpperBound get the iterator of the first row > key
func (t *BpTree) UpperBound(key []byte) (*Iterator, error) {
	nodeIndex, nodePos, err := t.query(t.root, key, false)
	if err != nil {
		return nil, err
	}
	if nodeIndex != types.InvalidPageNum {
		return newIterator(t.operator, nodeIndex, nodePos), nil
	}
	return endIterator(), nil
}

// Begin get the iterator of the first row
func (t *BpTree) Begin() (*Iterator, error) {
	if t.root == nil {
		return endIterator(), nil
	}
	var err error
	node := t.root
	for !node.isLeaf {
		node, err = node.getChild(0, t.operator)
		if err != nil {
			return nil, err
		}
	}
	return newIterator(t.operator, node.index, 0), nil
}

// End get the iterator of the end
func (t *BpTree) End() (*Iterator, error) {
	return endIterator(), nil
}
