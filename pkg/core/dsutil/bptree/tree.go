/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

// BpTree is structure of bptree
type BpTree struct {
	operator Operator
	root     *TreeNode
}

// NewBpTree returns a bptree by an operator
func NewBpTree(oper Operator) (*BpTree, error) {
	log.V(log.BptreeLevel).Debug("Start to create BpTree")
	root, err := oper.LoadRoot()
	if err != nil {
		return nil, err
	}
	log.V(log.BptreeLevel).Debug("Succeeded to create BpTree")
	return &BpTree{
		operator: oper,
		root:     root,
	}, nil
}

// Close closes the bptree
func (t *BpTree) Close() error {
	// TODO: nothing
	return nil
}

// Insert adds a row into bptree
func (t *BpTree) Insert(row *types.RID) error {
	log.V(log.BptreeLevel).Debug("Insert data into BpTree")
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
			newRoot, err = t.operator.NewNode(false)
			if err != nil {
				return err
			}
			err = newRoot.insertData(0, oldRoot.Keys[0], types.MakeRID(oldRoot.Index, -1))
			if err != nil {
				return err
			}
			err = newRoot.insertData(1, newNode.Keys[0], types.MakeRID(newNode.Index, -1))
			if err != nil {
				return err
			}
		}
		err = t.updateRoot(newRoot)
		if err != nil {
			return err
		}
	}
	if t.root != nil {
		log.V(log.BptreeLevel).Debugf("Root node size: %v\n", t.root.Size)
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
	for !node.IsLeaf {
		node, err = node.getChild(0, t.operator)
		if err != nil {
			return nil, err
		}
	}
	return newIterator(t.operator, node.Index, 0), nil
}

// End get the iterator of the end
func (t *BpTree) End() (*Iterator, error) {
	return endIterator(), nil
}
