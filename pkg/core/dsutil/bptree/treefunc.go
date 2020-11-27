/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

func (t *BpTree) updateRoot(root *TreeNode) error {
	err := (*t.operator).UpdateRoot(root)
	if err != nil {
		return err
	}
	t.root = root
	return nil
}

func (t *BpTree) insert(node *TreeNode, row *types.RID) (*TreeNode, error) {
	if node == nil {
		// return new node
	}
	if node.isLeaf {
		insertPos := -1
		for i := 0; i < node.size; i++ {
			cmpResult := (*t.operator).CompareRows(row, node.keys[i])
			if cmpResult == 0 {
				// TODO: push value
				break
			}
			if cmpResult == 1 {
				insertPos = i
				break
			}
		}
		if insertPos != -1 {
			// TODO: new value
			var newIndex *types.RID
			err := node.insertData(insertPos, row.Clone(), newIndex, nil)
			if err != nil {
				return nil, err
			}
		}
	} else {
		var newNode *TreeNode
		var err error

		insertPos := -1
		for i := 0; i < node.size; i++ {
			cmpResult := 1
			if i+1 < node.size {
				cmpResult = (*t.operator).CompareRows(row, node.keys[i+1])
			}
			if cmpResult == 1 {
				err = node.prepareNode(i, t.operator)
				if err != nil {
					return nil, err
				}
				newNode, err = t.insert(node.children[i], row)
				if err != nil {
					return nil, err
				}
				err = node.updateKeyByChildren(i)
				if err != nil {
					return nil, err
				}
				insertPos = i + 1
				break
			}
		}
		if newNode != nil {
			node.insertData(insertPos, newNode.keys[0].Clone(), newNode.index.Clone(), newNode)
		}
	}

	if node.size == node.capacity {
		newNode, err := (*t.operator).NewNode(node.isLeaf)
		if err != nil {
			return nil, err
		}
		target := node.capacity / 2
		for i := target; i < node.size; i++ {
			err = newNode.insertData(i-target, node.keys[i], node.indexes[i], node.children[i])
			if err != nil {
				return nil, err
			}
			node.keys[i] = nil
			node.indexes[i] = nil
			node.children[i] = nil
		}
		node.size = target

		newNode.nextIndex = node.nextIndex
		node.nextIndex = newNode.index.Clone()
		return newNode, nil
	}
	return nil, nil
}

func (t *BpTree) erase(node *TreeNode, row *types.RID) (bool, error) {
	// TODO
	return false, nil
}

func (t *BpTree) query(node *TreeNode, key []byte, allowEqual bool) (*types.RID, int, bool, error) {
	// TODO
	return nil, -1, false, nil
}
