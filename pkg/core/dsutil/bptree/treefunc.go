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
			cmpResult := (*t.operator).CompareRows(*row, node.keys[i])
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
			var newIndex types.RID
			err := node.insertData(insertPos, *row, newIndex, nil)
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
				cmpResult = (*t.operator).CompareRows(*row, node.keys[i+1])
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
			node.insertData(insertPos, newNode.keys[0], types.RID{Page: newNode.index, Slot: 0}, newNode)
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
			node.keys[i] = types.RID{}
			node.indexes[i] = types.RID{}
			node.children[i] = nil
		}
		node.size = target

		newNode.nextIndex = node.nextIndex
		node.nextIndex = newNode.index
		return newNode, nil
	}
	return nil, nil
}

// TODO: there is someting wrong with `erase` function
// if one node is deleted, the previous node's `next` attr shoule be updated
func (t *BpTree) erase(node *TreeNode, row *types.RID) (bool, error) {
	if node.isLeaf {
		for i := 0; i < node.size; i++ {
			if cmpResult := (*t.operator).CompareRows(*row, node.keys[i]); cmpResult == 0 {
				// TODO: delete value
				break
			}
		}
	} else {
		var err error
		erasePos := -1
		eraseNode := false

		for i := 0; i < node.size; i++ {
			cmpResult := 1
			if i+1 < node.size {
				cmpResult = (*t.operator).CompareRows(*row, node.keys[i+1])
			}
			if cmpResult == 1 {
				err = node.prepareNode(i, t.operator)
				if err != nil {
					return false, err
				}
				eraseNode, err = t.erase(node.children[i], row)
				if err != nil {
					return false, err
				}
				err = node.updateKeyByChildren(i)
				if err != nil {
					return false, err
				}
				erasePos = i + 1
				break
			}
		}
		if eraseNode {
			if err = node.eraseData(erasePos); err != nil {
				return false, err
			}
		}
	}
	if node.size == 0 {
		// TODO: delete node
		return true, nil
	}
	return false, nil
}

func (t *BpTree) query(node *TreeNode, key []byte, allowEqual bool) (types.PageNum, int, error) {
	if node == nil {
		return types.InvalidPageNum, -1, nil
	}
	if node.isLeaf {
		for i := 0; i < node.size; i++ {
			// cmpResult := (*t.operator).CompareKeys(*row, node.keys[i])
			var cmpResult int // TODO
			if cmpResult == 0 {
				if allowEqual {
					return node.index, i, nil
				}
				return node.nextIndex, 0, nil
			}
			if cmpResult == 1 {
				return node.index, i, nil
			}
		}
	} else {
		for i := 0; i < node.size; i++ {
			cmpResult := 1
			if i+1 < node.size {
				// cmpResult = (*t.operator).CompareRows(*row, node.keys[i+1])
				// TODO
			}
			if cmpResult == 1 {
				if err := node.prepareNode(i, t.operator); err != nil {
					return types.InvalidPageNum, -1, err
				}
				return t.query(node.children[i], key, allowEqual)
			}
		}
	}
	return types.InvalidPageNum, -1, nil
}
