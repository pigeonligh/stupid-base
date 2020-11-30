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
		newNode, err := (*t.operator).NewNode(true)
		if err != nil {
			return nil, err
		}
		rid, err := (*t.operator).NewValue(*row)
		newNode.insertData(0, *row, rid)
		if err = (*t.operator).UpdateNode(newNode); err != nil {
			return nil, err
		}
		return newNode, nil
	}
	if node.isLeaf {
		insertPos := -1
		for i := 0; i < node.size; i++ {
			cmpResult, err := (*t.operator).CompareRows(*row, node.keys[i])
			if err != nil {
				return nil, err
			}
			if cmpResult == 0 {
				rid, err := (*t.operator).PushValue(node.indexes[i], *row)
				if err != nil {
					return nil, err
				}
				node.indexes[i] = rid
				break
			}
			if cmpResult == 1 {
				insertPos = i
				break
			}
		}
		if insertPos != -1 {
			newIndex, err := (*t.operator).NewValue(*row)
			if err != nil {
				return nil, err
			}
			if err = node.insertData(insertPos, *row, newIndex); err != nil {
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
				cmpResult, err = (*t.operator).CompareRows(*row, node.keys[i+1])
				if err != nil {
					return nil, err
				}
			}
			if cmpResult == 1 {
				child, err := node.getChild(i, t.operator)
				if err != nil {
					return nil, err
				}
				newNode, err = t.insert(child, row)
				if err != nil {
					return nil, err
				}
				err = node.updateKey(i, child)
				if err != nil {
					return nil, err
				}
				insertPos = i + 1
				break
			}
		}
		if newNode != nil {
			node.insertData(insertPos, newNode.keys[0], types.MakeRID(newNode.index, -1))
		}
	}

	if node.size == node.capacity {
		// Split
		newNode, err := (*t.operator).NewNode(node.isLeaf)
		if err != nil {
			return nil, err
		}
		target := node.capacity / 2
		for i := target; i < node.size; i++ {
			err = newNode.insertData(i-target, node.keys[i], node.indexes[i])
			if err != nil {
				return nil, err
			}
			node.keys[i] = types.RID{}
			node.indexes[i] = types.RID{}
		}
		node.size = target

		newNode.prevIndex = node.index
		newNode.nextIndex = node.nextIndex
		node.nextIndex = newNode.index

		nextNode, err := (*t.operator).LoadNode(newNode.nextIndex)
		if err != nil {
			return nil, err
		}
		if nextNode != nil {
			nextNode.prevIndex = newNode.index
			if err = (*t.operator).UpdateNode(nextNode); err != nil {
				return nil, err
			}
		}

		if err = (*t.operator).UpdateNode(node); err != nil {
			return nil, err
		}
		if err = (*t.operator).UpdateNode(newNode); err != nil {
			return nil, err
		}
		return newNode, nil
	}
	if err := (*t.operator).UpdateNode(node); err != nil {
		return nil, err
	}
	return nil, nil
}

func (t *BpTree) erase(node *TreeNode, row *types.RID) (bool, error) {
	if node.isLeaf {
		for i := 0; i < node.size; i++ {
			cmpResult, err := (*t.operator).CompareRows(*row, node.keys[i])
			if err != nil {
				return false, err
			}
			if cmpResult == 0 {
				rid, err := (*t.operator).DeleteValue(node.indexes[i], *row)
				if err != nil {
					return false, err
				}
				node.indexes[i] = rid
				if rid.Page == types.InvalidPageNum {
					node.eraseData(i)
				}
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
				cmpResult, err = (*t.operator).CompareRows(*row, node.keys[i+1])
				if err != nil {
					return false, err
				}
			}
			if cmpResult == 1 {
				child, err := node.getChild(i, t.operator)
				if err != nil {
					return false, err
				}
				eraseNode, err = t.erase(child, row)
				if err != nil {
					return false, err
				}
				err = node.updateKey(i, child)
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
	prevNode, err := (*t.operator).LoadNode(node.prevIndex)
	if err != nil {
		return false, nil
	}
	if prevNode != nil && prevNode.size+node.size < NodeMinItem {
		// Merge
		for i := 0; i < node.size; i++ {
			err = prevNode.insertData(prevNode.size, node.keys[i], node.indexes[i])
			if err != nil {
				return false, err
			}
			node.keys[i] = types.RID{}
			node.indexes[i] = types.RID{}
		}
		node.size = 0

		if err = (*t.operator).UpdateNode(prevNode); err != nil {
			return false, err
		}
	}
	if node.size == 0 {
		nextNode, err := (*t.operator).LoadNode(node.nextIndex)
		if err != nil {
			return false, nil
		}
		if prevNode != nil {
			prevNode.nextIndex = node.nextIndex
		}
		if nextNode != nil {
			nextNode.prevIndex = node.prevIndex
		}
		if err = (*t.operator).UpdateNode(prevNode); err != nil {
			return false, err
		}
		if err = (*t.operator).UpdateNode(nextNode); err != nil {
			return false, err
		}
		if err = (*t.operator).DeleteNode(node); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := (*t.operator).UpdateNode(node); err != nil {
		return false, err
	}
	return false, nil
}

func (t *BpTree) query(node *TreeNode, key []byte, allowEqual bool) (types.PageNum, int, error) {
	if node == nil {
		return types.InvalidPageNum, -1, nil
	}

	if node.isLeaf {
		for i := 0; i < node.size; i++ {
			attr, err := (*t.operator).GetAttr(node.keys[i])
			if err != nil {
				return types.InvalidPageNum, -1, err
			}
			cmpResult, err := (*t.operator).CompareAttrs(key, attr)
			if err != nil {
				return types.InvalidPageNum, -1, err
			}
			if cmpResult == 0 {
				if allowEqual {
					return node.index, i, nil
				}
			}
			if cmpResult == 1 {
				return node.index, i, nil
			}
		}
		return node.nextIndex, 0, nil
	}

	// Internal Node
	for i := 0; i < node.size; i++ {
		cmpResult := 1
		if i+1 < node.size {
			attr, err := (*t.operator).GetAttr(node.keys[i+1])
			if err != nil {
				return types.InvalidPageNum, -1, err
			}
			cmpResult, err = (*t.operator).CompareAttrs(key, attr)
			if err != nil {
				return types.InvalidPageNum, -1, err
			}
		}
		if cmpResult == 1 {
			child, err := node.getChild(i, t.operator)
			if err != nil {
				return types.InvalidPageNum, -1, err
			}
			return t.query(child, key, allowEqual)
		}
	}
	return types.InvalidPageNum, -1, nil
}
