/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

func (t *BpTree) updateRoot(root *TreeNode) error {
	err := t.operator.UpdateRoot(root)
	if err != nil {
		return err
	}
	t.root = root
	return nil
}

func (t *BpTree) insert(node *TreeNode, row *types.RID) (*TreeNode, error) {
	if node == nil {
		newNode, err := t.operator.NewNode(true)
		if err != nil {
			return nil, err
		}
		rid, err := t.operator.NewValue(*row)
		if err != nil {
			return nil, err
		}
		if err = newNode.insertData(0, *row, rid); err != nil {
			return nil, err
		}
		if err = t.operator.UpdateNode(newNode); err != nil {
			return nil, err
		}
		return newNode, nil
	}
	if node.IsLeaf {
		insertPos := -1
		for i := 0; i < node.Size; i++ {
			cmpResult, err := t.operator.CompareRows(*row, node.Keys[i])
			if err != nil {
				return nil, err
			}
			if cmpResult == 0 {
				rid, err := t.operator.PushValue(node.Indexes[i], *row)
				if err != nil {
					return nil, err
				}
				node.Indexes[i] = rid
				break
			}
			if cmpResult == 1 {
				insertPos = i
				break
			}
		}
		if insertPos != -1 {
			newIndex, err := t.operator.NewValue(*row)
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
		for i := 0; i < node.Size; i++ {
			cmpResult := 1
			if i+1 < node.Size {
				cmpResult, err = t.operator.CompareRows(*row, node.Keys[i+1])
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
			if err = node.insertData(
				insertPos, newNode.Keys[0], types.MakeRID(newNode.Index, -1),
			); err != nil {
				return nil, err
			}
		}
	}

	if node.Size == node.Capacity {
		// Split
		newNode, err := t.operator.NewNode(node.IsLeaf)
		if err != nil {
			return nil, err
		}
		target := node.Capacity / 2
		for i := target; i < node.Size; i++ {
			err = newNode.insertData(i-target, node.Keys[i], node.Indexes[i])
			if err != nil {
				return nil, err
			}
			node.Keys[i] = types.RID{}
			node.Indexes[i] = types.RID{}
		}
		node.Size = target

		newNode.PrevIndex = node.Index
		newNode.NextIndex = node.NextIndex
		node.NextIndex = newNode.Index

		nextNode, err := t.operator.LoadNode(newNode.NextIndex)
		if err != nil {
			return nil, err
		}
		if nextNode != nil {
			nextNode.PrevIndex = newNode.Index
			if err = t.operator.UpdateNode(nextNode); err != nil {
				return nil, err
			}
		}

		if err = t.operator.UpdateNode(node); err != nil {
			return nil, err
		}
		if err = t.operator.UpdateNode(newNode); err != nil {
			return nil, err
		}
		return newNode, nil
	}
	if err := t.operator.UpdateNode(node); err != nil {
		return nil, err
	}
	return nil, nil
}

func (t *BpTree) erase(node *TreeNode, row *types.RID) (bool, error) {
	if node.IsLeaf {
		for i := 0; i < node.Size; i++ {
			cmpResult, err := t.operator.CompareRows(*row, node.Keys[i])
			if err != nil {
				return false, err
			}
			if cmpResult == 0 {
				rid, err := t.operator.DeleteValue(node.Indexes[i], *row)
				if err != nil {
					return false, err
				}
				node.Indexes[i] = rid
				if rid.Page == types.InvalidPageNum {
					if err = node.eraseData(i); err != nil {
						return false, err
					}
				}
				break
			}
		}
	} else {
		var err error
		erasePos := -1
		eraseNode := false

		for i := 0; i < node.Size; i++ {
			cmpResult := 1
			if i+1 < node.Size {
				cmpResult, err = t.operator.CompareRows(*row, node.Keys[i+1])
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
	prevNode, err := t.operator.LoadNode(node.PrevIndex)
	if err != nil {
		return false, nil
	}
	if prevNode != nil && prevNode.Size+node.Size < types.NodeMinItem {
		// Merge
		for i := 0; i < node.Size; i++ {
			err = prevNode.insertData(prevNode.Size, node.Keys[i], node.Indexes[i])
			if err != nil {
				return false, err
			}
			node.Keys[i] = types.RID{}
			node.Indexes[i] = types.RID{}
		}
		node.Size = 0

		if err = t.operator.UpdateNode(prevNode); err != nil {
			return false, err
		}
	}
	if node.Size == 0 {
		nextNode, err := t.operator.LoadNode(node.NextIndex)
		if err != nil {
			return false, nil
		}
		if prevNode != nil {
			prevNode.NextIndex = node.NextIndex
		}
		if nextNode != nil {
			nextNode.PrevIndex = node.PrevIndex
		}
		if err = t.operator.UpdateNode(prevNode); err != nil {
			return false, err
		}
		if err = t.operator.UpdateNode(nextNode); err != nil {
			return false, err
		}
		if err = t.operator.DeleteNode(node); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := t.operator.UpdateNode(node); err != nil {
		return false, err
	}
	return false, nil
}

func (t *BpTree) query(node *TreeNode, key []byte, allowEqual bool) (types.PageNum, int, error) {
	if node == nil {
		return types.InvalidPageNum, -1, nil
	}

	if node.IsLeaf {
		for i := 0; i < node.Size; i++ {
			attr, err := t.operator.GetAttr(node.Keys[i])
			if err != nil {
				return types.InvalidPageNum, -1, err
			}
			cmpResult, err := t.operator.CompareAttrs(key, attr)
			if err != nil {
				return types.InvalidPageNum, -1, err
			}
			if cmpResult == 0 {
				if allowEqual {
					return node.Index, i, nil
				}
			}
			if cmpResult == 1 {
				return node.Index, i, nil
			}
		}
		return node.NextIndex, 0, nil
	}

	// Internal Node
	for i := 0; i < node.Size; i++ {
		cmpResult := 1
		if i+1 < node.Size {
			attr, err := t.operator.GetAttr(node.Keys[i+1])
			if err != nil {
				return types.InvalidPageNum, -1, err
			}
			cmpResult, err = t.operator.CompareAttrs(key, attr)
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
