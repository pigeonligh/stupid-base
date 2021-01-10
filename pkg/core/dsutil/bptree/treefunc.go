/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

func (t *BpTree) updateRoot(root *TreeNode) error {
	log.V(log.BptreeLevel).Debug("Update Root")
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
		insertPos := node.Size
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
				insertPos = -1
				break
			}
			if cmpResult == 1 {
				insertPos = i
				break
			}
		}
		if insertPos != -1 {
			log.V(log.BptreeLevel).Debugf("Insert data in leaf, pos: %d", insertPos)
			newIndex, err := t.operator.NewValue(*row)
			if err != nil {
				return nil, err
			}
			if err = node.insertData(insertPos, *row, newIndex); err != nil {
				return nil, err
			}
		} else {
			log.V(log.BptreeLevel).Debugf("Add data in leaf")
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
			log.V(log.BptreeLevel).Debugf("Insert data in internal, pos: %d %d", insertPos, node.Size)
			for i := 0; i < node.Size; i++ {
				log.V(log.BptreeLevel).Debugf("%v", node.Keys[i])
			}
		} else {
			log.V(log.BptreeLevel).Debugf("Add data in internal, pos: %d", insertPos-1)
		}
	}

	if node.Size == node.Capacity {
		// Split
		log.V(log.BptreeLevel).Debug("Split node")
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
		// log.V(log.BptreeLevel).Debugf("to erase in leaf %d", node.Size)
		for i := 0; i < node.Size; i++ {
			// log.V(log.BptreeLevel).Debugf("compare %v with %v", *row, node.Keys[i])
			cmpResult, err := t.operator.CompareRows(*row, node.Keys[i])
			if err != nil {
				return false, err
			}
			if cmpResult == 0 {
				rid, err := t.operator.DeleteValue(node.Indexes[i], *row)
				if err != nil {
					return false, err
				}
				// log.V(log.BptreeLevel).Debugf("deleted %v and left %v", *row, rid)
				node.Indexes[i] = rid
				if rid.Page <= 0 {
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
				// log.V(log.BptreeLevel).Debugf("compare %v with %v", *row, node.Keys[i+1])
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
				log.V(log.BptreeLevel).Debugf("update Key: %v %v", i, node.Keys[i])
				if err != nil {
					return false, err
				}
				erasePos = i
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
		log.V(log.BptreeLevel).Debugf("Node merged")
	}
	if node.Size == 0 {
		nextNode, err := t.operator.LoadNode(node.NextIndex)
		if err != nil {
			return false, err
		}
		if prevNode != nil {
			prevNode.NextIndex = node.NextIndex
			if err = t.operator.UpdateNode(prevNode); err != nil {
				return false, err
			}
		}
		if nextNode != nil {
			nextNode.PrevIndex = node.PrevIndex
			if err = t.operator.UpdateNode(nextNode); err != nil {
				return false, err
			}
		}
		if err = t.operator.DeleteNode(node); err != nil {
			return false, err
		}
		log.V(log.BptreeLevel).Debugf("Node deleted")
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
