/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

// Operator is the operator provided for btree
type Operator interface {
	NewNode(isLeaf bool) (*TreeNode, error)
	LoadNode(types.PageNum) (*TreeNode, error)
	UpdateNode(*TreeNode) error
	DeleteNode(*TreeNode) error

	LoadRoot() (*TreeNode, error)
	UpdateRoot(*TreeNode) error

	CompareRows(types.RID, types.RID) (int, error)
	CompareAttrs([]byte, []byte) (int, error)
	GetAttr(types.RID) ([]byte, error)

	NewValue(types.RID) (types.RID, error)
	PushValue(types.RID, types.RID) (types.RID, error)
	DeleteValue(types.RID, types.RID) (types.RID, error)
	LoadValue(types.RID) (*types.IMValue, error)
}
