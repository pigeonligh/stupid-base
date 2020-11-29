/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

// Operator is the operator provided for btree
type Operator interface {
	LoadRoot() (*TreeNode, error)
	LoadNode(rid types.PageNum) (*TreeNode, error)
	NewNode(isLeaf bool) (*TreeNode, error)
	UpdateRoot(*TreeNode) error

	CompareRows(types.RID, types.RID) int
}
