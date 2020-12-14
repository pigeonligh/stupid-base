package index

import (
	"github.com/pigeonligh/stupid-base/pkg/core/dsutil/bptree"
	"github.com/pigeonligh/stupid-base/pkg/core/storage"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

type operator struct {
	handle *storage.FileHandle

	pageCnt int
}

func initNode(node *bptree.TreeNode, isLeaf bool) {
	// TODO: init
}

func (oper *operator) NewNode(isLeaf bool) (*bptree.TreeNode, error) {
	page, err := oper.handle.NewPage(oper.pageCnt)
	if err != nil {
		return nil, err
	}
	oper.pageCnt++
	node, err := bptree.NewTreeNodeByData(page.Data)
	if err != nil {
		return nil, err
	}
	initNode(node, isLeaf)
	// TODO: pin
	return node, nil
}

func (oper *operator) LoadNode(pageNum types.PageNum) (*bptree.TreeNode, error) {
	page, err := oper.handle.GetPage(pageNum)
	if err != nil {
		return nil, err
	}
	node, err := bptree.NewTreeNodeByData(page.Data)
	if err != nil {
		return nil, err
	}
	// TODO: pin
	return node, nil
}

func (oper *operator) UpdateNode(node *bptree.TreeNode) error {
	page, err := oper.handle.GetPage(node.Index)
	if err != nil {
		return err
	}
	page.Data[0] = '0' // TODO: save node into data
	err = oper.handle.MarkDirty(node.Index)
	if err != nil {
		return err
	}
	return nil
}

func (oper *operator) DeleteNode(*bptree.TreeNode) error {
	// TODO
	return nil
}

func (oper *operator) LoadRoot() (*bptree.TreeNode, error) {
	// TODO: load header to get node
	pageNum := 1
	return oper.LoadNode(pageNum)
}

func (oper *operator) UpdateRoot(root *bptree.TreeNode) error {
	if err := oper.UpdateNode(root); err != nil {
		return err
	}
	// TODO: update header
	return nil
}

func (oper *operator) CompareRows(row1, row2 types.RID) (int, error) {
	attr1, err := oper.GetAttr(row1)
	if err != nil {
		return 0, err
	}
	attr2, err := oper.GetAttr(row2)
	if err != nil {
		return 0, err
	}
	return oper.CompareAttrs(attr1, attr2)
}

func (oper *operator) CompareAttrs(attr1, attr2 []byte) (int, error) {
	// TODO: compare bytes
	return 0, nil
}

func (oper *operator) GetAttr(types.RID) ([]byte, error) {
	// TODO: get attr from row
	return nil, nil
}

func (oper *operator) NewValue(types.RID) (types.RID, error) {
	return types.RID{}, nil
}

func (oper *operator) PushValue(types.RID, types.RID) (types.RID, error) {
	return types.RID{}, nil
}

func (oper *operator) DeleteValue(types.RID, types.RID) (types.RID, error) {
	return types.RID{}, nil
}

func (oper *operator) LoadValue(types.RID) (*types.IMValue, error) {
	return nil, nil
}
