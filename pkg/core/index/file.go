package index

import "github.com/pigeonligh/stupid-base/pkg/core/dsutil/bptree"

type FileHandle struct {
	operator *Operator
	tree     *bptree.BpTree
}

func NewFileHandle(operator *Operator) (*FileHandle, error) {
	tree, err := bptree.NewBpTree(operator)
	if err != nil {
		return nil, err
	}
	return &FileHandle{
		operator: operator,
		tree:     tree,
	}, nil
}
