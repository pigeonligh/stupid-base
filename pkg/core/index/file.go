package index

import (
	"github.com/pigeonligh/stupid-base/pkg/core/dsutil/bptree"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

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

func (f *FileHandle) Close() error {
	if err := f.tree.Close(); err != nil {
		return err
	}
	if err := f.operator.Close(); err != nil {
		return err
	}
	return nil
}

func (f *FileHandle) InsertEntry(rid types.RID) error {
	if err := f.tree.Insert(&rid); err != nil {
		return err
	}
	if err := f.ForcePages(); err != nil {
		return err
	}
	return nil
}

func (f *FileHandle) DeleteEntry(rid types.RID) error {
	if err := f.tree.Delete(&rid); err != nil {
		return err
	}
	if err := f.ForcePages(); err != nil {
		return err
	}
	return nil
}

func (f *FileHandle) ForcePages() error {
	return nil
}

func (f *FileHandle) DeleteEntryByBatch(ridList []types.RID) error {
	for _, rid := range ridList {
		if err := f.DeleteEntry(rid); err != nil {
			return err
		}
	}
	return nil
}
