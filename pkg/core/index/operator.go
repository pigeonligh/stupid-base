package index

import (
	"github.com/pigeonligh/stupid-base/pkg/core/dsutil/bptree"
	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/storage"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

type Operator struct {
	Filename string

	iHandle *storage.FileHandle
	rHandle *record.FileHandle

	headerPage     types.IndexHeaderPage
	headerModified bool
	initialized    bool

	attr *types.AttrSet
}

func NewOperator(filename string, record *record.FileHandle, attr *types.AttrSet) (*Operator, error) {
	// Make sure the file is created, please
	handle, err := storage.GetInstance().OpenFile(filename)
	if err != nil {
		return nil, err
	}
	headerPage, err := handle.NewPage(0)
	if err != nil {
		log.V(log.IndexLevel).Errorf("handle.NewPage(0) failed")
		return nil, err
	}
	currentHeader := (*types.IndexHeaderPage)(types.ByteSliceToPointer(headerPage.Data))
	currentHeader.FirstFree = 0
	currentHeader.FirstFreeValue = types.RID{}
	currentHeader.Pages = 1
	currentHeader.RootPage = types.InvalidPageNum
	attr.WriteAttrToHeader(currentHeader)
	if err := handle.MarkDirty(0); err != nil {
		return nil, err
	}
	if err := handle.UnpinPage(0); err != nil {
		return nil, err
	}
	return &Operator{
		Filename:       filename,
		iHandle:        handle,
		rHandle:        record,
		headerPage:     *currentHeader,
		headerModified: false,
		initialized:    true,
		attr:           types.LoadAttrFromHeader(currentHeader),
	}, nil
}

func LoadOperator(filename string, record *record.FileHandle) (*Operator, error) {
	handle, err := storage.GetInstance().OpenFile(filename)
	if err != nil {
		return nil, err
	}
	headerPage, err := handle.GetPage(0)
	if err != nil {
		log.V(log.IndexLevel).Errorf("handle.GetPage(0) failed")
		return nil, err
	}
	copiedHeader := *(*types.IndexHeaderPage)(types.ByteSliceToPointer(headerPage.Data))
	if err := handle.UnpinPage(0); err != nil {
		return nil, err
	}
	return &Operator{
		Filename:       filename,
		iHandle:        handle,
		rHandle:        record,
		headerPage:     copiedHeader,
		headerModified: false,
		initialized:    true,
		attr:           types.LoadAttrFromHeader(&copiedHeader),
	}, nil
}

func (oper *Operator) Close() error {
	if !oper.initialized || !oper.headerModified {
		return nil
	}
	pageHandle, err := oper.iHandle.GetPage(0)
	if err != nil {
		return err
	}
	pageData := (*types.IndexHeaderPage)(types.ByteSliceToPointer(pageHandle.Data))
	*pageData = oper.headerPage

	if err = oper.iHandle.MarkDirty(pageHandle.Page); err != nil {
		return err
	}
	if err = oper.iHandle.UnpinPage(pageHandle.Page); err != nil {
		return err
	}
	oper.initialized = false
	oper.headerPage = types.IndexHeaderPage{}
	return nil
}

func (oper *Operator) NewNode(isLeaf bool) (*bptree.TreeNode, error) {
	log.V(log.BptreeLevel).Debug("New Node")
	var page *storage.PageHandle
	var err error
	if oper.headerPage.FirstFree != 0 {
		page, err = oper.iHandle.GetPage(oper.headerPage.FirstFree)
		if err != nil {
			return nil, err
		}
		currentPage := (*types.IMNodePage)(types.ByteSliceToPointer(page.Data))
		oper.headerPage.FirstFree = currentPage.NextFree
		oper.headerModified = true
	} else {
		page, err = oper.iHandle.NewPage(oper.headerPage.Pages + 1)
		if err != nil {
			return nil, err
		}
		oper.headerPage.Pages++
		oper.headerModified = true
	}
	node, err := bptree.NewTreeNodeByData(page.Data)
	if err != nil {
		return nil, err
	}
	bptree.InitTreeNode(page.Page, node, isLeaf)
	if err = oper.iHandle.UnpinPage(page.Page); err != nil {
		return nil, err
	}
	return node, nil
}

func (oper *Operator) LoadNode(pageNum types.PageNum) (*bptree.TreeNode, error) {
	// log.V(log.BptreeLevel).Debug("Load Node")
	if pageNum <= 0 || pageNum > oper.headerPage.Pages {
		log.V(log.BptreeLevel).Warningf("Loading valid page(%d), skip.", pageNum)
		return nil, nil
	}
	page, err := oper.iHandle.GetPage(pageNum)
	if err != nil {
		return nil, err
	}
	node, err := bptree.NewTreeNodeByData(page.Data)
	if err != nil {
		return nil, err
	}
	if err = oper.iHandle.UnpinPage(page.Page); err != nil {
		return nil, err
	}
	return node, nil
}

func (oper *Operator) UpdateNode(node *bptree.TreeNode) error {
	page, err := oper.iHandle.GetPage(node.Index)
	if err != nil {
		return err
	}
	currentPage := (*types.IMNodePage)(types.ByteSliceToPointer(page.Data))
	*currentPage = node.IMNodePage
	err = oper.iHandle.MarkDirty(node.Index)
	if err != nil {
		return err
	}
	err = oper.iHandle.UnpinPage(node.Index)
	if err != nil {
		return err
	}
	return nil
}

func (oper *Operator) DeleteNode(node *bptree.TreeNode) error {
	page, err := oper.iHandle.GetPage(node.Index)
	if err != nil {
		return err
	}

	currentPage := (*types.IMNodePage)(types.ByteSliceToPointer(page.Data))
	currentPage.NextFree = oper.headerPage.FirstFree
	err = oper.iHandle.MarkDirty(node.Index)
	if err != nil {
		return err
	}
	err = oper.iHandle.UnpinPage(node.Index)
	if err != nil {
		return err
	}

	oper.headerPage.FirstFree = node.Index
	oper.headerModified = true
	return nil
}

func (oper *Operator) LoadRoot() (*bptree.TreeNode, error) {
	if oper.headerPage.RootPage == types.InvalidPageNum {
		return nil, nil
	}
	return oper.LoadNode(oper.headerPage.RootPage)
}

func (oper *Operator) UpdateRoot(root *bptree.TreeNode) error {
	if err := oper.UpdateNode(root); err != nil {
		return err
	}
	oper.headerPage.RootPage = root.Index
	oper.headerModified = true
	return nil
}

func (oper *Operator) CompareRows(row1, row2 types.RID) (int, error) {
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

func (oper *Operator) CompareAttrs(attr1, attr2 []byte) (int, error) {
	return compareBytes(attr1, attr2), nil
}

func (oper *Operator) GetAttr(rid types.RID) ([]byte, error) {
	record, err := oper.rHandle.GetRec(rid)
	if err != nil {
		log.V(log.BptreeLevel).Debugf("Read RID %v failed\n", rid)
		return nil, err
	}
	return oper.attr.DataToAttrs(rid, record.Data), nil
}

func (oper *Operator) createFreeValues() error {
	var page *storage.PageHandle
	var err error
	if oper.headerPage.FirstFree != 0 {
		page, err = oper.iHandle.GetPage(oper.headerPage.FirstFree)
		if err != nil {
			return err
		}
		currentPage := (*types.IMNodePage)(types.ByteSliceToPointer(page.Data))
		oper.headerPage.FirstFree = currentPage.NextFree
		oper.headerModified = true
	} else {
		page, err = oper.iHandle.NewPage(oper.headerPage.Pages + 1)
		if err != nil {
			return err
		}
		oper.headerPage.Pages++
		oper.headerModified = true
	}
	oper.headerPage.FirstFreeValue = initValuePage(page)
	oper.headerModified = true

	err = oper.iHandle.MarkDirty(page.Page)
	if err != nil {
		return err
	}
	err = oper.iHandle.UnpinPage(page.Page)
	if err != nil {
		return err
	}
	return nil
}

func (oper *Operator) getIndexedValue(index types.RID) (*IndexedValue, error) {
	page, err := oper.iHandle.GetPage(index.Page)
	if err != nil {
		return nil, err
	}
	value := getValue(index, page.Data)
	if err = oper.iHandle.UnpinPage(index.Page); err != nil {
		return nil, err
	}
	return value, nil
}

func (oper *Operator) updateIndexedValue(value *IndexedValue) error {
	page, err := oper.iHandle.GetPage(value.index.Page)
	if err != nil {
		return err
	}
	setValue(value, page.Data)

	err = oper.iHandle.MarkDirty(page.Page)
	if err != nil {
		return err
	}
	err = oper.iHandle.UnpinPage(page.Page)
	if err != nil {
		return err
	}
	return nil
}

func (oper *Operator) NewValue(row types.RID) (types.RID, error) {
	return oper.PushValue(types.RID{}, row)
}

func (oper *Operator) PushValue(first types.RID, row types.RID) (types.RID, error) {
	firstIndex := oper.headerPage.FirstFreeValue
	if !firstIndex.IsValid() {
		err := oper.createFreeValues()
		if err != nil {
			return types.RID{}, err
		}
		firstIndex = oper.headerPage.FirstFreeValue
	}

	value, err := oper.getIndexedValue(firstIndex)
	if err != nil {
		return types.RID{}, err
	}

	oper.headerPage.FirstFreeValue = value.Next
	oper.headerModified = true

	value.Row = row
	value.Next = first
	value.index = firstIndex

	err = oper.updateIndexedValue(value)
	if err != nil {
		return types.RID{}, err
	}

	return value.index, nil
}

func (oper *Operator) deleteValue(index *IndexedValue, prev *IndexedValue) error {
	if prev != nil {
		prev.Next = index.Next
		if err := oper.updateIndexedValue(prev); err != nil {
			return err
		}
	}

	index.Next = oper.headerPage.FirstFreeValue
	oper.headerPage.FirstFreeValue = index.index
	oper.headerModified = true

	if err := oper.updateIndexedValue(index); err != nil {
		return err
	}
	return nil
}

func (oper *Operator) DeleteValue(first types.RID, row types.RID) (types.RID, error) {
	value, err := oper.getIndexedValue(first)
	if err != nil {
		return types.RID{}, err
	}
	if value.Row.Equal(&row) {
		next := value.Next
		err = oper.deleteValue(value, nil)
		if err != nil {
			return types.RID{}, err
		}
		return next, nil
	}
	for value.Next.IsValid() {
		next, err := oper.getIndexedValue(value.Next)
		if err != nil {
			return types.RID{}, err
		}
		if next.Row.Equal(&row) {
			err = oper.deleteValue(next, value)
			if err != nil {
				return types.RID{}, err
			}
			return first, nil
		}
		value = next
	}
	return types.RID{}, nil // delete nothing
}

func (oper *Operator) LoadValue(index types.RID) (*types.IMValue, error) {
	value, err := oper.getIndexedValue(index)
	if err != nil {
		return nil, err
	}
	return &value.IMValue, nil
}
