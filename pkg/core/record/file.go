package record

import (
	"github.com/pigeonligh/stupid-base/pkg/core/dsutil/bitset"
	"unsafe"

	"github.com/pigeonligh/stupid-base/pkg/core/storage"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

type FileHandle struct {
	filename       string
	header         types.RecordHeaderPage
	headerModified bool
	initialized    bool
	storageFH      *storage.FileHandle
}

func newFileHandle(filename string) (*FileHandle, error) {
	storageFH, err := storage.GetInstance().OpenFile(filename)
	if err != nil {
		return nil, err
	}
	pageHandle, err := storageFH.GetPage(0)
	if err != nil {
		return nil, err
	}
	copiedHeader := *(*types.RecordHeaderPage)(types.ByteSliceToPointer(pageHandle.Data))
	if err := storageFH.UnpinPage(0); err != nil {
		return nil, err
	}
	return &FileHandle{
		filename:       filename,
		header:         copiedHeader,
		headerModified: false,
		initialized:    true,
		storageFH:      storageFH,
	}, nil
}

func (f *FileHandle) Close() error {
	if !f.initialized || !f.headerModified {
		return nil
	} else {
		pageHandle, err := f.storageFH.GetPage(0)
		if err != nil {
			return err
		}
		pageData := (*types.RecordHeaderPage)(types.ByteSliceToPointer(pageHandle.Data))
		*pageData = f.header

		_ = f.storageFH.MarkDirty(0)
		if err = f.storageFH.UnpinPage(0); err != nil {
			return err
		}
		f.initialized = false
		f.filename = ""
		return nil
	}
}

func (f *FileHandle) AllocateFreeRID() error{
	if f.header.FileHeaderPage.FirstFree <= 0 {
		if err := f.insertPage(); err != nil {
			return err
		}
	}
	freePage := f.header.FirstFree
	pageHandle, err := f.storageFH.GetPage(freePage)
	if err != nil {
		return err
	}
	bitset := bitset.NewBitset(types.ByteSlice2uint32ArrayPtr(pageHandle.Data, int(unsafe.Sizeof(types.PageHeader{}))), f.header.RecordSize)
	freeSlot = bitset.FindLowestZeroBitIdx()
}

func (f* FileHandle) insertPage() error {
	pageHandle, err := f.storageFH.NewPage(f.header.Pages)
	if err != nil {
		return err
	}
	copy(pageHandle.Data, make([]byte, types.PageSize))	// Header.FirstFree = 0 marks for the last page
	_ = f.storageFH.MarkDirty(pageHandle.Page)
	_ = f.storageFH.UnpinPage(pageHandle.Page)

	f.header.FirstFree = pageHandle.Page
	f.header.Pages += 1
	f.headerModified = true
	return nil
}

RID RM_FileHandle::AllocateFreeRID() {
if (_header_page.first_free_page <= 0) {
this->_insert_page();
}
PF_PageHandle page_handle;
PageNum free_page = _header_page.first_free_page;
_pf_file_handle.GetThisPage(free_page, page_handle);
char *page_data = page_handle.GetData_SAFE();

MyBitset bitset(reinterpret_cast<unsigned *>(page_data + DPAGE_BITSET_OFFSET), _header_page.record_per_page);
SlotNum free_slot = bitset.FindLowestZeroBitIdx();
_pf_file_handle.UnpinPage(free_page);

//    RM_Manager::GetInstance()._pfm.PrintBuffer();

return {free_page, free_slot};
}

/**