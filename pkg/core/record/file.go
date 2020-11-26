package record

import (
	"unsafe"

	"github.com/pigeonligh/stupid-base/pkg/core/storage"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

type FileHandle struct {
	filename       string
	header         *types.RecordHeaderPage
	headerModified bool
	initialized    bool
	storageFH      storage.FileHandle
}

func newFileHandle(filename string, ph *storage.PageHandle) *FileHandle {
	return &FileHandle{
		filename:       filename,
		header:         (*types.RecordHeaderPage)(types.ByteSliceToPointer(ph.Data)),
		headerModified: false,
		initialized:    true,
		storageFH:      storage.FileHandle{},
	}
}

func (f *FileHandle) Close() error {
	if !f.initialized || !f.headerModified {
		return nil
	} else {
		pageHandle, err := f.storageFH.GetPage(0)
		if err != nil {
			return err
		}
		pageHandle.Data = *(*[]byte)(unsafe.Pointer(&f.header))
		_ = f.storageFH.MarkDirty(0)
		if err = f.storageFH.UnpinPage(0); err != nil {
			return err
		}
		f.initialized = false
		f.filename = ""
		return nil
	}
}

//func (f *FileHandle) AllocateFreeRID() error{
//	if f.header.FirstSparePage <= 0 {
//		page_handle, err := f.storageFH.NewPage(f.header.PageNum)
//		if err != nil {
//			return err
//		}
//
//	}
//}

//RID RM_FileHandle::AllocateFreeRID() {
//if (_header_page.first_free_page <= 0) {
//this->_insert_page();
//}
//PF_PageHandle page_handle;
//PageNum free_page = _header_page.first_free_page;
//_pf_file_handle.GetThisPage(free_page, page_handle);
//char *page_data = page_handle.GetData_SAFE();
//
//MyBitset bitset(reinterpret_cast<unsigned *>(page_data + DPAGE_BITSET_OFFSET), _header_page.record_per_page);
//SlotNum free_slot = bitset.FindLowestZeroBitIdx();
//_pf_file_handle.UnpinPage(free_page);
//
////    RM_Manager::GetInstance()._pfm.PrintBuffer();
//
//return {free_page, free_slot};
//}

//class RM_FileHandle {
//friend class RM_Manager;
//friend class RM_FileScan;
//public:
//RM_FileHandle  () = default;
//RM_FileHandle(const RM_FileHandle &) = delete;
//~RM_FileHandle();
//// Destructor
//RID AllocateFreeRID();
//RC GetRec         (const RID &rid, RM_Record &rec) const;
//// Get a record
//RC InsertRec      (const char *pData, RID &rid);       // Insert a new record,
////   return record id
//RC DeleteRec      (const RID &rid);                    // Delete a record
//RC UpdateRec      (const RM_Record &rec);              // Update a record
//RC ForcePages     (PageNum pageNum = ALL_PAGES) const; // Write dirty page(s) to disk
//inline bool IsInitialized() const{ return _initialized; }
//#ifdef DEBUG
//friend std::ostream & operator<<( std::ostream & os,const RM_FileHandle & c);
//#endif
//private:
//RM_HeaderPage _header_page;
//PF_FileHandle _pf_file_handle;
//bool _header_modified = false;
//bool _initialized = false;
//
//RC _insert_page();
//inline MyBitset _get_page_bitset(char* page_data) const;
//inline unsigned _get_slot_offset(SlotNum slot_num) const;
//inline char* _get_slot_cptr(char* page_data, SlotNum slot_num) const;
//};
