package record

import (
	"github.com/pigeonligh/stupid-base/pkg/core/dsutil/bitset"
	"github.com/pigeonligh/stupid-base/pkg/core/storage"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

type FileHandle struct {
	filename       string
	header         types.RecordHeaderPage
	headerModified bool
	initialized    bool
	storageFH      *storage.FileHandle
}

func NewFileHandle(filename string) (*FileHandle, error) {
	storageFH, err := storage.GetInstance().OpenFile(filename)
	if err != nil {
		return nil, err
	}
	pageHandle, err := storageFH.GetPage(0)
	if err != nil {
		log.V(log.RecordLevel).Errorf("storageFH.GetPage(0) failed")
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

		if err = f.storageFH.MarkDirty(pageHandle.Page); err != nil {
			return err
		}
		if err = f.storageFH.UnpinPage(pageHandle.Page); err != nil {
			return err
		}
		f.initialized = false
		return storage.GetInstance().CloseFile(f.filename)
	}
}

func (f *FileHandle) AllocateFreeRID() types.RID {
	ret := types.RID{
		Page: -1,
		Slot: -1,
	}
	if f.header.FileHeaderPage.FirstFree <= 0 {
		if err := f.insertPage(); err != nil {
			return ret
		}
	}
	freePage := f.header.FirstFree
	pageHandle, err := f.storageFH.GetPage(freePage)
	if err != nil {
		return ret
	}

	recordPagePtr := (*types.RecordPage)(types.ByteSliceToPointer(pageHandle.Data))
	myBitset := bitset.NewBitset(&recordPagePtr.BitsetData, f.header.RecordPerPage)
	freeSlot := myBitset.FindLowestZeroBitIdx()
	ret.Page = freePage
	ret.Slot = freeSlot
	log.V(log.RecordLevel).Infof("AllocateRID, (%v %v)", freePage, freeSlot)
	return ret
}

func (f *FileHandle) insertPage() error {
	pageHandle, err := f.storageFH.NewPage(f.header.Pages)
	if err != nil {
		return err
	}
	copy(pageHandle.Data, make([]byte, types.PageSize)) // Header.FirstFree = 0 marks for the last page
	_ = f.storageFH.MarkDirty(pageHandle.Page)
	_ = f.storageFH.UnpinPage(pageHandle.Page)

	f.header.FirstFree = pageHandle.Page
	f.header.Pages += 1
	f.headerModified = true
	log.V(log.RecordLevel).Infof("insertPage, FirstFree: %v, Pages: %v", f.header.FirstFree, f.header.Pages)
	return nil
}

func (f *FileHandle) getSlotByteSlice(data []byte, slot types.SlotNum) []byte {
	offset := slot * f.header.RecordSize
	ptr := types.ByteSliceToPointerWithOffset(data, offset)
	slice := types.PointerToByteSlice(ptr, f.header.RecordSize)
	return slice
}

func (f *FileHandle) InsertRec(data []byte, rid types.RID) (types.RID, error) {
	if len(data) != f.header.RecordSize {
		log.V(log.RecordLevel).Errorf("InsertRecord passed parameter len(data) won't match record size")
		return types.RID{}, errorutil.ErrorRecordLengthNotMatch
	}
	if !rid.IsValid() {
		rid = f.AllocateFreeRID()
	}
	freePage := rid.Page
	freeSlot := rid.Slot

	pageHandle, err := f.storageFH.GetPage(freePage)
	if err != nil {
		return types.RID{}, err
	}
	recordPagePtr := (*types.RecordPage)(types.ByteSliceToPointer(pageHandle.Data))
	slotByteSlice := f.getSlotByteSlice(recordPagePtr.Data[:], freeSlot)
	copy(slotByteSlice, data)
	mybitset := bitset.NewBitset(&recordPagePtr.BitsetData, f.header.RecordPerPage)
	mybitset.Set(freeSlot)
	if mybitset.FindLowestZeroBitIdx() == bitset.BitsetFindNoRes {
		log.V(log.RecordLevel).Infof("InsertRecord, current page(%v) full! Marked FirstFree 0", freePage)
		// current bitset if full
		if recordPagePtr.NextFree > 0 {
			f.header.FirstFree = recordPagePtr.NextFree
			recordPagePtr.NextFree = 0
		} else {
			f.header.FirstFree = 0 // there is no free page after this page
		}
	}

	if err := f.storageFH.MarkDirty(rid.Page); err != nil {
		return types.RID{}, err
	}
	if err := f.storageFH.UnpinPage(rid.Page); err != nil {
		return types.RID{}, err
	}
	f.header.RecordNum += 1
	f.headerModified = true
	log.V(log.RecordLevel).Infof("Insert record(%v %v) succeeded!", freePage, freeSlot)
	return types.RID{
		Page: freePage,
		Slot: freeSlot,
	}, nil
}

func (f *FileHandle) DeleteRec(rid types.RID) error {
	pageHandle, err := f.storageFH.GetPage(rid.Page)
	if err != nil {
		log.V(log.RecordLevel).Errorf("DelRecord failed: get rid(%v, %v) page fails", rid.Page, rid.Slot)
		return errorutil.ErrorRecordRidNotValid
	}
	recordPagePtr := (*types.RecordPage)(types.ByteSliceToPointer(pageHandle.Data))
	mybitset := bitset.NewBitset(&recordPagePtr.BitsetData, f.header.RecordPerPage)
	if !mybitset.IsOccupied(rid.Slot) || !rid.IsValid() {
		log.V(log.RecordLevel).Errorf("DelRecord failed: rid(%v, %v) not valid", rid.Page, rid.Slot)
		return errorutil.ErrorRecordRidNotValid
	}

	if mybitset.FindLowestZeroBitIdx() == bitset.BitsetFindNoRes {
		if f.header.FirstFree > 0 {
			recordPagePtr.NextFree = f.header.FirstFree // link this page to the previous page
		} else {
			recordPagePtr.NextFree = 0 // there is no free page after this
		}
		f.header.FirstFree = rid.Page
	}
	mybitset.Clean(rid.Slot)
	f.header.RecordNum -= 1
	f.headerModified = true

	if err := f.storageFH.MarkDirty(rid.Page); err != nil {
		return err
	}
	if err := f.storageFH.UnpinPage(rid.Page); err != nil {
		return err
	}
	log.V(log.RecordLevel).Infof("DelRecord(%v %v) succeeded!", rid.Page, rid.Slot)
	return nil
}

func (f *FileHandle) GetRec(rid types.RID) (*Record, error) {
	pageHandle, err := f.storageFH.GetPage(rid.Page)
	if err != nil {
		log.V(log.RecordLevel).Errorf("GetRecord failed: get rid(%v, %v) page fails", rid.Page, rid.Slot)
		return NewEmptyRecord(), errorutil.ErrorRecordRidNotValid
	}
	recordPagePtr := (*types.RecordPage)(types.ByteSliceToPointer(pageHandle.Data))
	mybitset := bitset.NewBitset(&recordPagePtr.BitsetData, f.header.RecordPerPage)
	if !mybitset.IsOccupied(rid.Slot) {
		return NewEmptyRecord(), errorutil.ErrorRecordRidNotValid
	}

	slotByteSlice := f.getSlotByteSlice(recordPagePtr.Data[:], rid.Slot)
	if err := f.storageFH.UnpinPage(pageHandle.Page); err != nil {
		return NewEmptyRecord(), err
	}

	return NewRecord(rid, slotByteSlice, f.header.RecordSize)
}
