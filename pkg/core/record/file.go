package record

import (
	"github.com/pigeonligh/stupid-base/pkg/core/dsutil/bitset"
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/storage"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

type FileHandle struct {
	Filename       string
	Header         types.RecordHeaderPage
	headerModified bool
	initialized    bool
	StorageFH      *storage.FileHandle
}

func NewFileHandle(filename string) (*FileHandle, error) {
	storageFH, err := storage.GetInstance().OpenFile(filename)
	if err != nil {
		return nil, err
	}
	pageHandle, err := storageFH.GetPage(0)
	if err != nil {
		log.V(log.RecordLevel).Errorf("StorageFH.GetPage(0) failed")
		return nil, err
	}
	copiedHeader := *(*types.RecordHeaderPage)(types.ByteSliceToPointer(pageHandle.Data))
	if err := storageFH.UnpinPage(0); err != nil {
		return nil, err
	}
	return &FileHandle{
		Filename:       filename,
		Header:         copiedHeader,
		headerModified: false,
		initialized:    true,
		StorageFH:      storageFH,
	}, nil
}

func (f *FileHandle) Close() error {
	if !f.initialized || !f.headerModified {
		return nil
	} else {
		pageHandle, err := f.StorageFH.GetPage(0)
		if err != nil {
			return err
		}
		pageData := (*types.RecordHeaderPage)(types.ByteSliceToPointer(pageHandle.Data))
		*pageData = f.Header

		if err = f.StorageFH.MarkDirty(pageHandle.Page); err != nil {
			return err
		}
		if err = f.StorageFH.UnpinPage(pageHandle.Page); err != nil {
			return err
		}
		f.initialized = false
		return storage.GetInstance().CloseFile(f.Filename)
	}
}

func (f *FileHandle) AllocateFreeRID() types.RID {
	ret := types.RID{
		Page: -1,
		Slot: -1,
	}
	if f.Header.FileHeaderPage.FirstFree <= 0 {
		if err := f.insertPage(); err != nil {
			return ret
		}
	}
	freePage := f.Header.FirstFree
	pageHandle, err := f.StorageFH.GetPage(freePage)
	if err != nil {
		return ret
	}

	recordPagePtr := (*types.RecordPage)(types.ByteSliceToPointer(pageHandle.Data))
	myBitset := bitset.NewBitset(&recordPagePtr.BitsetData, f.Header.RecordPerPage)
	freeSlot := myBitset.FindLowestZeroBitIdx()
	ret.Page = freePage
	ret.Slot = freeSlot
	//log.V(log.RecordLevel).Infof("AllocateRID, (%v %v)", freePage, freeSlot)
	return ret
}

func (f *FileHandle) insertPage() error {
	pageHandle, err := f.StorageFH.NewPage(f.Header.Pages)
	if err != nil {
		return err
	}
	copy(pageHandle.Data, make([]byte, types.PageSize)) // Header.FirstFree = 0 marks for the last page
	_ = f.StorageFH.MarkDirty(pageHandle.Page)
	_ = f.StorageFH.UnpinPage(pageHandle.Page)

	f.Header.FirstFree = pageHandle.Page
	f.Header.Pages += 1
	f.headerModified = true
	log.V(log.RecordLevel).Infof("insertPage, FirstFree: %v, Pages: %v", f.Header.FirstFree, f.Header.Pages)
	return nil
}

func (f *FileHandle) getSlotByteSlice(data []byte, slot types.SlotNum) []byte {
	offset := slot * f.Header.RecordSize
	ptr := types.ByteSliceToPointerWithOffset(data, offset)
	slice := types.PointerToByteSlice(ptr, f.Header.RecordSize)
	return slice
}

func (f *FileHandle) InsertRec(data []byte) (types.RID, error) {
	if len(data) != f.Header.RecordSize {
		log.V(log.RecordLevel).Errorf("InsertRecord passed parameter len(data) won't match record size")
		return types.RID{}, errorutil.ErrorRecordLengthNotMatch
	}

	rid := f.AllocateFreeRID()

	freePage := rid.Page
	freeSlot := rid.Slot

	pageHandle, err := f.StorageFH.GetPage(freePage)
	if err != nil {
		return types.RID{}, err
	}
	recordPagePtr := (*types.RecordPage)(types.ByteSliceToPointer(pageHandle.Data))
	slotByteSlice := f.getSlotByteSlice(recordPagePtr.Data[:], freeSlot)
	copy(slotByteSlice, data)
	mybitset := bitset.NewBitset(&recordPagePtr.BitsetData, f.Header.RecordPerPage)
	mybitset.Set(freeSlot)
	if mybitset.FindLowestZeroBitIdx() == bitset.BitsetFindNoRes {
		log.V(log.RecordLevel).Infof("InsertRecord, current page(%v) full! Marked FirstFree 0", freePage)
		// current bitset if full
		if recordPagePtr.NextFree > 0 {
			f.Header.FirstFree = recordPagePtr.NextFree
			recordPagePtr.NextFree = 0
		} else {
			f.Header.FirstFree = 0 // there is no free page after this page
		}
	}

	if err := f.StorageFH.MarkDirty(rid.Page); err != nil {
		return types.RID{}, err
	}
	if err := f.StorageFH.UnpinPage(rid.Page); err != nil {
		return types.RID{}, err
	}
	f.Header.RecordNum += 1
	f.headerModified = true
	log.V(log.RecordLevel).Infof("Insert record(%v %v) succeeded!", freePage, freeSlot)
	return types.RID{
		Page: freePage,
		Slot: freeSlot,
	}, nil
}

func (f *FileHandle) DeleteRec(rid types.RID) error {
	pageHandle, err := f.StorageFH.GetPage(rid.Page)
	if err != nil {
		log.V(log.RecordLevel).Errorf("DelRecord failed: get rid(%v, %v) page fails", rid.Page, rid.Slot)
		return errorutil.ErrorRecordRidNotValid
	}
	recordPagePtr := (*types.RecordPage)(types.ByteSliceToPointer(pageHandle.Data))
	mybitset := bitset.NewBitset(&recordPagePtr.BitsetData, f.Header.RecordPerPage)
	if !mybitset.IsOccupied(rid.Slot) || !rid.IsValid() {
		log.V(log.RecordLevel).Errorf("DelRecord failed: rid(%v, %v) not valid", rid.Page, rid.Slot)
		return errorutil.ErrorRecordRidNotValid
	}

	if mybitset.FindLowestZeroBitIdx() == bitset.BitsetFindNoRes {
		if f.Header.FirstFree > 0 {
			recordPagePtr.NextFree = f.Header.FirstFree // link this page to the previous page
		} else {
			recordPagePtr.NextFree = 0 // there is no free page after this
		}
		f.Header.FirstFree = rid.Page
	}
	mybitset.Clean(rid.Slot)
	f.Header.RecordNum -= 1
	f.headerModified = true

	if err := f.StorageFH.MarkDirty(rid.Page); err != nil {
		return err
	}
	if err := f.StorageFH.UnpinPage(rid.Page); err != nil {
		return err
	}
	log.V(log.RecordLevel).Infof("DelRecord(%v %v) succeeded!", rid.Page, rid.Slot)
	return nil
}

func (f *FileHandle) GetRec(rid types.RID) (*Record, error) {
	pageHandle, err := f.StorageFH.GetPage(rid.Page)
	if err != nil {
		log.V(log.RecordLevel).Errorf("GetRecord failed: get rid(%v, %v) page fails", rid.Page, rid.Slot)
		return NewEmptyRecord(), errorutil.ErrorRecordRidNotValid
	}
	recordPagePtr := (*types.RecordPage)(types.ByteSliceToPointer(pageHandle.Data))
	mybitset := bitset.NewBitset(&recordPagePtr.BitsetData, f.Header.RecordPerPage)
	if !mybitset.IsOccupied(rid.Slot) {
		return NewEmptyRecord(), errorutil.ErrorRecordRidNotValid
	}

	slotByteSlice := f.getSlotByteSlice(recordPagePtr.Data[:], rid.Slot)
	if err := f.StorageFH.UnpinPage(pageHandle.Page); err != nil {
		return NewEmptyRecord(), err
	}

	return NewRecord(rid, slotByteSlice, f.Header.RecordSize)
}

func (f *FileHandle) GetRecList() []*Record {
	relScan := FileScan{}
	_ = relScan.OpenFullScan(f)
	recCollection := make([]*Record, 0, types.MaxAttrNums) // Though it's useful, currently it serves for db meta and table meta

	for rec, err := relScan.GetNextRecord(); rec != nil && err == nil; rec, _ = relScan.GetNextRecord() {
		recCollection = append(recCollection, rec)

	}
	return recCollection
}

// filter condition
type FilterCond struct {
	AttrSize   int
	AttrOffset int
	CompOp     types.OpType
	Value      parser.Value
}

func (f *FileHandle) GetFilteredRecList(cond FilterCond) ([]*Record, error) {
	relScan := FileScan{}
	if err := relScan.OpenScan(f, cond.Value.ValueType, cond.AttrSize, cond.AttrOffset, cond.CompOp, cond.Value); err != nil {
		return nil, err
	}
	recCollection := make([]*Record, types.MaxAttrNums) // Though it's useful, currently it serves for db meta and table meta

	for rec, err := relScan.GetNextRecord(); rec != nil; rec, _ = relScan.GetNextRecord() {
		if err != nil {
			return nil, err
		}
		recCollection = append(recCollection, rec)
	}
	return recCollection, nil
}
