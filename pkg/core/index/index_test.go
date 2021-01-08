package index

import (
	"math/rand"
	"testing"
	"unsafe"

	"github.com/pigeonligh/stupid-base/pkg/core/record"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
)

func testContext(rHandle *record.FileHandle, iHandle *FileHandle, t *testing.T) {
	ridVec := make([]types.RID, 0, 200)

	type EmployerRecord struct {
		id  int
		age int
	}

	nameMap := make(map[int]string)
	nameMap[0] = "Alice"
	nameMap[1] = "Bob"
	nameMap[2] = "Carol"
	nameMap[3] = "Dog"
	nameMap[4] = "Emily"
	nameMap[5] = "Fred"

	for i := 0; i < cap(ridVec); i++ {
		data := make([]byte, rHandle.GetHeader().RecordSize)
		rec := EmployerRecord{
			id:  i,
			age: rand.Int()%20 + 30,
		}
		ptr := unsafe.Pointer(&rec)
		// string will always contains null-ended
		byteSlice := types.PointerToByteSlice(ptr, int(unsafe.Sizeof(rec)))
		name := []byte(nameMap[rand.Int()%len(nameMap)])
		recordSlice := append(byteSlice, name...)

		copy(data, recordSlice)

		rid, _ := rHandle.InsertRec(data)
		if err := iHandle.InsertEntry(rid); err != nil {
			t.Error(err)
			return
		}
		ridVec = append(ridVec, rid)
	}

	for i := 0; i < cap(ridVec); i += 2 {
		t.Log(ridVec[i])
		if err := iHandle.DeleteEntry(ridVec[i]); err != nil {
			t.Error(err)
			return
		}
		_ = rHandle.DeleteRec(ridVec[i])
	}

	t.Logf("%v\n", rHandle.GetHeader())

	//
	scaner, err := NewFullScaner(iHandle)
	if err != nil {
		t.Error(err)
		return
	}

	var rid types.RID
	for {
		rid, err = scaner.GetNextEntry()
		if err != nil {
			t.Error(err)
			return
		}
		if rid.Page <= 0 {
			break
		}

		t.Log(rid)

		rec, err := rHandle.GetRec(rid)
		if err != nil {
			t.Log(rid)
			t.Error(err)
			return
		}

		id := record.RecordData2IntWithOffset(rec.Data, 0)
		age := record.RecordData2IntWithOffset(rec.Data, 8)
		name := record.RecordData2TrimmedStringWithOffset(rec.Data, 16)
		t.Logf("id: %v, age: %v, name: %v\n", id, age, name)

	}
}

func TestIndex(t *testing.T) {
	log.SetLevel(log.IndexLevel | log.StorageLevel | log.BptreeLevel)

	iManager := GetInstance()
	rManager := record.GetInstance()
	indexFilename := "testfiles_test_index.bin"
	recordFilename := "testfiles_test_record.bin"
	recordSize := 50

	attrSet := types.NewAttrSet()
	attrSet.AddSingleAttr(types.AttrInfo{
		AttrSize:    8,
		AttrOffset:  0,
		AttrType:    types.INT,
		NullAllowed: false,
	})

	if err := iManager.CreateIndex(indexFilename, *attrSet); err != nil {
		t.Error(err)
		return
	}

	if err := rManager.CreateFile(recordFilename, recordSize); err != nil {
		t.Error(err)
		return
	}

	rHandle, err := rManager.OpenFile(recordFilename)
	if err != nil {
		t.Error(err)
		return
	}

	iHandle, err := iManager.OpenIndex(indexFilename, rHandle)
	if err != nil {
		t.Error(err)
		return
	}

	testContext(rHandle, iHandle, t)

	if err = iHandle.ForcePages(); err != nil {
		t.Error(err)
		return
	}

	if err = iManager.CloseIndex(indexFilename); err != nil {
		t.Error(err)
		return
	}

	if err = rManager.CloseFile(recordFilename); err != nil {
		t.Error(err)
		return
	}
}
