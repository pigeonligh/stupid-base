package record

import (
	"github.com/pigeonligh/stupid-base/pkg/core/parser"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"math/rand"
	"testing"
	"unsafe"
)

func TestRecord(t *testing.T) {
	log.SetLevel(log.RecordLevel | log.StorageLevel | log.ExprLevel)
	manager := GetInstance()

	filename1 := "testfiles_test1.bin"
	recordSize1 := 50

	if err := manager.CreateFile(filename1, recordSize1); err != nil {
		t.Error(err)
		return
	}

	f1, err := manager.OpenFile(filename1)
	if err != nil {
		t.Error(err)
		return
	}

	//ridVec := [200]types.RID{}
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

	//nameSize := 20

	for i := 0; i < cap(ridVec); i++ {

		data := make([]byte, recordSize1)
		record := EmployerRecord{
			id:  i,
			age: rand.Int()%20 + 30,
		}
		ptr := unsafe.Pointer(&record)
		// string will always contains null-ended
		byteSlice := types.PointerToByteSlice(ptr, int(unsafe.Sizeof(record)))
		name := []byte(nameMap[rand.Int()%len(nameMap)])
		recordSlice := append(byteSlice, name...)

		copy(data, recordSlice)

		rid, _ := f1.InsertRec(data, types.RID{})
		ridVec = append(ridVec, rid)
	}
	t.Logf("%v\n", f1.header)

	for i := 0; i < 5; i++ {
		record, _ := f1.GetRec(ridVec[i])

		id := recordData2IntWithOffset(record.Data, 0)
		age := recordData2IntWithOffset(record.Data, 8)
		name := recordData2TrimmedStringWithOffset(record.Data, 16)

		t.Logf("Rid(%v %v)\n - id: %v, age: %v, name: %v\n", ridVec[i].Page, ridVec[i].Slot, id, age, name)

	}

	for i := 0; i < 200; i += 2 {
		_ = f1.DeleteRec(ridVec[i])
	}

	t.Logf("%v\n", f1.header)

	//
	fscan1 := FileScan{}
	if err = fscan1.OpenScan(f1, types.INT, 8, 0, types.OpCompLE, parser.NewValueFromInt64(20)); err != nil {
		t.Error(err)
		return
	}

	var record *Record
	t.Logf("Filtered record:")
	for {
		record, err = fscan1.GetNextRecord()
		if err != nil {
			t.Error(err)
			return
		}
		if record == nil {
			break
		}
		id := recordData2IntWithOffset(record.Data, 0)
		age := recordData2IntWithOffset(record.Data, 8)
		name := recordData2TrimmedStringWithOffset(record.Data, 16)
		t.Logf("id: %v, age: %v, name: %v\n", id, age, name)

	}

	fscan2 := FileScan{}
	if err = fscan2.OpenScan(f1, types.STRING, 20, 16, types.OpCompLE, parser.NewValueFromStr("Carol")); err != nil {
		t.Error(err)
		return
	}
	t.Logf("Filtered record:")
	for {
		record, err = fscan2.GetNextRecord()
		if err != nil {
			t.Error(err)
			return
		}
		if record == nil {
			break
		}
		id := recordData2IntWithOffset(record.Data, 0)
		age := recordData2IntWithOffset(record.Data, 8)
		name := recordData2TrimmedStringWithOffset(record.Data, 16)
		t.Logf("id: %v, age: %v, name: %v\n", id, age, name)

	}

	err = manager.CloseFile(filename1)
	if err != nil {
		t.Error(err)
		return
	}
}
