package record

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"strings"
	"testing"
)

func TestRecord(t *testing.T) {
	log.SetLevel(log.RecordLevel | log.StorageLevel)
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

	ridVec := [200]types.RID{}

	for i := 0; i < 200; i++ {
		data := make([]byte, recordSize1)
		var sb strings.Builder
		sb.Write([]byte(string(rune(i))))
		str := []byte(sb.String())
		t.Logf("Insert - %v\n", str)
		copy(data, str)

		rid, _ := f1.InsertRec(data, types.RID{})
		ridVec[i] = rid
	}
	t.Logf("%v\n", f1.header)

	for i := 0; i < 200; i++ {
		record, _ := f1.GetRec(ridVec[i])
		t.Logf("Rid(%v %v) - %v\n", ridVec[i].Page, ridVec[i].Slot, record.Data)
	}

	for i := 0; i < 200; i += 2 {
		_ = f1.DeleteRec(ridVec[i])
	}

	t.Logf("%v\n", f1.header)

	err = manager.CloseFile(filename1)
	if err != nil {
		t.Error(err)
		return
	}
}
