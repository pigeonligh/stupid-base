package record

import (
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"testing"
)

func TestRecord(t *testing.T) {
	log.SetLevel(log.RecordLevel | log.StorageLevel)

	manager := GetInstance()


	filename1 := "testfiles_test1.bin"
	recordSize1 := 50
	filename2 := "testfiles_test2.bin"
	recordSize2 := 100

	if err := manager.CreateFile(filename1, recordSize1); err != nil {
		t.Error(err)
		return
	}
	if err := manager.CreateFile(filename2, recordSize2); err != nil {
		t.Error(err)
		return
	}

	f1, err := manager.OpenFile(filename1)
	if err != nil {
		t.Error(err)
		return
	}
	f2, err := manager.OpenFile(filename2)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("%v\n", f1.header)
	t.Logf("%v\n", f2.header)
}