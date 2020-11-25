/*
Copyright (c) 2020, pigeonligh.
*/

package storage

import (
	"testing"

	"github.com/pigeonligh/stupid-base/pkg/database/types"
)

func TestBuffer(t *testing.T) {
	manager := GetInstance()

	if err := manager.CreateFile("test1.bin"); err != nil {
		t.Error(err)
		return
	}
	if err := manager.CreateFile("test2.bin"); err != nil {
		t.Error(err)
		return
	}

	f1, err := manager.OpenFile("test1.bin")
	if err != nil {
		t.Error(err)
		return
	}
	f2, err := manager.OpenFile("test2.bin")
	if err != nil {
		t.Error(err)
		return
	}

	size := manager.GetBuffer().GetBlockSize()
	if size != types.PageSize {
		t.Error("BlockSize error")
	}

	for i := 0; i < 100; i++ {
		data, err := f1.NewPage(i)
		if err != nil {
			t.Error(err)
		}
		data.Data[0] = byte(48)
		if _, err := f2.NewPage(i); err != nil {
			t.Error(err)
		}
	}
	if err := f1.FlushPages(); err != nil {
		t.Error(err)
	}
	if err := f2.FlushPages(); err != nil {
		t.Error(err)
	}
	if err := manager.CloseFile("test1.bin"); err != nil {
		t.Error(err)
	}
	if err := manager.CloseFile("test2.bin"); err != nil {
		t.Error(err)
	}

	ff, err := manager.OpenFile("test1.bin")
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 100; i++ {
		data, err := ff.GetPage(i)
		if err != nil {
			t.Error(err)
		}
		if data.Data[0] != byte(48) {
			t.Error("Data error")
		}
	}
	if err := manager.CloseFile("test1.bin"); err != nil {
		t.Error(err)
	}

	if err := manager.DestroyFile("test1.bin"); err != nil {
		t.Error(err)
	}
	if err := manager.DestroyFile("test2.bin"); err != nil {
		t.Error(err)
	}
	return
}
