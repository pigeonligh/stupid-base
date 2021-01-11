/*
Copyright (c) 2020, pigeonligh.
*/

package storage

import (
	"testing"

	"github.com/pigeonligh/stupid-base/pkg/core/env"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

func TestBuffer(t *testing.T) {
	env.SetWorkDir(".")

	manager := GetInstance()

	count := 10000
	filename1 := "testfiles_test1.bin"
	filename2 := "testfiles_test2.bin"

	if err := manager.CreateFile(filename1); err != nil {
		t.Error(err)
		return
	}
	if err := manager.CreateFile(filename2); err != nil {
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

	size := manager.GetBuffer().GetBlockSize()
	if size != types.PageSize {
		t.Error("BlockSize error")
	}

	for i := 0; i < count; i++ {
		data, err := f1.NewPage(i)
		if err != nil {
			t.Error(err)
			break
		}
		data.Data[0] = byte(48 + i)
		if err = f1.UnpinPage(data.Page); err != nil {
			t.Error(err)
			break
		}

		data, err = f2.NewPage(i)
		if err != nil {
			t.Error(err)
			break
		}
		if err = f2.UnpinPage(data.Page); err != nil {
			t.Error(err)
			break
		}
	}
	if err := f1.FlushPages(); err != nil {
		t.Error(err)
	}
	if err := f2.FlushPages(); err != nil {
		t.Error(err)
	}
	if err := manager.CloseFile(filename1); err != nil {
		t.Error(err)
	}
	if err := manager.CloseFile(filename2); err != nil {
		t.Error(err)
	}

	ff, err := manager.OpenFile(filename1)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < count; i++ {
		data, err := ff.GetPage(i)
		if err != nil {
			t.Error(err)
			break
		}
		if data.Data[0] != byte(48+i) {
			t.Error("Data error")
			break
		}
		if err = ff.UnpinPage(data.Page); err != nil {
			t.Error(err)
			break
		}
	}
	if err := manager.CloseFile(filename1); err != nil {
		t.Error(err)
	}
	if err := manager.DestroyFile(filename1); err != nil {
		t.Error(err)
	}
	if err := manager.DestroyFile(filename2); err != nil {
		t.Error(err)
	}
	return
}
