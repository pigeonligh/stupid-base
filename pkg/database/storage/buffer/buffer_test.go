/*
Copyright (c) 2020, pigeonligh.
*/

package buffer

import (
	"testing"

	"github.com/pigeonligh/stupid-base/pkg/database/storage"
)

func TestBuffer(t *testing.T) {
	manager := NewManager(32, storage.PageSize)

	_, _, err := manager.AllocateBlock()
	if err != nil {
		t.Error(err)
	}
	return
}
