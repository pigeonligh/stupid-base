/*
Copyright (c) 2020, pigeonligh.
*/

package types

import "testing"

func TestRaw(t *testing.T) {
	data := make([]byte, PageSize)

	for i := 0; i < 12; i++ {
		data[i] = byte(i)
	}

	page := (*BitsetPage)(ByteSliceToPointer(data))

	t.Log(page.NextFree)
	t.Log(page.Data[0])

	// t.Error("display")
}
