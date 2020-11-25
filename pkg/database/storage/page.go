/*
Copyright (c) 2020, pigeonligh.
*/

package storage

import "os"

// PageID saves a page's fd and PageNum
type PageID struct {
	File *os.File
	Page PageNum
}

// PageData is type of data
type PageData = []byte

// PageHandle is PF page interface
type PageHandle struct {
	Number int
	Data   interface{}
}
