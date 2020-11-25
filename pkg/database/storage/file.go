/*
Copyright (c) 2020, pigeonligh.
*/

package storage

// FileHeader is Header structure for files
type FileHeader struct {
	FirstFree int
	Number    int
}

// FileHandle is PF file interface
type FileHandle struct {
	Number int
	Data   interface{}
}
