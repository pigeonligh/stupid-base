/*
Copyright (c) 2020, pigeonligh.
*/

package storage

import (
	"errors"
	"os"

	"github.com/pigeonligh/stupid-base/pkg/database/errormsg"
	"github.com/pigeonligh/stupid-base/pkg/database/storage/buffer"
	"github.com/pigeonligh/stupid-base/pkg/database/types"
)

// FileHandle is PF file interface
type FileHandle struct {
	buffer *buffer.Manager
	file   *os.File
}

// newFileHanle returns a file handle
func newFileHanle(filename string, bm *buffer.Manager) (*FileHandle, error) {
	if bm == nil {
		return nil, errors.New(errormsg.UnknownError)
	}
	file, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &FileHandle{
		buffer: bm,
		file:   file,
	}, nil
}

// GetPage gets the page in a file
func (fh *FileHandle) GetPage(current types.PageNum) (*PageHandle, error) {
	if fh.file == nil {
		return nil, errors.New(errormsg.ErrorFileNotOpened)
	}
	if current < 0 {
		return nil, errors.New(errormsg.ErrorInvalidPage)
	}
	id := types.PageID{
		File: fh.file,
		Page: current,
	}
	pageData, err := fh.buffer.GetPage(id)
	if err != nil {
		return nil, err
	}
	return &PageHandle{Page: current, Data: pageData}, nil
}

// NewPage gets a new page in a file
func (fh *FileHandle) NewPage(current types.PageNum) (*PageHandle, error) {
	if fh.file == nil {
		return nil, errors.New(errormsg.ErrorFileNotOpened)
	}
	if current < 0 {
		return nil, errors.New(errormsg.ErrorInvalidPage)
	}
	id := types.PageID{
		File: fh.file,
		Page: current,
	}
	pageData, err := fh.buffer.AllocatePage(id)
	if err != nil {
		return nil, err
	}
	if err = fh.MarkDirty(current); err != nil {
		return nil, err
	}
	return &PageHandle{Page: current, Data: pageData}, nil
}

// DisposePage disposes of a page
func (fh *FileHandle) DisposePage(current types.PageNum) error {
	if fh.file == nil {
		return errors.New(errormsg.ErrorFileNotOpened)
	}
	return fh.MarkDirty(current)
}

// MarkDirty marks a page as being dirty
func (fh *FileHandle) MarkDirty(current types.PageNum) error {
	if fh.file == nil {
		return errors.New(errormsg.ErrorFileNotOpened)
	}
	if current < 0 {
		return errors.New(errormsg.ErrorInvalidPage)
	}
	return fh.buffer.MarkDirty(types.PageID{File: fh.file, Page: current})
}

// ForcePage forces a page to disk
func (fh *FileHandle) ForcePage(current types.PageNum) error {
	if fh.file == nil {
		return errors.New(errormsg.ErrorFileNotOpened)
	}
	if current < 0 {
		return errors.New(errormsg.ErrorInvalidPage)
	}
	return fh.buffer.ForcePage(types.PageID{File: fh.file, Page: current})
}

// FlushPages flushes all dirty pages from the buffer manager for this file
func (fh *FileHandle) FlushPages() error {
	if fh.file == nil {
		return errors.New(errormsg.ErrorFileNotOpened)
	}
	return fh.buffer.FlushPages(fh.file)
}
