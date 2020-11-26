/*
Copyright (c) 2020, pigeonligh.
*/

package errorutil

import "errors"

var (
	// ErrorPagePinned is an error
	ErrorPagePinned = errors.New("the page has been pinned")

	// ErrorPageUnPinned is an error
	ErrorPageUnPinned = errors.New("the page has been unpinned")

	// ErrorPageInBuffer is an error
	ErrorPageInBuffer = errors.New("the page is already in buffer")

	// ErrorPageNotInBuffer is an error
	ErrorPageNotInBuffer = errors.New("the page is not in buffer")

	// ErrorBufferFull is an error
	ErrorBufferFull = errors.New("the buffer is full")

	// ErrorImcompleteRead is an error
	ErrorImcompleteRead = errors.New("the data is imcomplete read")

	// ErrorImcompleteWrite is an error
	ErrorImcompleteWrite = errors.New("the data is imcomplete written")

	// ErrorNotImplemented is an error
	ErrorNotImplemented = errors.New("function is not implemented")

	// ErrorFileNotOpened is an error
	ErrorFileNotOpened = errors.New("the file is not opened")

	// ErrorInvalidPage is an error
	ErrorInvalidPage = errors.New("the page is invalid")

	// ErrorEOF is an error
	ErrorEOF = errors.New("you meet EOF")

	// ErrorUnknown is an error
	ErrorUnknown = errors.New("you meet unknown error")
)
