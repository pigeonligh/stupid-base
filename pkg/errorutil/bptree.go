/*
Copyright (c) 2020, pigeonligh.
*/

package errorutil

import "errors"

var (
	// ErrorBpTreeNodeOutOfBound is an error
	ErrorBpTreeNodeOutOfBound = errors.New("index out of bounds in bptree node")

	// ErrorBpTreeNodeChildrenNotFound is an error
	ErrorBpTreeNodeChildrenNotFound = errors.New("children not found in bptree node")
)
