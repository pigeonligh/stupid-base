/*
Copyright (c) 2020, pigeonligh.
*/

package bptree

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
)

// Iterator is the iterator for bptree values
type Iterator struct {
	operator *Operator

	ended bool
}

func endIterator() *Iterator {
	return &Iterator{ended: true}
}

func newIterator(oper *Operator, nodeIndex types.PageNum, nodePos int) *Iterator {
	return &Iterator{}
}

// TODO
