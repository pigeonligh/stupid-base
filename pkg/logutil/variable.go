/*
Copyright (c) 2020, pigeonligh.
*/

package log

const (
	// BufferLevel is buffer log level
	BufferLevel Level = 1 << iota

	// StorageLevel is storage log level
	StorageLevel

	// RecordLevel is record log level
	RecordLevel

	// BitsetLevel is bitset log level
	BitsetLevel

	// ExprLevel
	ExprLevel

	// DbSysLevel
	DbSysLevel

	// IndexLevel
	IndexLevel
)
