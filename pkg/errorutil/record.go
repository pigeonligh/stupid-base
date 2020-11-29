package errorutil

import "errors"

var (
	ErrorRecordLengthNotMatch = errors.New("passed record length did not match record size in header")

	ErrorRecordRidNotValid = errors.New("record rid not valid")

	ErrorRecordScanWithNonCompOp = errors.New("open scan with non comp op")

	ErrorRecordScanValueTypeNotMatch =  errors.New("open scan value type not match")
)
