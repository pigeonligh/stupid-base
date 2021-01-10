package errorutil

import "errors"

var (
	ErrorRecordLengthNotMatch = errors.New("passed record length did not match record size in header")

	ErrorRecordRidNotValid = errors.New("record rid not valid")

	ErrorRecordScanNotInit = errors.New("open scan not init yet")

	ErrorRecordScanWithNonCompOp = errors.New("open scan with non comp op")

	ErrorRecordScanValueTypeNotMatch = errors.New("open scan value type not match")

	ErrorColNotFound = errors.New("col not found")

	ErrorColDuplicated = errors.New("col duplicated")
)
