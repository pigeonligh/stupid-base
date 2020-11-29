package errorutil

import "errors"

var (
	ErrorExprInvalidComparison = errors.New("invalid comparison between two operands")

	ErrorExprNonNullNotCalculated = errors.New("non null value not calculated")

	ErrorExprNodeNotImplemented = errors.New("exp node type not implemented")
)