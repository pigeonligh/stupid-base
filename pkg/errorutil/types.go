package errorutil

import "errors"

var (
	ErrorTypesIsNotOpLogic = errors.New("op is not op logic")

	ErrorOpNotFound = errors.New("op is not found")
)
