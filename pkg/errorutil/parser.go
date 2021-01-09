package errorutil

import "errors"

var (
	ErrorUnknownCommand = errors.New("unknown command")

	ErrorParseCommand = errors.New("failed to parse command")
)
