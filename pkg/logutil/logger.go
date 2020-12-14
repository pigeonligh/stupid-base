/*
Copyright (c) 2020, pigeonligh.
*/

package log

import (
	"fmt"
	"log"
	"runtime"
	"strings"

	"github.com/pigeonligh/stupid-base/pkg"
)

const (
	prefixString string = ""
	suffixString string = ""
)

// Logger is used to log
type Logger struct {
	debugLogger   *log.Logger
	infoLogger    *log.Logger
	warningLogger *log.Logger
	errorLogger   *log.Logger

	depth int
}

func (logger *Logger) prefix() string {
	_, file, line, ok := runtime.Caller(logger.depth + 1)
	if !ok {
		return "???: "
	}
	return fmt.Sprintf("%s:%d: ", strings.TrimPrefix(file, pkg.ProjectPath), line)
}

func (logger *Logger) wrap(s string) string {
	return logger.prefix() + prefixString + s + suffixString
}

// Debug logs important message
func (logger *Logger) Debug(v ...interface{}) {
	if logger.debugLogger != nil && mode == modeDebug {
		logger.debugLogger.Output(logger.depth, logger.wrap(fmt.Sprintln(v...)))
	}
}

// Debugf logs important message
func (logger *Logger) Debugf(format string, v ...interface{}) {
	if logger.debugLogger != nil && mode == modeDebug {
		logger.debugLogger.Output(logger.depth, logger.wrap(fmt.Sprintf(format, v...)+"\n"))
	}
}

// Info logs important message
func (logger *Logger) Info(v ...interface{}) {
	if logger.infoLogger != nil {
		logger.infoLogger.Output(logger.depth, logger.wrap(fmt.Sprintln(v...)))
	}
}

// Infof logs important message
func (logger *Logger) Infof(format string, v ...interface{}) {
	if logger.infoLogger != nil {
		logger.infoLogger.Output(logger.depth, logger.wrap(fmt.Sprintf(format, v...)+"\n"))
	}
}

// Warning logs warning message
func (logger *Logger) Warning(v ...interface{}) {
	if logger.warningLogger != nil {
		logger.warningLogger.Output(logger.depth, logger.wrap(fmt.Sprintln(v...)))
	}
}

// Warningf logs important message
func (logger *Logger) Warningf(format string, v ...interface{}) {
	if logger.warningLogger != nil {
		logger.warningLogger.Output(logger.depth, logger.wrap(fmt.Sprintf(format, v...)+"\n"))
	}
}

// Error logs error message
func (logger *Logger) Error(v ...interface{}) {
	if logger.errorLogger != nil {
		logger.errorLogger.Output(logger.depth, logger.wrap(fmt.Sprintln(v...)))
	}
}

// Errorf logs important message
func (logger *Logger) Errorf(format string, v ...interface{}) {
	if logger.errorLogger != nil {
		logger.errorLogger.Output(logger.depth, logger.wrap(fmt.Sprintf(format, v...)+"\n"))
	}
}
