/*
Copyright (c) 2020, pigeonligh.
*/

package log

import (
	"fmt"
	"log"
)

// Logger is used to log
type Logger struct {
	debugLogger   *log.Logger
	infoLogger    *log.Logger
	warningLogger *log.Logger
	errorLogger   *log.Logger

	depth int
}

// Debug logs important message
func (logger *Logger) Debug(v ...interface{}) {
	if logger.debugLogger != nil && debugMode {
		logger.debugLogger.Output(logger.depth, fmt.Sprintln(v...))
	}
}

// Debugf logs important message
func (logger *Logger) Debugf(format string, v ...interface{}) {
	if logger.debugLogger != nil && debugMode {
		logger.debugLogger.Output(logger.depth, fmt.Sprintf(format, v...))
	}
}

// Info logs important message
func (logger *Logger) Info(v ...interface{}) {
	if logger.infoLogger != nil {
		logger.infoLogger.Output(logger.depth, fmt.Sprintln(v...))
	}
}

// Infof logs important message
func (logger *Logger) Infof(format string, v ...interface{}) {
	if logger.infoLogger != nil {
		logger.infoLogger.Output(logger.depth, fmt.Sprintf(format, v...))
	}
}

// Warning logs warning message
func (logger *Logger) Warning(v ...interface{}) {
	if logger.warningLogger != nil {
		logger.warningLogger.Output(logger.depth, fmt.Sprintln(v...))
	}
}

// Warningf logs important message
func (logger *Logger) Warningf(format string, v ...interface{}) {
	if logger.warningLogger != nil {
		logger.warningLogger.Output(logger.depth, fmt.Sprintf(format, v...))
	}
}

// Error logs error message
func (logger *Logger) Error(v ...interface{}) {
	if logger.errorLogger != nil {
		logger.errorLogger.Output(logger.depth, fmt.Sprintln(v...))
	}
}

// Errorf logs important message
func (logger *Logger) Errorf(format string, v ...interface{}) {
	if logger.errorLogger != nil {
		logger.errorLogger.Output(logger.depth, fmt.Sprintf(format, v...))
	}
}
