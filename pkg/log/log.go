/*
Copyright (c) 2020, pigeonligh.
*/

package log

import (
	"io"
	"log"
	"os"
)

var (
	infoLogger    *log.Logger
	warningLogger *log.Logger
	errorLogger   *log.Logger

	logLevel uint = ^uint(0) // block the logs
)

func init() {
	infoLogger = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	warningLogger = log.New(os.Stdout, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLogger = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}

// SetOutput sets log's output
func SetOutput(infoOutput, warningOutput, errorOutput io.Writer) {
	infoLogger.SetOutput(infoOutput)
	warningLogger.SetOutput(warningOutput)
	errorLogger.SetOutput(errorOutput)
}

// SetLevel sets log's level
func SetLevel(level uint) {
	logLevel = level
}

// V gets logger by level
func V(level uint) *Logger {
	return get(level, 2)
}

func get(level uint, depth int) *Logger {
	if level < logLevel {
		return &Logger{}
	}
	return &Logger{
		infoLogger:    infoLogger,
		warningLogger: warningLogger,
		errorLogger:   errorLogger,

		depth: depth,
	}
}

// Info logs important message
func Info(v ...interface{}) {
	get(logLevel, 3).Info(v...)
}

// Infof logs important message
func Infof(format string, v ...interface{}) {
	get(logLevel, 3).Infof(format, v...)
}

// Warning logs warning message
func Warning(v ...interface{}) {
	get(logLevel, 3).Warning(v...)
}

// Warningf logs important message
func Warningf(format string, v ...interface{}) {
	get(logLevel, 3).Warningf(format, v...)
}

// Error logs error message
func Error(v ...interface{}) {
	get(logLevel, 3).Error(v...)
}

// Errorf logs important message
func Errorf(format string, v ...interface{}) {
	get(logLevel, 3).Errorf(format, v...)
}
