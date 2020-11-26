/*
Copyright (c) 2020, pigeonligh.
*/

package log

import (
	"io"
	"log"
	"os"
)

// Level is log level type
type Level = uint

var (
	loggers *Logger

	logLevel Level = 0 // block the logs

	debugMode bool = false
)

func init() {
	loggers = &Logger{
		debugLogger:   log.New(os.Stderr, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile),
		infoLogger:    log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile),
		warningLogger: log.New(os.Stdout, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile),
		errorLogger:   log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

// SetDebugMode sets debug mode
func SetDebugMode(debug bool) {
	debugMode = debug
}

// SetOutput sets log's output
func SetOutput(debugOutput, infoOutput, warningOutput, errorOutput io.Writer) {
	loggers.debugLogger.SetOutput(debugOutput)
	loggers.infoLogger.SetOutput(infoOutput)
	loggers.warningLogger.SetOutput(warningOutput)
	loggers.errorLogger.SetOutput(errorOutput)
}

// SetLevel sets log's level
func SetLevel(level Level) {
	logLevel = level
}

// V gets logger by level
func V(level Level) *Logger {
	return get(level, 2)
}

func get(level Level, depth int) *Logger {
	if level&logLevel == 0 {
		return &Logger{}
	}
	return &Logger{
		debugLogger:   loggers.debugLogger,
		infoLogger:    loggers.infoLogger,
		warningLogger: loggers.warningLogger,
		errorLogger:   loggers.errorLogger,

		depth: depth,
	}
}

// Debug logs important message
func Debug(v ...interface{}) {
	get(logLevel, 3).Debug(v...)
}

// Debugf logs important message
func Debugf(format string, v ...interface{}) {
	get(logLevel, 3).Debugf(format, v...)
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
