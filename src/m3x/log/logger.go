// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package xlog

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// Logger provides an abstract interface for logging
type Logger interface {
	// Enabled returns whether the given level is enabled.
	Enabled(level LogLevel) bool

	// Fatalf logs a message, then exits with os.Exit(1).
	Fatalf(msg string, args ...interface{})

	// Fatal logs a message, then exits with os.Exit(1).
	Fatal(msg string)

	// Errorf logs a message at error priority.
	Errorf(msg string, args ...interface{})

	// Error logs a message at error priority.
	Error(msg string)

	// Warnf logs a message at warning priority.
	Warnf(msg string, args ...interface{})

	// Warn logs a message at warning priority.
	Warn(msg string)

	// Infof logs a message at info priority.
	Infof(msg string, args ...interface{})

	// Info logs a message at info priority.
	Info(msg string)

	// Debugf logs a message at debug priority.
	Debugf(msg string, args ...interface{})

	// Debug logs a message at debug priority.
	Debug(msg string)

	// Fields returns the fields that this logger contains.
	Fields() LogFields

	// WithFields returns a logger with the current logger's fields and fields.
	WithFields(fields ...LogField) Logger
}

// LogField is a single field of additional information passed to the logger.
type LogField interface {
	Key() string
	Value() interface{}
}

// NewLogField creates a new log field
func NewLogField(key string, value interface{}) LogField {
	return &logField{key, value}
}

// NewLogErrField wraps an error string as a LogField named "error"
func NewLogErrField(err error) LogField {
	return NewLogField("error", err.Error())
}

type logField struct {
	key   string
	value interface{}
}

func (f *logField) Key() string {
	return f.key
}

func (f *logField) Value() interface{} {
	return f.value
}

func (f *logField) String() string {
	return fmt.Sprintf("%v", *f)
}

// LogFields is a list of LogFields used to pass additional information to the logger.
type LogFields interface {
	// Len returns the length
	Len() int

	// ValueAt returns a value for an index
	ValueAt(i int) LogField
}

// Fields is a list of log fields that implements the LogFields interface
type Fields []LogField

// Len returns the length
func (f Fields) Len() int { return len(f) }

// ValueAt returns a value for an index
func (f Fields) ValueAt(i int) LogField { return f[i] }

// NullLogger is a logger that emits nowhere
var NullLogger Logger = nullLogger{}

type nullLogger struct {
	fields LogFields
}

func (nullLogger) Enabled(_ LogLevel) bool                { return false }
func (nullLogger) Fatalf(msg string, args ...interface{}) {}
func (nullLogger) Fatal(msg string)                       { os.Exit(1) }
func (nullLogger) Errorf(msg string, args ...interface{}) {}
func (nullLogger) Error(msg string)                       {}
func (nullLogger) Warnf(msg string, args ...interface{})  {}
func (nullLogger) Warn(msg string)                        {}
func (nullLogger) Infof(msg string, args ...interface{})  {}
func (nullLogger) Info(msg string)                        {}
func (nullLogger) Debugf(msg string, args ...interface{}) {}
func (nullLogger) Debug(msg string)                       {}
func (l nullLogger) Fields() LogFields                    { return l.fields }

func (l nullLogger) WithFields(newFields ...LogField) Logger {
	existingFields := l.Fields()
	fields := make([]LogField, 0, existingFields.Len()+len(newFields))
	for i := 0; i < existingFields.Len(); i++ {
		fields = append(fields, existingFields.ValueAt(i))
	}
	fields = append(fields, newFields...)
	return nullLogger{Fields(fields)}
}

// SimpleLogger prints logging information to standard out.
var SimpleLogger = NewLogger(os.Stdout)

type writerLogger struct {
	writer io.Writer
	fields LogFields
}

const writerLoggerStamp = "15:04:05.000000"

// NewLogger returns a Logger that writes to the given writer.
func NewLogger(writer io.Writer, fields ...LogField) Logger {
	return &writerLogger{writer, Fields(fields)}
}

func (l writerLogger) Fatalf(msg string, args ...interface{}) {
	l.printfn("F", msg, args...)
	os.Exit(1)
}

func (l writerLogger) Fatal(msg string) {
	l.printfn("F", msg)
	os.Exit(1)
}

func (l writerLogger) Enabled(_ LogLevel) bool                { return true }
func (l writerLogger) Errorf(msg string, args ...interface{}) { l.printfn("E", msg, args...) }
func (l writerLogger) Error(msg string)                       { l.printfn("E", msg) }
func (l writerLogger) Warnf(msg string, args ...interface{})  { l.printfn("W", msg, args...) }
func (l writerLogger) Warn(msg string)                        { l.printfn("W", msg) }
func (l writerLogger) Infof(msg string, args ...interface{})  { l.printfn("I", msg, args...) }
func (l writerLogger) Info(msg string)                        { l.printfn("I", msg) }
func (l writerLogger) Debugf(msg string, args ...interface{}) { l.printfn("D", msg, args...) }
func (l writerLogger) Debug(msg string)                       { l.printfn("D", msg) }
func (l writerLogger) printfn(prefix, msg string, args ...interface{}) {
	ft := time.Now().Format(writerLoggerStamp)
	fma := fmt.Sprintf(msg, args...)
	if l.fields.Len() == 0 {
		fmt.Fprintf(l.writer, "%s[%s] %s\n", ft, prefix, fma)
		return
	}
	fmt.Fprintf(l.writer, "%s[%s] %s %v\n", ft, prefix, fma, l.fields)
}

func (l writerLogger) Fields() LogFields {
	return l.fields
}

func (l writerLogger) WithFields(newFields ...LogField) Logger {
	existingFields := l.Fields()
	fields := make([]LogField, 0, existingFields.Len()+1)
	for i := 0; i < existingFields.Len(); i++ {
		fields = append(fields, existingFields.ValueAt(i))
	}
	fields = append(fields, newFields...)
	return &writerLogger{l.writer, Fields(fields)}
}

// LogLevel is the level of logging used by LevelLogger.
type LogLevel int

// The minimum level that will be logged. e.g. LogLevelError only logs errors and fatals.
const (
	LogLevelAll LogLevel = iota
	LogLevelDebug
	LogLevelInfo
	LogLevelWarn
	LogLevelError
	LogLevelFatal
)

var logLevels = []LogLevel{
	LogLevelAll,
	LogLevelDebug,
	LogLevelInfo,
	LogLevelWarn,
	LogLevelError,
	LogLevelFatal,
}

func (l LogLevel) String() string {
	switch l {
	case LogLevelAll:
		return "all"
	case LogLevelDebug:
		return "debug"
	case LogLevelInfo:
		return "info"
	case LogLevelWarn:
		return "warn"
	case LogLevelError:
		return "error"
	case LogLevelFatal:
		return "fatal"
	}
	return ""
}

// ParseLogLevel parses a log level string to log level
func ParseLogLevel(level string) (LogLevel, error) {
	level = strings.ToLower(level)
	for _, l := range logLevels {
		if strings.ToLower(l.String()) == level {
			return l, nil
		}
	}
	return LogLevel(0), fmt.Errorf("unrecognized log level: %s", level)
}

type levelLogger struct {
	logger Logger
	level  LogLevel
}

// NewLevelLogger returns a logger that only logs messages with a minimum of level.
func NewLevelLogger(logger Logger, level LogLevel) Logger {
	return &levelLogger{logger, level}
}

func (l levelLogger) Enabled(level LogLevel) bool {
	return l.level <= level
}

func (l levelLogger) Fatalf(msg string, args ...interface{}) {
	if l.level <= LogLevelFatal {
		l.logger.Fatalf(msg, args...)
	}
}

func (l levelLogger) Fatal(msg string) {
	if l.level <= LogLevelFatal {
		l.logger.Fatal(msg)
	}
}

func (l levelLogger) Errorf(msg string, args ...interface{}) {
	if l.level <= LogLevelError {
		l.logger.Errorf(msg, args...)
	}
}

func (l levelLogger) Error(msg string) {
	if l.level <= LogLevelError {
		l.logger.Error(msg)
	}
}

func (l levelLogger) Warnf(msg string, args ...interface{}) {
	if l.level <= LogLevelWarn {
		l.logger.Warnf(msg, args...)
	}
}

func (l levelLogger) Warn(msg string) {
	if l.level <= LogLevelWarn {
		l.logger.Warn(msg)
	}
}

func (l levelLogger) Infof(msg string, args ...interface{}) {
	if l.level <= LogLevelInfo {
		l.logger.Infof(msg, args...)
	}
}

func (l levelLogger) Info(msg string) {
	if l.level <= LogLevelInfo {
		l.logger.Info(msg)
	}
}

func (l levelLogger) Debugf(msg string, args ...interface{}) {
	if l.level <= LogLevelDebug {
		l.logger.Debugf(msg, args...)
	}
}

func (l levelLogger) Debug(msg string) {
	if l.level <= LogLevelDebug {
		l.logger.Debug(msg)
	}
}

func (l levelLogger) Fields() LogFields {
	return l.logger.Fields()
}

func (l levelLogger) WithFields(fields ...LogField) Logger {
	return &levelLogger{
		logger: l.logger.WithFields(fields...),
		level:  l.level,
	}
}
