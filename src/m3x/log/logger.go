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

// Package log implements a logging package.
package log

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// Logger provides an abstract interface for logging.
type Logger interface {
	// Enabled returns whether the given level is enabled.
	Enabled(level Level) bool

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
	Fields() LoggerFields

	// WithFields returns a logger with the current logger's fields and fields.
	WithFields(fields ...Field) Logger
}

// Field is a single field of additional information passed to the logger.
type Field interface {
	Key() string
	Value() interface{}
}

// NewField creates a new log field.
func NewField(key string, value interface{}) Field {
	return &field{key, value}
}

// NewErrField wraps an error string as a Field named "error".
func NewErrField(err error) Field {
	return NewField("error", err.Error())
}

type field struct {
	key   string
	value interface{}
}

func (f *field) Key() string {
	return f.key
}

func (f *field) Value() interface{} {
	return f.value
}

func (f *field) String() string {
	return fmt.Sprintf("%v", *f)
}

// LoggerFields is a list of Fields used to pass additional information to the logger.
type LoggerFields interface {
	// Len returns the length
	Len() int

	// ValueAt returns a value for an index.
	ValueAt(i int) Field
}

// Fields is a list of log fields that implements the LoggerFields interface.
type Fields []Field

// Len returns the length.
func (f Fields) Len() int { return len(f) }

// ValueAt returns a value for an index.
func (f Fields) ValueAt(i int) Field { return f[i] }

// NullLogger is a logger that emits nowhere.
var NullLogger Logger = nullLogger{}

type nullLogger struct {
	fields Fields
}

func (nullLogger) Enabled(_ Level) bool                   { return false }
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
func (l nullLogger) Fields() LoggerFields                 { return l.fields }

func (l nullLogger) WithFields(newFields ...Field) Logger {
	existingLen := 0

	existingFields := l.Fields()
	if existingFields != nil {
		existingLen = existingFields.Len()
	}

	fields := make([]Field, 0, existingLen+len(newFields))
	for i := 0; i < existingLen; i++ {
		fields = append(fields, existingFields.ValueAt(i))
	}
	fields = append(fields, newFields...)
	return nullLogger{Fields(fields)}
}

// SimpleLogger prints logging information to standard out.
var SimpleLogger = NewLogger(os.Stdout)

type writerLogger struct {
	writer io.Writer
	fields LoggerFields
}

const writerLoggerStamp = "15:04:05.000000"

// NewLogger returns a Logger that writes to the given writer.
func NewLogger(writer io.Writer, fields ...Field) Logger {
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

func (l writerLogger) Enabled(_ Level) bool                   { return true }
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

func (l writerLogger) Fields() LoggerFields {
	return l.fields
}

func (l writerLogger) WithFields(newFields ...Field) Logger {
	existingFields := l.Fields()
	fields := make([]Field, 0, existingFields.Len()+1)
	for i := 0; i < existingFields.Len(); i++ {
		fields = append(fields, existingFields.ValueAt(i))
	}
	fields = append(fields, newFields...)
	return &writerLogger{l.writer, Fields(fields)}
}

// Level is the level of logging used by LevelLogger.
type Level int

// The minimum level that will be logged. e.g. LevelError only logs errors and fatals.
const (
	LevelAll Level = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

var levels = []Level{
	LevelAll,
	LevelDebug,
	LevelInfo,
	LevelWarn,
	LevelError,
	LevelFatal,
}

func (l Level) String() string {
	switch l {
	case LevelAll:
		return "all"
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	case LevelFatal:
		return "fatal"
	}
	return ""
}

// ParseLevel parses a log level string to log level.
func ParseLevel(level string) (Level, error) {
	level = strings.ToLower(level)
	for _, l := range levels {
		if strings.ToLower(l.String()) == level {
			return l, nil
		}
	}
	return Level(0), fmt.Errorf("unrecognized log level: %s", level)
}

type levelLogger struct {
	logger Logger
	level  Level
}

// NewLevelLogger returns a logger that only logs messages with a minimum of level.
func NewLevelLogger(logger Logger, level Level) Logger {
	return &levelLogger{logger, level}
}

func (l levelLogger) Enabled(level Level) bool {
	return l.level <= level
}

func (l levelLogger) Fatalf(msg string, args ...interface{}) {
	if l.level <= LevelFatal {
		l.logger.Fatalf(msg, args...)
	}
}

func (l levelLogger) Fatal(msg string) {
	if l.level <= LevelFatal {
		l.logger.Fatal(msg)
	}
}

func (l levelLogger) Errorf(msg string, args ...interface{}) {
	if l.level <= LevelError {
		l.logger.Errorf(msg, args...)
	}
}

func (l levelLogger) Error(msg string) {
	if l.level <= LevelError {
		l.logger.Error(msg)
	}
}

func (l levelLogger) Warnf(msg string, args ...interface{}) {
	if l.level <= LevelWarn {
		l.logger.Warnf(msg, args...)
	}
}

func (l levelLogger) Warn(msg string) {
	if l.level <= LevelWarn {
		l.logger.Warn(msg)
	}
}

func (l levelLogger) Infof(msg string, args ...interface{}) {
	if l.level <= LevelInfo {
		l.logger.Infof(msg, args...)
	}
}

func (l levelLogger) Info(msg string) {
	if l.level <= LevelInfo {
		l.logger.Info(msg)
	}
}

func (l levelLogger) Debugf(msg string, args ...interface{}) {
	if l.level <= LevelDebug {
		l.logger.Debugf(msg, args...)
	}
}

func (l levelLogger) Debug(msg string) {
	if l.level <= LevelDebug {
		l.logger.Debug(msg)
	}
}

func (l levelLogger) Fields() LoggerFields {
	return l.logger.Fields()
}

func (l levelLogger) WithFields(fields ...Field) Logger {
	return &levelLogger{
		logger: l.logger.WithFields(fields...),
		level:  l.level,
	}
}
