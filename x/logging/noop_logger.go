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

package logging

import (
	"os"

	"github.com/uber-common/bark"
)

// NoopLogger is a bark.Logger which does nothing.
var NoopLogger bark.Logger = noopLogger{}

type noopLogger struct{}

func (l noopLogger) Debug(args ...interface{})                           {}
func (l noopLogger) Debugf(format string, args ...interface{})           {}
func (l noopLogger) Info(args ...interface{})                            {}
func (l noopLogger) Infof(format string, args ...interface{})            {}
func (l noopLogger) Warn(args ...interface{})                            {}
func (l noopLogger) Warnf(format string, args ...interface{})            {}
func (l noopLogger) Error(args ...interface{})                           {}
func (l noopLogger) Errorf(format string, args ...interface{})           {}
func (l noopLogger) Fatal(args ...interface{})                           { os.Exit(1) }
func (l noopLogger) Fatalf(format string, args ...interface{})           { os.Exit(1) }
func (l noopLogger) Panic(args ...interface{})                           { panic("logger panic") }
func (l noopLogger) Panicf(format string, args ...interface{})           { panic("logger panic") }
func (l noopLogger) WithField(key string, value interface{}) bark.Logger { return l }
func (l noopLogger) WithFields(keyValues bark.LogFields) bark.Logger     { return l }
func (l noopLogger) Fields() bark.Fields                                 { return bark.Fields{} }
