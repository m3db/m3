// Copyright (c) 2017 Uber Technologies, Inc.
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

package xtchannel

import (
	"fmt"
	"os"

	tchannel "github.com/uber/tchannel-go"
)

var noLog noopLogger

type noopLogger struct{}

// NewNoopLogger returns a default tchannel no-op logger.
func NewNoopLogger() tchannel.Logger { return noLog }

func (noopLogger) Enabled(_ tchannel.LogLevel) bool {
	return false
}
func (noopLogger) Fatal(msg string) {
	fmt.Fprintf(os.Stderr, msg)
	os.Exit(1)
}
func (noopLogger) Error(msg string) {

}
func (noopLogger) Warn(msg string) {

}
func (noopLogger) Infof(msg string, args ...interface{}) {

}
func (noopLogger) Info(msg string) {

}
func (noopLogger) Debugf(msg string, args ...interface{}) {

}
func (noopLogger) Debug(msg string) {

}
func (noopLogger) Fields() tchannel.LogFields {
	return nil
}
func (l noopLogger) WithFields(fields ...tchannel.LogField) tchannel.Logger {
	return l
}
