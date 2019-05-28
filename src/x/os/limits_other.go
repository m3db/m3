// +build !linux
//
// Copyright (c) 2018 Uber Technologies, Inc.
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

package xos

import (
	"errors"
)

const (
	nonLinuxWarning = "unable to determine process limits on non-linux os"
)

var (
	errUnableToDetermineProcessLimits     = errors.New(nonLinuxWarning)
	errUnableToRaiseProcessNoFileNonLinux = errors.New("unable to raise no file limits on non-linux os")
)

// CanGetProcessLimits returns a boolean to signify if it can return limits,
// and a warning message if it cannot.
func CanGetProcessLimits() (bool, string) {
	return false, nonLinuxWarning
}

// GetProcessLimits returns the known process limits.
func GetProcessLimits() (ProcessLimits, error) {
	return ProcessLimits{}, errUnableToDetermineProcessLimits
}

// RaiseProcessNoFileToNROpen attempts to raise the process num files
// open limit to the nr_open system value.
func RaiseProcessNoFileToNROpen() (RaiseProcessNoFileToNROpenResult, error) {
	return RaiseProcessNoFileToNROpenResult{}, errUnableToRaiseProcessNoFileNonLinux
}
