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

package test

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// FailNowPanicsTestingT returns a TestingT that panics on a failed assertion.
// This is useful for aborting a test on failed assertions from an asynchronous
// goroutine (since stretchr calls FailNow on testing.T but it will not abort
// the test unless the goroutine is the one running the benchmark or test).
// For more info see: https://github.com/stretchr/testify/issues/652.
func FailNowPanicsTestingT(t *testing.T) require.TestingT {
	return failNowPanicsTestingT{TestingT: t}
}

var _ require.TestingT = failNowPanicsTestingT{}

type failNowPanicsTestingT struct {
	require.TestingT
}

func (t failNowPanicsTestingT) FailNow() {
	panic("failed assertion")
}

// ByteSlicesBackedBySameData returns a bool indicating if the raw backing bytes
// under the []byte slice point to the same memory.
func ByteSlicesBackedBySameData(a, b []byte) bool {
	aHeader := (*reflect.SliceHeader)(unsafe.Pointer(&a))
	bHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return aHeader.Data == bHeader.Data
}
