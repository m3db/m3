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

// Package test contains utility methods for testing.
package test

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
)

// Reporter wraps a *testing.T, and provides a more useful failure mode
// when interacting with gomock.Controller.
//
// For example, consider:
//   func TestMyThing(t *testing.T) {
//     mockCtrl := gomock.NewController(t)
//     defer mockCtrl.Finish()
//     mockObj := something.NewMockMyInterface(mockCtrl)
//     go func() {
//       mockObj.SomeMethod(4, "blah")
//     }
//   }
//
// It hangs without any indication that it's missing an EXPECT() on `mockObj`.
// Providing the Reporter to the gomock.Controller ctor avoids this, and terminates
// with useful feedback. i.e.
//   func TestMyThing(t *testing.T) {
//     mockCtrl := gomock.NewController(test.Reporter{t})
//     defer mockCtrl.Finish()
//     mockObj := something.NewMockMyInterface(mockCtrl)
//     go func() {
//       mockObj.SomeMethod(4, "blah") // crashes the test now
//     }
//   }
type Reporter struct {
	T *testing.T
}

// ensure Reporter implements gomock.TestReporter.
var _ gomock.TestReporter = Reporter{}

// Errorf is equivalent testing.T.Errorf.
func (r Reporter) Errorf(format string, args ...interface{}) {
	r.T.Errorf(format, args...)
}

// Fatalf crashes the program with a panic to allow users to diagnose
// missing expects.
func (r Reporter) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}
