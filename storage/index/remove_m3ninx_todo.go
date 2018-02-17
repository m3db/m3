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

package index

// TODO(prateek): move this to m3ninx

import (
	"github.com/golang/mock/gomock"
	"github.com/m3db/m3ninx/doc"
)

// Mock of ResultsIter interface
type MockResultsIter struct {
	ctrl     *gomock.Controller
	recorder *_MockResultsIterRecorder
}

// Recorder for MockResultsIter (not exported)
type _MockResultsIterRecorder struct {
	mock *MockResultsIter
}

func NewMockResultsIter(ctrl *gomock.Controller) *MockResultsIter {
	mock := &MockResultsIter{ctrl: ctrl}
	mock.recorder = &_MockResultsIterRecorder{mock}
	return mock
}

func (_m *MockResultsIter) EXPECT() *_MockResultsIterRecorder {
	return _m.recorder
}

func (_m *MockResultsIter) Next() bool {
	ret := _m.ctrl.Call(_m, "Next")
	ret0, _ := ret[0].(bool)
	return ret0
}

func (_mr *_MockResultsIterRecorder) Next() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Next")
}

func (_m *MockResultsIter) Current() (doc.Document, bool) {
	ret := _m.ctrl.Call(_m, "Current")
	ret0, _ := ret[0].(doc.Document)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

func (_mr *_MockResultsIterRecorder) Current() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Current")
}

func (_m *MockResultsIter) Err() error {
	ret := _m.ctrl.Call(_m, "Err")
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockResultsIterRecorder) Err() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Err")
}
