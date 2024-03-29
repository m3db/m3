// Code generated by MockGen. DO NOT EDIT.
// Source: ../../executor/transform/exec.go

// Copyright (c) 2022 Uber Technologies, Inc.
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

// Package transform is a generated GoMock package.
package transform

import (
	"reflect"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"

	"github.com/golang/mock/gomock"
)

// MocksimpleOpNode is a mock of simpleOpNode interface.
type MocksimpleOpNode struct {
	ctrl     *gomock.Controller
	recorder *MocksimpleOpNodeMockRecorder
}

// MocksimpleOpNodeMockRecorder is the mock recorder for MocksimpleOpNode.
type MocksimpleOpNodeMockRecorder struct {
	mock *MocksimpleOpNode
}

// NewMocksimpleOpNode creates a new mock instance.
func NewMocksimpleOpNode(ctrl *gomock.Controller) *MocksimpleOpNode {
	mock := &MocksimpleOpNode{ctrl: ctrl}
	mock.recorder = &MocksimpleOpNodeMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MocksimpleOpNode) EXPECT() *MocksimpleOpNodeMockRecorder {
	return m.recorder
}

// Params mocks base method.
func (m *MocksimpleOpNode) Params() parser.Params {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Params")
	ret0, _ := ret[0].(parser.Params)
	return ret0
}

// Params indicates an expected call of Params.
func (mr *MocksimpleOpNodeMockRecorder) Params() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Params", reflect.TypeOf((*MocksimpleOpNode)(nil).Params))
}

// ProcessBlock mocks base method.
func (m *MocksimpleOpNode) ProcessBlock(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) (block.Block, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProcessBlock", queryCtx, ID, b)
	ret0, _ := ret[0].(block.Block)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ProcessBlock indicates an expected call of ProcessBlock.
func (mr *MocksimpleOpNodeMockRecorder) ProcessBlock(queryCtx, ID, b interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProcessBlock", reflect.TypeOf((*MocksimpleOpNode)(nil).ProcessBlock), queryCtx, ID, b)
}
