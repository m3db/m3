// Code generated by MockGen. DO NOT EDIT.
// Source: ../../build/types.go

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

// Package build is a generated GoMock package.
package build

import (
	"reflect"

	"github.com/m3db/m3/src/m3em/os/fs"

	"github.com/golang/mock/gomock"
)

// MockIterableBytesWithID is a mock of IterableBytesWithID interface.
type MockIterableBytesWithID struct {
	ctrl     *gomock.Controller
	recorder *MockIterableBytesWithIDMockRecorder
}

// MockIterableBytesWithIDMockRecorder is the mock recorder for MockIterableBytesWithID.
type MockIterableBytesWithIDMockRecorder struct {
	mock *MockIterableBytesWithID
}

// NewMockIterableBytesWithID creates a new mock instance.
func NewMockIterableBytesWithID(ctrl *gomock.Controller) *MockIterableBytesWithID {
	mock := &MockIterableBytesWithID{ctrl: ctrl}
	mock.recorder = &MockIterableBytesWithIDMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockIterableBytesWithID) EXPECT() *MockIterableBytesWithIDMockRecorder {
	return m.recorder
}

// ID mocks base method.
func (m *MockIterableBytesWithID) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockIterableBytesWithIDMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockIterableBytesWithID)(nil).ID))
}

// Iter mocks base method.
func (m *MockIterableBytesWithID) Iter(bufferSize int) (fs.FileReaderIter, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Iter", bufferSize)
	ret0, _ := ret[0].(fs.FileReaderIter)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Iter indicates an expected call of Iter.
func (mr *MockIterableBytesWithIDMockRecorder) Iter(bufferSize interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Iter", reflect.TypeOf((*MockIterableBytesWithID)(nil).Iter), bufferSize)
}

// MockServiceBuild is a mock of ServiceBuild interface.
type MockServiceBuild struct {
	ctrl     *gomock.Controller
	recorder *MockServiceBuildMockRecorder
}

// MockServiceBuildMockRecorder is the mock recorder for MockServiceBuild.
type MockServiceBuildMockRecorder struct {
	mock *MockServiceBuild
}

// NewMockServiceBuild creates a new mock instance.
func NewMockServiceBuild(ctrl *gomock.Controller) *MockServiceBuild {
	mock := &MockServiceBuild{ctrl: ctrl}
	mock.recorder = &MockServiceBuildMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockServiceBuild) EXPECT() *MockServiceBuildMockRecorder {
	return m.recorder
}

// ID mocks base method.
func (m *MockServiceBuild) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockServiceBuildMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockServiceBuild)(nil).ID))
}

// Iter mocks base method.
func (m *MockServiceBuild) Iter(bufferSize int) (fs.FileReaderIter, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Iter", bufferSize)
	ret0, _ := ret[0].(fs.FileReaderIter)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Iter indicates an expected call of Iter.
func (mr *MockServiceBuildMockRecorder) Iter(bufferSize interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Iter", reflect.TypeOf((*MockServiceBuild)(nil).Iter), bufferSize)
}

// SourcePath mocks base method.
func (m *MockServiceBuild) SourcePath() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SourcePath")
	ret0, _ := ret[0].(string)
	return ret0
}

// SourcePath indicates an expected call of SourcePath.
func (mr *MockServiceBuildMockRecorder) SourcePath() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SourcePath", reflect.TypeOf((*MockServiceBuild)(nil).SourcePath))
}

// MockServiceConfiguration is a mock of ServiceConfiguration interface.
type MockServiceConfiguration struct {
	ctrl     *gomock.Controller
	recorder *MockServiceConfigurationMockRecorder
}

// MockServiceConfigurationMockRecorder is the mock recorder for MockServiceConfiguration.
type MockServiceConfigurationMockRecorder struct {
	mock *MockServiceConfiguration
}

// NewMockServiceConfiguration creates a new mock instance.
func NewMockServiceConfiguration(ctrl *gomock.Controller) *MockServiceConfiguration {
	mock := &MockServiceConfiguration{ctrl: ctrl}
	mock.recorder = &MockServiceConfigurationMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockServiceConfiguration) EXPECT() *MockServiceConfigurationMockRecorder {
	return m.recorder
}

// Bytes mocks base method.
func (m *MockServiceConfiguration) Bytes() ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Bytes")
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Bytes indicates an expected call of Bytes.
func (mr *MockServiceConfigurationMockRecorder) Bytes() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Bytes", reflect.TypeOf((*MockServiceConfiguration)(nil).Bytes))
}

// ID mocks base method.
func (m *MockServiceConfiguration) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockServiceConfigurationMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockServiceConfiguration)(nil).ID))
}

// Iter mocks base method.
func (m *MockServiceConfiguration) Iter(bufferSize int) (fs.FileReaderIter, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Iter", bufferSize)
	ret0, _ := ret[0].(fs.FileReaderIter)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Iter indicates an expected call of Iter.
func (mr *MockServiceConfigurationMockRecorder) Iter(bufferSize interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Iter", reflect.TypeOf((*MockServiceConfiguration)(nil).Iter), bufferSize)
}
