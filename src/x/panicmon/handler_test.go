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

package panicmon

import (
	"sync"

	"github.com/stretchr/testify/mock"
)

type mockProcessHandler struct {
	mock.Mock
	sync.Mutex
}

func (m *mockProcessHandler) ProcessStarted(e ProcessStartEvent) {
	m.Lock()
	defer m.Unlock()
	m.Called(e)
}

func (m *mockProcessHandler) ProcessFailed(e ProcessFailedEvent) {
	m.Lock()
	defer m.Unlock()
	m.Called(e)
}

func (m *mockProcessHandler) ProcessExited(e ProcessExitedEvent) {
	m.Lock()
	defer m.Unlock()
	m.Called(e)
}

type mockSignalHandler struct {
	mock.Mock
	sync.Mutex
}

func (m *mockSignalHandler) SignalReceived(e SignalReceivedEvent) {
	m.Lock()
	defer m.Unlock()
	m.Called(e)
}

func (m *mockSignalHandler) SignalPassed(e SignalPassedEvent) {
	m.Lock()
	defer m.Unlock()
	m.Called(e)
}

func (m *mockSignalHandler) SignalFailed(e SignalFailedEvent) {
	m.Lock()
	defer m.Unlock()
	m.Called(e)
}
