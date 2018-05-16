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

package runtime

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockListener struct {
	sync.RWMutex
	value Options
}

func (l *mockListener) SetRuntimeOptions(value Options) {
	l.Lock()
	defer l.Unlock()
	l.value = value
}

func (l *mockListener) runtimeOptions() Options {
	l.RLock()
	defer l.RUnlock()
	return l.value
}

func TestRuntimeOptionsManagerUpdate(t *testing.T) {
	mgr := NewOptionsManager()
	opts := NewOptions().SetWriteNewSeriesAsync(false)
	require.NoError(t, mgr.Update(opts))

	assert.Equal(t, false, mgr.Get().WriteNewSeriesAsync())

	l := &mockListener{}
	assert.Nil(t, l.value)

	// Ensure immediately sets the value
	mgr.RegisterListener(l)
	assert.Equal(t, opts, l.runtimeOptions())

	// Update and verify
	mgr.Update(opts.SetWriteNewSeriesAsync(true))

	// Verify listener receives update
	for func() bool {
		return !l.runtimeOptions().WriteNewSeriesAsync()
	}() {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, true, l.runtimeOptions().WriteNewSeriesAsync())
}
