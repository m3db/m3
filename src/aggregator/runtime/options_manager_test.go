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
)

func TestRuntimeOptionsManagerUpdate(t *testing.T) {
	limit := int64(100)
	opts := NewOptions().SetWriteValuesPerMetricLimitPerSecond(limit)
	mgr := NewOptionsManager(opts)

	assert.Equal(t, limit, mgr.RuntimeOptions().WriteValuesPerMetricLimitPerSecond())

	w := &mockWatcher{}
	assert.Nil(t, w.value)

	// Ensure immediately sets the value.
	mgr.RegisterWatcher(w)
	assert.Equal(t, opts, w.runtimeOptions())

	// Update and verify.
	newLimit := int64(200)
	mgr.SetRuntimeOptions(opts.SetWriteValuesPerMetricLimitPerSecond(newLimit))

	// Verify watcher receives update.
	for {
		if w.runtimeOptions().WriteValuesPerMetricLimitPerSecond() == newLimit {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type mockWatcher struct {
	sync.RWMutex

	value Options
}

func (m *mockWatcher) SetRuntimeOptions(value Options) {
	m.Lock()
	m.value = value
	m.Unlock()
}

func (m *mockWatcher) runtimeOptions() Options {
	m.RLock()
	value := m.value
	m.RUnlock()
	return value
}
