// Copyright (c) 2016 Uber Technologies, Inc.
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

package xdebug

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type captureIOWriter struct {
	callback func(p []byte)
}

func (b *captureIOWriter) Write(p []byte) (int, error) {
	b.callback(p)
	return len(p), nil
}

func TestRWMutexReportEvery(t *testing.T) {
	var (
		captured string
		wg       sync.WaitGroup
		doneWg   sync.WaitGroup
	)

	doneWg.Add(1)
	wg.Add(1)
	hasCaptured := false
	mutex := &RWMutex{Name: "lock", Writer: &captureIOWriter{callback: func(p []byte) {
		if !hasCaptured {
			hasCaptured = true
			captured = string(p)
			wg.Done()
		}
	}}}

	// Create a writer that holds onto the lock
	go func() {
		mutex.Lock()
		doneWg.Wait()
		mutex.Unlock()
	}()

	// Wait for writer to appear
	for {
		if atomic.LoadInt64(&mutex.writers) != 1 {
			time.Sleep(time.Millisecond)
		}
		break
	}

	// Create a few pending writers and readers
	for i := 0; i < 3; i++ {
		go func() {
			mutex.Lock()
			mutex.Unlock()
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			mutex.RLock()
			mutex.RUnlock()
		}()
	}

	// Start reporter
	reporter := mutex.ReportEvery(time.Millisecond)

	// Wait for report
	wg.Wait()

	// Close the reporter
	reporter.Close()

	// Unlock the active writer
	doneWg.Done()

	// Match that the data is as expected
	firstExpect := "debug.RWMutex[lock]: writers 1 (pending 3) readers 10, stack: goroutine"
	require.True(t, len(captured) > len(firstExpect))
	require.Equal(t, firstExpect, captured[:len(firstExpect)])

	// Fuzzy match that the stack looks correct
	require.True(t, strings.Contains(captured, "TestRWMutexReportEvery.func"))
}
