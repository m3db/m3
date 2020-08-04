// Copyright (c) 2019 Uber Technologies, Inc.
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

package storage

import (
	"sync"
)

type memoryTracker struct {
	sync.Mutex

	opts MemoryTrackerOptions

	numLoadedBytes        int64
	numPendingLoadedBytes int64

	waitForDecWg *sync.WaitGroup
}

func (m *memoryTracker) IncNumLoadedBytes(x int64) (okToLoad bool) {
	m.Lock()
	defer m.Unlock()
	limit := m.opts.numLoadedBytesLimit
	if limit == 0 {
		// Limit of 0 means no limit.
		return true
	}
	// This check is optimistic in the sense that as long as the number of loaded bytes
	// is currently under the limit then x is accepted, regardless of how far over the
	// limit it brings the value of numLoadedBytes.
	//
	// The reason for this is to avoid scenarios where some process gets permanently
	// stuck because the amount of data it needs to load at once is larger than the limit
	// and as a result it's never able to make any progress.
	//
	// In practice this implementation should be fine for the vast majority of configurations
	// and workloads.
	if m.numLoadedBytes < limit {
		m.numLoadedBytes += x
		return true
	}

	return false
}

func (m *memoryTracker) NumLoadedBytes() int64 {
	m.Lock()
	defer m.Unlock()
	return m.numLoadedBytes
}

func (m *memoryTracker) MarkLoadedAsPending() {
	m.Lock()
	m.numPendingLoadedBytes = m.numLoadedBytes
	m.Unlock()
}

func (m *memoryTracker) DecPendingLoadedBytes() {
	m.Lock()
	m.numLoadedBytes -= m.numPendingLoadedBytes
	m.numPendingLoadedBytes = 0
	if m.waitForDecWg != nil {
		m.waitForDecWg.Done()
		m.waitForDecWg = nil
	}
	m.Unlock()
}

func (m *memoryTracker) WaitForDec() {
	m.Lock()
	if m.waitForDecWg == nil {
		m.waitForDecWg = &sync.WaitGroup{}
		m.waitForDecWg.Add(1)
	}
	wg := m.waitForDecWg
	m.Unlock()
	wg.Wait()
}

// MemoryTrackerOptions are the options for the MemoryTracker.
type MemoryTrackerOptions struct {
	numLoadedBytesLimit int64
}

// NewMemoryTrackerOptions creates a new MemoryTrackerOptions.
func NewMemoryTrackerOptions(numLoadedBytesLimit int64) MemoryTrackerOptions {
	return MemoryTrackerOptions{
		numLoadedBytesLimit: numLoadedBytesLimit,
	}
}

// NewMemoryTracker creates a new MemoryTracker.
func NewMemoryTracker(opts MemoryTrackerOptions) MemoryTracker {
	return &memoryTracker{
		opts: opts,
	}
}
