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

import "sync"

type memoryTracker struct {
	sync.Mutex

	opts MemoryTrackerOptions

	numLoadedBytes        int
	numPendingLoadedBytes int
}

func (m *memoryTracker) IncNumLoadedBytes(x int) (okToLoad bool) {
	m.Lock()
	defer m.Unlock()
	limit := m.opts.numLoadedBytesLimit
	if limit > 0 && m.numLoadedBytes+x > limit {
		return false
	}
	m.numLoadedBytes += x
	return true
}

func (m *memoryTracker) NumLoadedBytes() int {
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
	m.Unlock()
}

// MemoryTrackerOptions are the options for the MemoryTracker.
type MemoryTrackerOptions struct {
	numLoadedBytesLimit int
}

// NewMemoryTrackerOptions creates a new MemoryTrackerOptions.
func NewMemoryTrackerOptions(numLoadedBytesLimit int) MemoryTrackerOptions {
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
