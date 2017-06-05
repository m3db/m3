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

package aggregation

import "sync"

var (
	emptyCounter Counter
)

// Counter aggregates counter values.
type Counter struct {
	sum int64 // Sum of the values received
}

// NewCounter creates a new counter.
func NewCounter() Counter { return emptyCounter }

// Add adds a counter value.
func (c *Counter) Add(value int64) { c.sum += value }

// Sum returns the sum of counter values.
func (c *Counter) Sum() int64 { return c.sum }

// LockedCounter is a locked counter.
type LockedCounter struct {
	sync.Mutex
	Counter
}

// NewLockedCounter creates a new locked counter.
func NewLockedCounter() *LockedCounter { return &LockedCounter{} }

// Reset resets the locked counter.
func (lc *LockedCounter) Reset() { lc.Counter = emptyCounter }
