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

package xcounter

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFrequencyCounterSequential(t *testing.T) {
	numBuckets := 10
	interval := time.Second
	opts := NewOptions().SetNumBuckets(numBuckets).SetInterval(interval)
	c := NewFrequencyCounter(opts)
	var now time.Time
	c.nowFn = func() time.Time { return now }

	// Record a value in each bucket
	now = time.Now().Truncate(interval)
	iter := 10
	for i := 0; i < iter; i++ {
		now = now.Add(interval)
		c.Record(1)
	}
	require.Equal(t, int64(iter-1), c.Count(time.Duration(iter)*interval))

	// Move time forward
	now = now.Add(interval)
	c.Record(2)
	require.Equal(t, int64(iter-1), c.Count(time.Duration(iter)*interval))

	// Move time forward again
	now = now.Add(interval)
	c.Record(2)
	require.Equal(t, int64(iter), c.Count(time.Duration(iter)*interval))

	// Move time forward far into future
	now = now.Add(20 * interval)
	require.Equal(t, int64(0), c.Count(time.Duration(iter)*interval))
}

func TestFrequencyCounterParallel(t *testing.T) {
	numBuckets := 10
	interval := time.Second
	opts := NewOptions().SetNumBuckets(numBuckets).SetInterval(interval)
	c := NewFrequencyCounter(opts)
	var now time.Time
	c.nowFn = func() time.Time { return now }

	var (
		wg         sync.WaitGroup
		numWorkers = 10
		iter       = 10
	)
	now = time.Now().Truncate(interval)
	for i := 0; i < iter; i++ {
		now = now.Add(interval)
		wg.Add(numWorkers)
		for j := 0; j < numWorkers; j++ {
			go func() {
				defer wg.Done()

				c.Record(1)
			}()
		}
		wg.Wait()
	}
	require.Equal(t, int64(iter-1)*int64(numWorkers), c.Count(time.Duration(iter)*interval))
}
