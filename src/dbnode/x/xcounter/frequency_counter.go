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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3x/clock"
)

type frequencyBucket struct {
	sync.RWMutex

	timestamp time.Time
	count     int64
}

// record records the frequency value n for a given time.
// If the frequency value is too old, it's discarded.
// Returns true if the value is processed, false if it
// requires further processing.
func (b *frequencyBucket) record(t time.Time, n int64) {
	b.RLock()
	if processed := b.recordWithLock(t, n); processed {
		b.RUnlock()
		return
	}
	b.RUnlock()

	b.Lock()
	if processed := b.recordWithLock(t, n); processed {
		b.Unlock()
		return
	}
	b.timestamp = t
	b.count = n
	b.Unlock()
}

// countFor returns the frequency value if the bucket
// timestamp matches the given time, 0 otherwise
func (b *frequencyBucket) countFor(t time.Time) int64 {
	b.RLock()
	if b.timestamp != t {
		b.RUnlock()
		return 0
	}
	count := b.count
	b.RUnlock()

	return count
}

func (b *frequencyBucket) recordWithLock(t time.Time, n int64) bool {
	// The information is too old to keep
	if t.Before(b.timestamp) {
		return true
	}
	// The timestamp matches so it's safe to record
	if t.Equal(b.timestamp) {
		// NB(xichen): use atomics here so multiple goroutines
		// can record at the same time
		atomic.AddInt64(&b.count, n)
		return true
	}
	// Otherwise this bucket is stale, don't record
	return false
}

// FrequencyCounter keeps track of the frequency counts for the
// previous interval and the current interval
type FrequencyCounter struct {
	nowFn    clock.NowFn
	interval time.Duration
	buckets  []frequencyBucket
}

// NewFrequencyCounter creates a new frequency counter
func NewFrequencyCounter(opts Options) FrequencyCounter {
	return FrequencyCounter{
		nowFn:    time.Now,
		interval: opts.Interval(),
		buckets:  make([]frequencyBucket, opts.NumBuckets()),
	}
}

// Record records a frequency value in the corresponding bucket
func (c *FrequencyCounter) Record(n int64) {
	now := c.nowFn().Truncate(c.interval)
	bucketIdx := c.bucketIdx(now)
	c.buckets[bucketIdx].record(now, n)
}

// Count returns the frequency count between now - dur and now
func (c *FrequencyCounter) Count(dur time.Duration) int64 {
	if dur <= 0 {
		return 0
	}
	var (
		n        = int(math.Min(float64(dur/c.interval), float64(len(c.buckets))))
		now      = c.nowFn().Truncate(c.interval)
		currIdx  = c.bucketIdx(now)
		currTime = now
		total    int64
	)
	// NB(xichen): we discount the current bucket because it may be incomplete
	for i := 0; i < n; i++ {
		currTime = currTime.Add(-c.interval)
		currIdx--
		if currIdx < 0 {
			currIdx += len(c.buckets)
		}
		total += c.buckets[currIdx].countFor(currTime)
	}
	return total
}

func (c *FrequencyCounter) bucketIdx(now time.Time) int {
	return int((now.UnixNano() / int64(c.interval)) % int64(len(c.buckets)))
}
