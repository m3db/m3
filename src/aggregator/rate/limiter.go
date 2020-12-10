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

package rate

import (
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sys/cpu"

	xtime "github.com/m3db/m3/src/x/time"
)

var (
	zeroTime = xtime.UnixNano(0)
)

// Limiter is a simple rate limiter to control how frequently events are allowed to happen.
// It trades some accuracy on resets for speed.
type Limiter struct {
	limitPerSecond atomic.Int64
	alignedLast    atomic.Int64
	_              cpu.CacheLinePad // prevent false sharing
	allowed        atomic.Int64
}

// NewLimiter creates a new rate limiter.
func NewLimiter(l int64) *Limiter {
	limiter := &Limiter{}
	limiter.limitPerSecond.Store(l)
	return limiter
}

// Limit returns the current limit. A zero limit means no limit is set.
func (l *Limiter) Limit() int64 {
	return l.limitPerSecond.Load()
}

// IsAllowed returns whether n events may happen now.
// NB(xichen): If a large request comes in, this could potentially block following
// requests in the same second from going through. This is a non-issue if the limit
// is much bigger than the typical batch size, which is indeed the case in the aggregation
// layer as each batch size is usually fairly small.
// The limiter is racy on window changes, and can be overly aggressive rejecting requests.
// As the limits are usually at least 10k+, the error is worth the speedup.
func (l *Limiter) IsAllowed(n int64, now xtime.UnixNano) bool {
	limit := l.Limit()
	if limit <= 0 {
		return true
	}

	var (
		allowed     = l.allowed.Add(n)
		alignedNow  = now - now%xtime.UnixNano(time.Second) // truncate to second boundary
		alignedLast = xtime.UnixNano(l.alignedLast.Load())
	)

	if alignedNow > alignedLast && l.alignedLast.CAS(int64(alignedLast), int64(alignedNow)) {
		l.allowed.Store(n)
		allowed = n
	}

	return allowed <= limit
}

// Reset resets the internal state. It does not reset all the values atomically,
// but it should not be a problem, as dynamic rate limits in aggregator
// are usually reset under a lock.
func (l *Limiter) Reset(limit int64) {
	l.allowed.Store(0)
	l.alignedLast.Store(int64(zeroTime))
	l.limitPerSecond.Store(limit)
}
