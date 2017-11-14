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
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3x/clock"
)

var (
	zeroTime = time.Unix(0, 0)
)

// Limiter is a simple rate limiter to control how frequently events are allowed to happen.
type Limiter struct {
	sync.RWMutex

	limitPerSecond int64
	nowFn          clock.NowFn

	alignedLast time.Time
	allowed     int64
}

// NewLimiter creates a new rate limiter.
func NewLimiter(l int64, fn clock.NowFn) *Limiter {
	return &Limiter{
		limitPerSecond: l,
		nowFn:          fn,
	}
}

// Limit returns the current limit.
func (l *Limiter) Limit() int64 {
	l.RLock()
	limit := l.limitPerSecond
	l.RUnlock()
	return limit
}

// IsAllowed returns whether n events may happen now.
// NB(xichen): If a large request comes in, this could potentially block following
// requests in the same second from going through. This is a non-issue if the limit
// is much bigger than the typical batch size, which is indeed the case in the aggregation
// layer as each batch size is usually fairly small.
func (l *Limiter) IsAllowed(n int64) bool {
	alignedNow := l.nowFn().Truncate(time.Second)
	l.RLock()
	if !alignedNow.After(l.alignedLast) {
		isAllowed := atomic.AddInt64(&l.allowed, n) <= l.limitPerSecond
		l.RUnlock()
		return isAllowed
	}
	l.RUnlock()

	l.Lock()
	if !alignedNow.After(l.alignedLast) {
		isAllowed := atomic.AddInt64(&l.allowed, n) <= l.limitPerSecond
		l.Unlock()
		return isAllowed
	}
	l.alignedLast = alignedNow
	l.allowed = n
	isAllowed := l.allowed <= l.limitPerSecond
	l.Unlock()
	return isAllowed
}

// Reset resets the internal state.
func (l *Limiter) Reset(limit int64) {
	l.Lock()
	l.alignedLast = zeroTime
	l.allowed = 0
	l.limitPerSecond = limit
	l.Unlock()
}
