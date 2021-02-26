// Copyright (c) 2021 Uber Technologies, Inc.
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

package clock

import (
	"time"
)

// OffsetClock represents offset clock which returns current time from the initial seed time value.
type OffsetClock struct {
	timeDelta time.Duration
	nowFn     NowFn
}

// NewOffsetClock returns new offset clock.
func NewOffsetClock(offsetTime time.Time, nowFn NowFn) OffsetClock {
	return OffsetClock{nowFn: nowFn, timeDelta: offsetTime.Sub(nowFn())}
}

// Now returns current time from the initial seed time value.
func (c OffsetClock) Now() time.Time {
	now := c.nowFn()
	return now.Add(c.timeDelta)
}
