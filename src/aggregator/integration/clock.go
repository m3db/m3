//go:build integration
// +build integration

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

package integration

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/x/clock"
)

type testClock struct {
	sync.RWMutex

	now time.Time
}

func newTestClock(now time.Time) *testClock {
	return &testClock{
		now: now,
	}
}

func (c *testClock) Now() time.Time {
	c.RLock()
	defer c.RUnlock()
	return c.now
}

func (c *testClock) SetNow(now time.Time) {
	c.Lock()
	defer c.Unlock()
	c.now = now
}

func (c *testClock) Options() clock.Options {
	return clock.NewOptions().SetNowFn(c.Now)
}
