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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimiterLimit(t *testing.T) {
	allowedPerSecond := int64(10)
	now := time.Now().Truncate(time.Second)
	nowFn := func() time.Time { return now }

	limiter := NewLimiter(allowedPerSecond, nowFn)
	require.Equal(t, allowedPerSecond, limiter.Limit())
}

func TestLimiterIsAllowed(t *testing.T) {
	allowedPerSecond := int64(10)
	now := time.Now().Truncate(time.Second)
	nowFn := func() time.Time { return now }

	limiter := NewLimiter(allowedPerSecond, nowFn)
	require.True(t, limiter.IsAllowed(5))
	for i := 0; i < 5; i++ {
		now = now.Add(100 * time.Millisecond)
		require.True(t, limiter.IsAllowed(1))
	}
	require.False(t, limiter.IsAllowed(1))

	// Advance time to the next second and confirm the quota is reset.
	now = now.Add(time.Second)
	require.True(t, limiter.IsAllowed(5))
}

func TestLimiterReset(t *testing.T) {
	allowedPerSecond := int64(10)
	now := time.Now().Truncate(time.Second)
	nowFn := func() time.Time { return now }

	limiter := NewLimiter(allowedPerSecond, nowFn)
	require.False(t, limiter.IsAllowed(20))

	// Resetting to a higher limit and confirm all requests are allowed.
	limiter.Reset(20)
	require.True(t, limiter.IsAllowed(20))
}
