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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xtime "github.com/m3db/m3/src/x/time"
)

func xnow() xtime.UnixNano {
	return xtime.ToUnixNano(time.Now().Truncate(time.Second))
}

func BenchmarkLimiter(b *testing.B) {
	var (
		allowedPerSecond int64 = 100
		now                    = xnow()
		limiter                = NewLimiter(allowedPerSecond)
	)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var allowed bool
			for i := int64(0); i <= allowedPerSecond+1; i++ {
				allowed = limiter.IsAllowed(1, now)
			}

			if allowed {
				b.Fatalf("expected limit to be hit")
			}
		}
	})
	require.Equal(b, allowedPerSecond, limiter.Limit())
}

func TestLimiterLimit(t *testing.T) {
	var allowedPerSecond int64 = 10

	limiter := NewLimiter(allowedPerSecond)
	require.Equal(t, allowedPerSecond, limiter.Limit())
}

func TestLimiterIsAllowed(t *testing.T) {
	var (
		allowedPerSecond int64 = 10
		now                    = xnow()
	)

	limiter := NewLimiter(allowedPerSecond)
	require.True(t, limiter.IsAllowed(5, now))
	for i := 0; i < 5; i++ {
		now += xtime.UnixNano(100 * time.Millisecond)
		require.True(t, limiter.IsAllowed(1, now))
	}
	require.False(t, limiter.IsAllowed(1, now))

	// Advance time to the next second and confirm the quota is reset.
	now += xtime.UnixNano(time.Second)
	require.True(t, limiter.IsAllowed(5, now))
}

func TestLimiterUnlimited(t *testing.T) {
	var (
		unlimitedLimit int64 = 0
		now                  = xnow()
	)

	limiter := NewLimiter(unlimitedLimit)
	require.True(t, limiter.IsAllowed(math.MaxInt64, now))

	limiter.Reset(1)
	require.False(t, limiter.IsAllowed(2, now))
}

func TestLimiterReset(t *testing.T) {
	var (
		allowedPerSecond int64 = 10
		now                    = xnow()
	)

	limiter := NewLimiter(allowedPerSecond)
	require.False(t, limiter.IsAllowed(20, now))

	// Resetting to a higher limit and confirm all requests are allowed.
	limiter.Reset(20)
	require.True(t, limiter.IsAllowed(20, now))
}
