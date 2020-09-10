// Copyright (c) 2020 Uber Technologies, Inc.
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

package limits

import (
	"fmt"
	"testing"
	"time"

	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/uber-go/tally"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLookbackLimit(t *testing.T) {
	for _, test := range []struct {
		name  string
		limit int64
	}{
		{name: "no limit", limit: 0},
		{name: "limit", limit: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			scope := tally.NewTestScope("scope", nil)
			opts := LookbackLimitOptions{
				Limit:    test.limit,
				Lookback: time.Millisecond * 100,
			}
			name := "test"
			limit := NewLookbackLimit(name, scope, opts)

			require.Equal(t, int64(0), limit.current())
			err := limit.Exceeded()
			require.NoError(t, err)

			// Validate ascending while checking limits.
			var exceededCount int64
			exceededCount += verifyLimit(t, limit, 3, test.limit)
			require.Equal(t, int64(3), limit.current())
			verifyMetrics(t, scope, name, 3, 0, 3, exceededCount)

			exceededCount += verifyLimit(t, limit, 2, test.limit)
			require.Equal(t, int64(5), limit.current())
			verifyMetrics(t, scope, name, 5, 0, 5, exceededCount)

			exceededCount += verifyLimit(t, limit, 1, test.limit)
			require.Equal(t, int64(6), limit.current())
			verifyMetrics(t, scope, name, 6, 0, 6, exceededCount)

			exceededCount += verifyLimit(t, limit, 4, test.limit)
			require.Equal(t, int64(10), limit.current())
			verifyMetrics(t, scope, name, 10, 0, 10, exceededCount)

			// Validate first reset.
			limit.reset()
			require.Equal(t, int64(0), limit.current())
			verifyMetrics(t, scope, name, 0, 10, 10, exceededCount)

			// Validate ascending again post-reset.
			exceededCount += verifyLimit(t, limit, 2, test.limit)
			require.Equal(t, int64(2), limit.current())
			verifyMetrics(t, scope, name, 2, 10, 12, exceededCount)

			exceededCount += verifyLimit(t, limit, 5, test.limit)
			require.Equal(t, int64(7), limit.current())
			verifyMetrics(t, scope, name, 7, 10, 17, exceededCount)

			// Validate second reset.
			limit.reset()

			require.Equal(t, int64(0), limit.current())
			verifyMetrics(t, scope, name, 0, 7, 17, exceededCount)

			// Validate consecutive reset (ensure peak goes to zero).
			limit.reset()

			require.Equal(t, int64(0), limit.current())
			verifyMetrics(t, scope, name, 0, 0, 17, exceededCount)
		})
	}
}

func verifyLimit(t *testing.T, limit LookbackLimit, inc int, expectedLimit int64) int64 {
	var exceededCount int64
	err := limit.Inc(inc)
	if limit.current() <= expectedLimit || expectedLimit == 0 {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		exceededCount++
	}
	err = limit.Exceeded()
	if limit.current() <= expectedLimit || expectedLimit == 0 {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		exceededCount++
	}
	return exceededCount
}

func TestLookbackReset(t *testing.T) {
	scope := tally.NewTestScope("scope", nil)
	opts := LookbackLimitOptions{
		Limit:    5,
		Lookback: time.Millisecond * 100,
	}
	name := "test"
	limit := NewLookbackLimit(name, scope, opts)

	err := limit.Inc(3)
	require.NoError(t, err)
	require.Equal(t, int64(3), limit.current())

	limit.Start()
	defer limit.Stop()
	time.Sleep(opts.Lookback * 2)

	success := xclock.WaitUntil(func() bool {
		return limit.current() == 0
	}, 5*time.Second)
	require.True(t, success, "did not eventually reset to zero")
}

func TestValidateLookbackLimitOptions(t *testing.T) {
	for _, test := range []struct {
		name        string
		max         int64
		lookback    time.Duration
		expectError bool
	}{
		{
			name:     "valid lookback without limit",
			max:      0,
			lookback: time.Millisecond,
		},
		{
			name:     "valid lookback with valid limit",
			max:      1,
			lookback: time.Millisecond,
		},
		{
			name:        "negative lookback",
			max:         0,
			lookback:    -time.Millisecond,
			expectError: true,
		},
		{
			name:        "zero lookback",
			max:         0,
			lookback:    time.Duration(0),
			expectError: true,
		},
		{
			name:        "negative max",
			max:         -1,
			lookback:    time.Millisecond,
			expectError: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := LookbackLimitOptions{
				Limit:    test.max,
				Lookback: test.lookback,
			}.Validate()
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Validate empty.
			require.Error(t, LookbackLimitOptions{}.Validate())
		})
	}
}

func verifyMetrics(t *testing.T,
	scope tally.TestScope,
	name string,
	expectedRecent float64,
	expectedRecentPeak float64,
	expectedTotal int64,
	expectedExceeded int64,
) {
	snapshot := scope.Snapshot()

	recent, exists := snapshot.Gauges()[fmt.Sprintf("scope.recent-%s+", name)]
	assert.True(t, exists)
	assert.Equal(t, expectedRecent, recent.Value(), "recent wrong")

	recentPeak, exists := snapshot.Gauges()[fmt.Sprintf("scope.recent-peak-%s+", name)]
	assert.True(t, exists)
	assert.Equal(t, expectedRecentPeak, recentPeak.Value(), "recent peak wrong")

	total, exists := snapshot.Counters()[fmt.Sprintf("scope.total-%s+", name)]
	assert.True(t, exists)
	assert.Equal(t, expectedTotal, total.Value(), "total wrong")

	exceeded, exists := snapshot.Counters()[fmt.Sprintf("scope.limit-exceeded+limit=%s", name)]
	assert.True(t, exists)
	assert.Equal(t, expectedExceeded, exceeded.Value(), "exceeded wrong")
}
