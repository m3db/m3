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
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func testQueryLimitOptions(
	docOpts LookbackLimitOptions,
	bytesOpts LookbackLimitOptions,
	seriesOpts LookbackLimitOptions,
	iOpts instrument.Options,
) Options {
	return NewOptions().
		SetDocsLimitOpts(docOpts).
		SetBytesReadLimitOpts(bytesOpts).
		SetDiskSeriesReadLimitOpts(seriesOpts).
		SetInstrumentOptions(iOpts)
}

func TestQueryLimits(t *testing.T) {
	l := int64(1)
	docOpts := LookbackLimitOptions{
		Limit:    l,
		Lookback: time.Second,
	}
	bytesOpts := LookbackLimitOptions{
		Limit:    l,
		Lookback: time.Second,
	}
	seriesOpts := LookbackLimitOptions{
		Limit:    l,
		Lookback: time.Second,
	}
	opts := testQueryLimitOptions(docOpts, bytesOpts, seriesOpts, instrument.NewOptions())
	queryLimits, err := NewQueryLimits(opts)
	require.NoError(t, err)
	require.NotNil(t, queryLimits)

	// No error yet.
	require.NoError(t, queryLimits.AnyExceeded())

	// Limit from docs.
	require.Error(t, queryLimits.DocsLimit().Inc(2, nil))
	err = queryLimits.AnyExceeded()
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
	require.True(t, IsQueryLimitExceededError(err))

	opts = testQueryLimitOptions(docOpts, bytesOpts, seriesOpts, instrument.NewOptions())
	queryLimits, err = NewQueryLimits(opts)
	require.NoError(t, err)
	require.NotNil(t, queryLimits)

	// No error yet.
	err = queryLimits.AnyExceeded()
	require.NoError(t, err)

	// Limit from bytes.
	require.Error(t, queryLimits.BytesReadLimit().Inc(2, nil))
	err = queryLimits.AnyExceeded()
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
	require.True(t, IsQueryLimitExceededError(err))

	opts = testQueryLimitOptions(docOpts, bytesOpts, seriesOpts, instrument.NewOptions())
	queryLimits, err = NewQueryLimits(opts)
	require.NoError(t, err)
	require.NotNil(t, queryLimits)

	// No error yet.
	err = queryLimits.AnyExceeded()
	require.NoError(t, err)

	// Limit from bytes.
	require.Error(t, queryLimits.DiskSeriesReadLimit().Inc(2, nil))
	err = queryLimits.AnyExceeded()
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
	require.True(t, IsQueryLimitExceededError(err))
}

func TestLookbackLimit(t *testing.T) {
	for _, test := range []struct {
		name          string
		limit         int64
		forceExceeded bool
	}{
		{name: "no limit", limit: 0},
		{name: "limit", limit: 5},
		{name: "force exceeded limit", limit: 5, forceExceeded: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scope := tally.NewTestScope("", nil)
			iOpts := instrument.NewOptions().SetMetricsScope(scope)
			opts := LookbackLimitOptions{
				Limit:         test.limit,
				Lookback:      time.Millisecond * 100,
				ForceExceeded: test.forceExceeded,
			}
			name := "test"
			limit := newLookbackLimit(iOpts, opts, name, &sourceLoggerBuilder{})

			require.Equal(t, int64(0), limit.current())

			var exceededCount int64
			err := limit.exceeded()
			if test.limit >= 0 && !test.forceExceeded {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				exceededCount++
			}

			// Validate ascending while checking limits.
			exceededCount += verifyLimit(t, limit, 3, test.limit, test.forceExceeded)
			require.Equal(t, int64(3), limit.current())
			verifyMetrics(t, scope, name, 3, 0, 3, exceededCount)

			exceededCount += verifyLimit(t, limit, 2, test.limit, test.forceExceeded)
			require.Equal(t, int64(5), limit.current())
			verifyMetrics(t, scope, name, 5, 0, 5, exceededCount)

			exceededCount += verifyLimit(t, limit, 1, test.limit, test.forceExceeded)
			require.Equal(t, int64(6), limit.current())
			verifyMetrics(t, scope, name, 6, 0, 6, exceededCount)

			exceededCount += verifyLimit(t, limit, 4, test.limit, test.forceExceeded)
			require.Equal(t, int64(10), limit.current())
			verifyMetrics(t, scope, name, 10, 0, 10, exceededCount)

			// Validate first reset.
			limit.reset()
			require.Equal(t, int64(0), limit.current())
			verifyMetrics(t, scope, name, 0, 10, 10, exceededCount)

			// Validate ascending again post-reset.
			exceededCount += verifyLimit(t, limit, 2, test.limit, test.forceExceeded)
			require.Equal(t, int64(2), limit.current())
			verifyMetrics(t, scope, name, 2, 10, 12, exceededCount)

			exceededCount += verifyLimit(t, limit, 5, test.limit, test.forceExceeded)
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

			limit.reset()

			opts.Limit = 0
			require.NoError(t, limit.Update(opts))

			exceededCount += verifyLimit(t, limit, 0, opts.Limit, test.forceExceeded)
			require.Equal(t, int64(0), limit.current())

			opts.Limit = 2
			require.NoError(t, limit.Update(opts))

			exceededCount += verifyLimit(t, limit, 1, opts.Limit, test.forceExceeded)
			require.Equal(t, int64(1), limit.current())
			verifyMetrics(t, scope, name, 1, 0, 18, exceededCount)

			exceededCount += verifyLimit(t, limit, 1, opts.Limit, test.forceExceeded)
			require.Equal(t, int64(2), limit.current())
			verifyMetrics(t, scope, name, 2, 0, 19, exceededCount)

			exceededCount += verifyLimit(t, limit, 1, opts.Limit, test.forceExceeded)
			require.Equal(t, int64(3), limit.current())
			verifyMetrics(t, scope, name, 3, 0, 20, exceededCount)
		})
	}
}

func verifyLimit(t *testing.T, limit *lookbackLimit, inc int, expectedLimit int64, forceExceeded bool) int64 {
	var exceededCount int64
	err := limit.Inc(inc, nil)
	if (expectedLimit == 0 || limit.current() < expectedLimit) && !forceExceeded {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		require.True(t, xerrors.IsInvalidParams(err))
		require.True(t, IsQueryLimitExceededError(err))
		exceededCount++
	}

	err = limit.exceeded()
	if (expectedLimit == 0 || limit.current() < expectedLimit) && !forceExceeded {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		require.True(t, xerrors.IsInvalidParams(err))
		require.True(t, IsQueryLimitExceededError(err))
		exceededCount++
	}
	return exceededCount
}

func TestLookbackReset(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)
	opts := LookbackLimitOptions{
		Limit:    5,
		Lookback: time.Millisecond * 100,
	}
	name := "test"
	limit := newLookbackLimit(iOpts, opts, name, &sourceLoggerBuilder{})

	err := limit.Inc(3, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), limit.current())

	limit.start()
	defer limit.stop()
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
			}.validate()
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Validate empty.
			require.Error(t, LookbackLimitOptions{}.validate())
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

	recent, exists := snapshot.Gauges()[fmt.Sprintf("query-limit.recent-count-%s+", name)]
	assert.True(t, exists)
	assert.Equal(t, expectedRecent, recent.Value(), "recent count wrong")

	recentPeak, exists := snapshot.Gauges()[fmt.Sprintf("query-limit.recent-max-%s+", name)]
	assert.True(t, exists)
	assert.Equal(t, expectedRecentPeak, recentPeak.Value(), "recent max wrong")

	total, exists := snapshot.Counters()[fmt.Sprintf("query-limit.total-%s+", name)]
	assert.True(t, exists)
	assert.Equal(t, expectedTotal, total.Value(), "total wrong")

	exceeded, exists := snapshot.Counters()[fmt.Sprintf("query-limit.exceeded+limit=%s", name)]
	assert.True(t, exists)
	assert.Equal(t, expectedExceeded, exceeded.Value(), "exceeded wrong")
}

type testLoggerRecord struct {
	name   string
	val    int64
	source []byte
}

func TestSourceLogger(t *testing.T) {
	var (
		scope   = tally.NewTestScope("test", nil)
		iOpts   = instrument.NewOptions().SetMetricsScope(scope)
		noLimit = LookbackLimitOptions{
			Limit:    0,
			Lookback: time.Millisecond * 100,
		}

		builder = &testBuilder{records: []testLoggerRecord{}}
		opts    = testQueryLimitOptions(noLimit, noLimit, noLimit, iOpts).
			SetSourceLoggerBuilder(builder)
	)

	require.NoError(t, opts.Validate())

	queryLimits, err := NewQueryLimits(opts)
	require.NoError(t, err)
	require.NotNil(t, queryLimits)

	require.NoError(t, queryLimits.DocsLimit().Inc(100, []byte("docs")))
	require.NoError(t, queryLimits.BytesReadLimit().Inc(200, []byte("bytes")))

	assert.Equal(t, []testLoggerRecord{
		{name: "docs-matched", val: 100, source: []byte("docs")},
		{name: "disk-bytes-read", val: 200, source: []byte("bytes")},
	}, builder.records)
}

// NB: creates test logger records that share an underlying record set,
// differentiated by source logger name.
type testBuilder struct {
	records []testLoggerRecord
}

var _ SourceLoggerBuilder = (*testBuilder)(nil)

func (s *testBuilder) NewSourceLogger(n string, _ instrument.Options) SourceLogger {
	return &testSourceLogger{name: n, builder: s}
}

type testSourceLogger struct {
	name    string
	builder *testBuilder
}

var _ SourceLogger = (*testSourceLogger)(nil)

func (l *testSourceLogger) LogSourceValue(val int64, source []byte) {
	l.builder.records = append(l.builder.records, testLoggerRecord{
		name:   l.name,
		val:    val,
		source: source,
	})
}
