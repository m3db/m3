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
	"sync"
	"testing"
	"time"

	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testQueryStatsTracker struct {
	sync.RWMutex
	QueryStatsValues
	lookback time.Duration
}

var _ QueryStatsTracker = (*testQueryStatsTracker)(nil)

func (t *testQueryStatsTracker) TrackStats(values QueryStatsValues) error {
	t.Lock()
	defer t.Unlock()

	t.QueryStatsValues = values
	return nil
}

func (t *testQueryStatsTracker) StatsValues() QueryStatsValues {
	t.RLock()
	defer t.RUnlock()

	return t.QueryStatsValues
}

func (t *testQueryStatsTracker) Options() QueryStatsOptions {
	return QueryStatsOptions{
		MaxDocsLookback:      t.lookback,
		MaxBytesReadLookback: t.lookback,
	}
}

func TestUpdateTracker(t *testing.T) {
	tracker := &testQueryStatsTracker{}

	queryStats := NewQueryStats(tracker)
	defer queryStats.Stop()

	// Test docs adding.
	err := queryStats.UpdateDocs(3)
	require.NoError(t, err)
	verifyStats(t, tracker, 3, 3, 0, 0)

	err = queryStats.UpdateDocs(2)
	require.NoError(t, err)
	verifyStats(t, tracker, 2, 5, 0, 0)

	// Test bytes adding.
	err = queryStats.UpdateBytesRead(4)
	require.NoError(t, err)
	verifyStats(t, tracker, 0, 5, 4, 4)

	err = queryStats.UpdateBytesRead(2)
	require.NoError(t, err)
	verifyStats(t, tracker, 0, 5, 2, 6)

	// Test docs adding after bytes and zeroes.
	err = queryStats.UpdateDocs(0)
	require.NoError(t, err)
	verifyStats(t, tracker, 0, 5, 0, 6)

	err = queryStats.UpdateDocs(1)
	require.NoError(t, err)
	verifyStats(t, tracker, 1, 6, 0, 6)

	// Test bytes adding after docs and zeroes.
	err = queryStats.UpdateBytesRead(0)
	require.NoError(t, err)
	verifyStats(t, tracker, 0, 6, 0, 6)

	err = queryStats.UpdateBytesRead(1)
	require.NoError(t, err)
	verifyStats(t, tracker, 0, 6, 1, 7)
}

func TestPeriodicallyResetRecentDocs(t *testing.T) {
	tracker := &testQueryStatsTracker{lookback: 100 * time.Millisecond}

	queryStats := NewQueryStats(tracker)

	err := queryStats.UpdateDocs(1)
	require.NoError(t, err)
	verifyStats(t, tracker, 1, 1, 0, 0)

	err = queryStats.UpdateBytesRead(1)
	require.NoError(t, err)
	verifyStats(t, tracker, 0, 1, 1, 1)

	queryStats.Start()
	defer queryStats.Stop()
	time.Sleep(tracker.lookback * 2)

	success := xclock.WaitUntil(func() bool {
		return statsEqual(tracker.StatsValues(), 0, 0, 0, 0)
	}, 10*time.Second)
	require.True(t, success, "did not eventually reset to zeroes")
}

func TestValidateTrackerInputs(t *testing.T) {
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
			// Validate docs limit.
			err := QueryStatsOptions{
				MaxDocs:              test.max,
				MaxDocsLookback:      test.lookback,
				MaxBytesReadLookback: test.lookback,
			}.Validate()
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Validate bytes read limit.
			err = QueryStatsOptions{
				MaxBytesRead:         test.max,
				MaxDocsLookback:      test.lookback,
				MaxBytesReadLookback: test.lookback,
			}.Validate()
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Validate all limits.
			err = QueryStatsOptions{
				MaxDocs:              test.max,
				MaxDocsLookback:      test.lookback,
				MaxBytesRead:         test.max,
				MaxBytesReadLookback: test.lookback,
			}.Validate()
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Validate empty.
			require.Error(t, QueryStatsOptions{}.Validate())
		})
	}
}

func TestEmitQueryStatsBasedMetrics(t *testing.T) {
	for _, test := range []struct {
		name string
		opts QueryStatsOptions
	}{
		{
			name: "metrics only",
			opts: QueryStatsOptions{
				MaxDocsLookback: time.Second,
			},
		},
		{
			name: "metrics and limits",
			opts: QueryStatsOptions{
				MaxDocs:         1000,
				MaxDocsLookback: time.Second,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Test tracking docs.
			getStats := func(val QueryStatsValue) QueryStatsValues {
				return QueryStatsValues{DocsMatched: val}
			}
			verifyTrackedStats(t, test.opts, getStats, true, false)

			// Test tracking bytes-read.
			getStats = func(val QueryStatsValue) QueryStatsValues {
				return QueryStatsValues{BytesRead: val}
			}
			verifyTrackedStats(t, test.opts, getStats, false, true)

			// Test tracking both.
			getStats = func(val QueryStatsValue) QueryStatsValues {
				return QueryStatsValues{DocsMatched: val, BytesRead: val}
			}
			verifyTrackedStats(t, test.opts, getStats, true, true)
		})
	}
}

func verifyTrackedStats(t *testing.T,
	opts QueryStatsOptions,
	getStats func(QueryStatsValue) QueryStatsValues,
	expectDocs bool,
	expectBytesRead bool,
) {
	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)
	tracker := DefaultQueryStatsTracker(iOpts, opts)

	docsName := "docs-matched"
	bytesReadName := "bytes-read"

	err := tracker.TrackStats(getStats(QueryStatsValue{Recent: 100, New: 5, Reset: true}))
	require.NoError(t, err)
	if expectDocs {
		verifyMetrics(t, scope, 100, 5, docsName)
	}
	if expectBytesRead {
		verifyMetrics(t, scope, 100, 5, bytesReadName)
	}

	err = tracker.TrackStats(getStats(QueryStatsValue{Recent: 140, New: 10, Reset: true}))
	require.NoError(t, err)
	if expectDocs {
		verifyMetrics(t, scope, 140, 15, docsName)
	}
	if expectBytesRead {
		verifyMetrics(t, scope, 140, 15, bytesReadName)
	}

	err = tracker.TrackStats(getStats(QueryStatsValue{Recent: 140, New: 20, Reset: false}))
	require.NoError(t, err)
	if expectDocs {
		verifyMetrics(t, scope, 140, 35, docsName)
	}
	if expectBytesRead {
		verifyMetrics(t, scope, 140, 35, bytesReadName)
	}

	err = tracker.TrackStats(getStats(QueryStatsValue{Recent: 140, New: 40, Reset: false}))
	require.NoError(t, err)
	if expectDocs {
		verifyMetrics(t, scope, 140, 75, docsName)
	}
	if expectBytesRead {
		verifyMetrics(t, scope, 140, 75, bytesReadName)
	}

	err = tracker.TrackStats(getStats(QueryStatsValue{Recent: 0, New: 25, Reset: true}))
	require.NoError(t, err)
	if expectDocs {
		verifyMetrics(t, scope, 0, 100, docsName)
	}
	if expectBytesRead {
		verifyMetrics(t, scope, 0, 100, bytesReadName)
	}

}

func TestLimits(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	opts := instrument.NewOptions().SetMetricsScope(scope)

	maxDocs := int64(100)
	maxBytes := int64(100)

	for _, test := range []struct {
		name                  string
		opts                  QueryStatsOptions
		expectDocsLimitError  string
		expectBytesLimitError string
	}{
		{
			name: "metrics only",
			opts: QueryStatsOptions{
				MaxDocsLookback:      time.Second,
				MaxBytesReadLookback: time.Second,
			},
		},
		{
			name: "metrics and docs limit",
			opts: QueryStatsOptions{
				MaxDocs:              100,
				MaxDocsLookback:      time.Second,
				MaxBytesReadLookback: time.Second,
			},
			expectDocsLimitError: "query aborted, global recent time series blocks over limit: limit=100, current=101, within=1s",
		},
		{
			name: "metrics and bytes limit",
			opts: QueryStatsOptions{
				MaxBytesRead:         100,
				MaxDocsLookback:      time.Second,
				MaxBytesReadLookback: time.Second,
			},
			expectBytesLimitError: "query aborted, global recent time series bytes read from disk over limit: limit=100, current=101, within=1s",
		},
		{
			name: "metrics and limits",
			opts: QueryStatsOptions{
				MaxDocs:              100,
				MaxBytesRead:         100,
				MaxDocsLookback:      time.Second,
				MaxBytesReadLookback: time.Second,
			},
			expectDocsLimitError:  "query aborted, global recent time series blocks over limit: limit=100, current=101, within=1s",
			expectBytesLimitError: "query aborted, global recent time series bytes read from disk over limit: limit=100, current=101, within=1s",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracker := DefaultQueryStatsTracker(opts, test.opts)

			err := tracker.TrackStats(QueryStatsValues{
				DocsMatched: QueryStatsValue{Recent: maxDocs + 1},
				BytesRead:   QueryStatsValue{Recent: maxBytes - 1},
			})
			if test.expectDocsLimitError != "" {
				require.Error(t, err)
				require.Equal(t, test.expectDocsLimitError, err.Error())
			} else {
				require.NoError(t, err)
			}

			err = tracker.TrackStats(QueryStatsValues{
				DocsMatched: QueryStatsValue{Recent: maxDocs - 1},
				BytesRead:   QueryStatsValue{Recent: maxBytes + 1},
			})
			if test.expectBytesLimitError != "" {
				require.Error(t, err)
				require.Equal(t, test.expectBytesLimitError, err.Error())
			} else {
				require.NoError(t, err)
			}

			err = tracker.TrackStats(QueryStatsValues{
				DocsMatched: QueryStatsValue{Recent: maxDocs + 1},
				BytesRead:   QueryStatsValue{Recent: maxBytes + 1},
			})
			if test.expectBytesLimitError != "" || test.expectDocsLimitError != "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			err = tracker.TrackStats(QueryStatsValues{
				DocsMatched: QueryStatsValue{Recent: maxDocs - 1},
				BytesRead:   QueryStatsValue{Recent: maxBytes - 1},
			})
			require.NoError(t, err)

			err = tracker.TrackStats(QueryStatsValues{
				DocsMatched: QueryStatsValue{Recent: 0},
				BytesRead:   QueryStatsValue{Recent: 0},
			})
			require.NoError(t, err)

			err = tracker.TrackStats(QueryStatsValues{
				DocsMatched: QueryStatsValue{Recent: maxDocs + 1},
				BytesRead:   QueryStatsValue{Recent: maxBytes + 1},
			})
			if test.expectBytesLimitError != "" || test.expectDocsLimitError != "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			err = tracker.TrackStats(QueryStatsValues{
				DocsMatched: QueryStatsValue{Recent: maxDocs - 1},
				BytesRead:   QueryStatsValue{Recent: maxBytes - 1},
			})
			require.NoError(t, err)

			err = tracker.TrackStats(QueryStatsValues{
				DocsMatched: QueryStatsValue{Recent: 0},
				BytesRead:   QueryStatsValue{Recent: 0},
			})
			require.NoError(t, err)
		})
	}
}

func verifyMetrics(t *testing.T, scope tally.TestScope, expectedRecent float64, expectedTotal int64, name string) {
	snapshot := scope.Snapshot()

	recent, exists := snapshot.Gauges()[fmt.Sprintf("query-stats.recent-%s+", name)]
	assert.True(t, exists)
	assert.Equal(t, expectedRecent, recent.Value())

	total, exists := snapshot.Counters()[fmt.Sprintf("query-stats.total-%s+", name)]
	assert.True(t, exists)
	assert.Equal(t, expectedTotal, total.Value())
}

func verifyStats(t *testing.T,
	tracker *testQueryStatsTracker,
	expectedNewDocs int64,
	expectedRecentDocs int64,
	expectedNewBytesRead int64,
	expectedRecentBytesRead int64) {
	values := tracker.StatsValues()
	assert.True(t, statsEqual(values, expectedNewDocs, expectedRecentDocs, expectedNewBytesRead, expectedRecentBytesRead))
}

func statsEqual(values QueryStatsValues,
	expectedNewDocs int64,
	expectedRecentDocs int64,
	expectedNewBytesRead int64,
	expectedRecentBytesRead int64,
) bool {
	return expectedNewDocs == values.DocsMatched.New &&
		expectedRecentDocs == values.DocsMatched.Recent &&
		expectedNewBytesRead == values.BytesRead.New &&
		expectedRecentBytesRead == values.BytesRead.Recent
}
