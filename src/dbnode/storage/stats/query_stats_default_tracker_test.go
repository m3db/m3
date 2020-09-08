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

package stats

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

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
			scope := tally.NewTestScope("", nil)
			opts := instrument.NewOptions().SetMetricsScope(scope)

			tracker := DefaultQueryStatsTracker(opts, test.opts)

			err := tracker.TrackStats(QueryStatsValues{RecentDocs: 100, NewDocs: 5})
			require.NoError(t, err)
			verifyMetrics(t, scope, 100, 5)

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: 140, NewDocs: 10})
			require.NoError(t, err)
			verifyMetrics(t, scope, 140, 15)
		})
	}
}

func TestLimitMaxDocs(t *testing.T) {
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

			err := tracker.TrackStats(QueryStatsValues{RecentDocs: maxDocs + 1, RecentBytesRead: maxBytes - 1})
			if test.expectDocsLimitError != "" {
				require.Error(t, err)
				require.Equal(t, test.expectDocsLimitError, err.Error())
			} else {
				require.NoError(t, err)
			}

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: maxDocs - 1, RecentBytesRead: maxBytes + 1})
			if test.expectBytesLimitError != "" {
				require.Error(t, err)
				require.Equal(t, test.expectBytesLimitError, err.Error())
			} else {
				require.NoError(t, err)
			}

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: maxDocs + 1, RecentBytesRead: maxBytes + 1})
			if test.expectBytesLimitError != "" || test.expectDocsLimitError != "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: maxDocs - 1, RecentBytesRead: maxBytes - 1})
			require.NoError(t, err)

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: 0, RecentBytesRead: 0})
			require.NoError(t, err)

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: maxDocs + 1, RecentBytesRead: maxBytes + 1})
			if test.expectBytesLimitError != "" || test.expectDocsLimitError != "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: maxDocs - 1, RecentBytesRead: maxBytes - 1})
			require.NoError(t, err)

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: 0, RecentBytesRead: 0})
			require.NoError(t, err)
		})
	}
}

func verifyMetrics(t *testing.T, scope tally.TestScope, expectedRecent float64, expectedTotal int64) {
	snapshot := scope.Snapshot()

	recent, exists := snapshot.Gauges()["query-stats.recent-docs-per-block+"]
	assert.True(t, exists)
	assert.Equal(t, expectedRecent, recent.Value())

	total, exists := snapshot.Counters()["query-stats.total-docs-per-block+"]
	assert.True(t, exists)
	assert.Equal(t, expectedTotal, total.Value())
}
