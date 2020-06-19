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
		name          string
		maxDocs       int64
		lookback      time.Duration
		expectedError string
	}{
		{
			name:     "valid lookback without limit",
			maxDocs:  0,
			lookback: time.Millisecond,
		},
		{
			name:     "valid lookback with valid limit",
			maxDocs:  1,
			lookback: time.Millisecond,
		},
		{
			name:          "negative lookback",
			maxDocs:       0,
			lookback:      -time.Millisecond,
			expectedError: "query stats tracker requires lookback > 0 (-1000000)",
		},
		{
			name:          "zero lookback",
			maxDocs:       0,
			lookback:      time.Duration(0),
			expectedError: "query stats tracker requires lookback > 0 (0)",
		},
		{
			name:          "negative max",
			maxDocs:       -1,
			lookback:      time.Millisecond,
			expectedError: "query stats tracker requires max docs >= 0 (-1)",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := QueryStatsOptions{
				MaxDocs:  test.maxDocs,
				Lookback: test.lookback,
			}.Validate()
			if test.expectedError != "" {
				require.Error(t, err)
				require.Equal(t, test.expectedError, err.Error())
			} else {
				require.NoError(t, err)
			}
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
				Lookback: time.Second,
			},
		},
		{
			name: "metrics and limits",
			opts: QueryStatsOptions{
				MaxDocs:  1000,
				Lookback: time.Second,
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

	for _, test := range []struct {
		name             string
		opts             QueryStatsOptions
		expectLimitError string
	}{
		{
			name: "metrics only",
			opts: QueryStatsOptions{
				Lookback: time.Second,
			},
		},
		{
			name: "metrics and limits",
			opts: QueryStatsOptions{
				MaxDocs:  100,
				Lookback: time.Second,
			},
			expectLimitError: "query aborted, global recent time series blocks over limit: limit=100, current=101, within=1s",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracker := DefaultQueryStatsTracker(opts, test.opts)

			err := tracker.TrackStats(QueryStatsValues{RecentDocs: maxDocs + 1})
			if test.expectLimitError != "" {
				require.Error(t, err)
				require.Equal(t, test.expectLimitError, err.Error())
			} else {
				require.NoError(t, err)
			}

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: maxDocs - 1})
			require.NoError(t, err)

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: 0})
			require.NoError(t, err)

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: maxDocs + 1})
			if test.expectLimitError != "" {
				require.Error(t, err)
				require.Equal(t, test.expectLimitError, err.Error())
			} else {
				require.NoError(t, err)
			}

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: maxDocs - 1})
			require.NoError(t, err)

			err = tracker.TrackStats(QueryStatsValues{RecentDocs: 0})
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
