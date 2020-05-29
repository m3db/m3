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

	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestEmitQueryStatsBasedMetrics(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	opts := instrument.NewOptions().SetMetricsScope(scope)

	tracker := DefaultQueryStatsTrackerForMetrics(opts)

	err := tracker.TrackStats(QueryStatsValues{RecentDocs: 100, NewDocs: 5})
	require.NoError(t, err)
	verifyMetrics(t, scope, 100, 5)

	err = tracker.TrackStats(QueryStatsValues{RecentDocs: 140, NewDocs: 10})
	require.NoError(t, err)
	verifyMetrics(t, scope, 140, 15)
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
