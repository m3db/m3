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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testQueryStatsTracker struct {
	QueryStatsValues
	lookback time.Duration
}

var _ QueryStatsTracker = (*testQueryStatsTracker)(nil)

func (t *testQueryStatsTracker) TrackStats(values QueryStatsValues) error {
	t.QueryStatsValues = values
	return nil
}

func (t *testQueryStatsTracker) Lookback() time.Duration {
	return t.lookback
}

func TestUpdateTracker(t *testing.T) {
	tracker := &testQueryStatsTracker{}

	queryStats := NewQueryStats(tracker)
	defer queryStats.Stop()

	err := queryStats.Update(3)
	require.NoError(t, err)
	verifyStats(t, tracker,  3, 3)

	err = queryStats.Update(2)
	require.NoError(t, err)
	verifyStats(t, tracker,  2, 5)
}

func TestPeriodicallyResetRecentDocs(t *testing.T) {
	tracker := &testQueryStatsTracker{lookback: time.Millisecond}

	queryStats := NewQueryStats(tracker)
	defer queryStats.Stop()
	queryStats.Start()

	err := queryStats.Update(1)
	require.NoError(t, err)
	verifyStats(t, tracker,  1, 1)

	time.Sleep(tracker.lookback * 2)

	verifyStats(t, tracker,  0, 0)
}

func verifyStats(t *testing.T, tracker *testQueryStatsTracker, expectedNew int, expectedRecent int64) {
	assert.Equal(t, expectedNew, tracker.NewDocs)
	assert.Equal(t, expectedRecent, tracker.RecentDocs)
}
