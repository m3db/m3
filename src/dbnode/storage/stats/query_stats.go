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
	"sync"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
)

var (
	globalQueryStats *queryStats
)

// For tracking query stats in past X duration such as blocks queried.
type queryStats struct {
	length  time.Duration
	tracker QueryStatsTracker

	recentDocs *atomic.Int64
	stopCh     chan struct{}
}

// Tracker implementation that emits query stats as metrics.
type queryStatsMetricsTracker struct {
	sync.Mutex
	recentDocs tally.Gauge
}

// QueryStatsTracker provides an interface for handling current query stats.
type QueryStatsTracker interface {
	TrackDocs(recentDocs int64) error
}

// UpdateQueryStats adds new query stats which are being tracked.
func UpdateQueryStats(newDocs int) error {
	if globalQueryStats == nil {
		return nil
	}
	if newDocs <= 0 {
		return nil
	}

	// Add the new stats to the global state.
	recentDocs := globalQueryStats.recentDocs.Add(int64(newDocs))

	// Invoke the custom tracker based on the new stats values.
	return globalQueryStats.tracker.TrackDocs(recentDocs)
}

// EnableQueryStatsTracking enables query stats to be tracked within a recency time window.
func EnableQueryStatsTracking(within time.Duration, tracker QueryStatsTracker) chan struct{} {
	globalQueryStats = &queryStats{
		length:     within,
		tracker:    tracker,
		recentDocs: atomic.NewInt64(0),
		stopCh:     make(chan struct{}),
	}
	globalQueryStats.start()

	// Give caller handle to stop the background tracking.
	return globalQueryStats.stopCh
}

func (w queryStats) start() {
	ticker := time.NewTicker(w.length)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Clear recent docs every X duration.
			w.recentDocs.Store(0)

			// Also invoke the track func for having zero value.
			w.tracker.TrackDocs(0)
		case <-w.stopCh:
			return
		}
	}
}

// DefaultQueryStatsTrackerForMetrics provides a tracker
// implementation that emits query stats as metrics.
func DefaultQueryStatsTrackerForMetrics(opts instrument.Options) QueryStatsTracker {
	scope := opts.
		MetricsScope().
		SubScope("query-stats")
	return &queryStatsMetricsTracker{
		recentDocs: scope.Gauge("recentDocs"),
	}
}

func (t *queryStatsMetricsTracker) TrackDocs(recentDocs int64) error {
	t.Lock()
	t.recentDocs.Update(float64(recentDocs))
	t.Unlock()
	return nil
}
