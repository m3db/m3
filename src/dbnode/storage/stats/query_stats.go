// Copyright (c) 2018 Uber Technologies, Inc.
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
	trackFn QueryStatsTrackFn

	recentDocs *atomic.Int64
	stopCh     chan struct{}
}

type queryStatsMetrics struct {
	sync.Mutex
	recentDocs tally.Gauge
}

// QueryStatsTrackFn provides an interface for handling current query stats.
type QueryStatsTrackFn func(
	recentDocs int64,
) error

// EnableQueryStatsTracking enables query stats to be tracked within a recency time window.
func EnableQueryStatsTracking(within time.Duration, trackFn QueryStatsTrackFn) chan struct{} {
	globalQueryStats = &queryStats{
		length:     within,
		trackFn:    trackFn,
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
			w.trackFn(0)
		case <-w.stopCh:
			return
		}
	}
}

// TrackStats tracks new query stats.
func TrackStats(newDocs int) error {
	if globalQueryStats == nil {
		return nil
	}
	if newDocs <= 0 {
		return nil
	}

	recentDocs := globalQueryStats.recentDocs.Add(int64(newDocs))
	return globalQueryStats.trackFn(recentDocs)
}

// QueryStatsMetricsTrackFn provides a tracking function that emits query stats as metrics.
func QueryStatsMetricsTrackFn(opts instrument.Options) QueryStatsTrackFn {
	scope := opts.
		MetricsScope().
		SubScope("query-stats")
	statsMetrics := queryStatsMetrics{
		recentDocs: scope.Gauge("recentDocs"),
	}
	return func(recentDocs int64) error {
		statsMetrics.Lock()
		statsMetrics.recentDocs.Update(float64(recentDocs))
		statsMetrics.Unlock()
		return nil
	}
}
