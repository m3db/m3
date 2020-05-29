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
	"time"

	"go.uber.org/atomic"
)

// For tracking query stats in past X duration such as blocks queried.
type queryStats struct {
	tracker QueryStatsTracker

	recentDocs *atomic.Int64
	stopCh     chan struct{}
}

type noOpQueryStats struct {
}

var (
	_ QueryStats = (*queryStats)(nil)
	_ QueryStats = (*noOpQueryStats)(nil)
)

// QueryStats provides an interface for updating query stats.
type QueryStats interface {
	Update(newDocs int) error
	Start()
	Stop()
}

// QueryStatsValues stores values of query stats.
type QueryStatsValues struct {
	RecentDocs int64
	NewDocs    int
}

var zeros = QueryStatsValues{
	RecentDocs: 0,
	NewDocs:    0,
}

// QueryStatsTracker provides an interface for tracking current query stats.
type QueryStatsTracker interface {
	Lookback() time.Duration
	TrackStats(stats QueryStatsValues) error
}

// NewQueryStats enables query stats to be tracked within a recency lookback duration.
func NewQueryStats(tracker QueryStatsTracker) QueryStats {
	return &queryStats{
		tracker:    tracker,
		recentDocs: atomic.NewInt64(0),
		stopCh:     make(chan struct{}),
	}
}

// NoOpQueryStats returns inactive query stats.
func NoOpQueryStats() QueryStats {
	return &noOpQueryStats{}
}

// UpdateQueryStats adds new query stats which are being tracked.
func (q *queryStats) Update(newDocs int) error {
	if q == nil {
		return nil
	}
	if newDocs <= 0 {
		return nil
	}

	// Add the new stats to the global state.
	recentDocs := q.recentDocs.Add(int64(newDocs))

	values := QueryStatsValues{
		RecentDocs: recentDocs,
		NewDocs:    newDocs,
	}

	// Invoke the custom tracker based on the new stats values.
	return q.tracker.TrackStats(values)
}

// Start initializes background processing for handling query stats.
func (q *queryStats) Start() {
	if q == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(q.tracker.Lookback())
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Clear recent docs every X duration.
				q.recentDocs.Store(0)

				// Also invoke the track func for having zero value.
				_ = q.tracker.TrackStats(zeros)
			case <-q.stopCh:
				return
			}
		}
	}()
}

func (q *queryStats) Stop() {
	if q == nil {
		return
	}
	close(q.stopCh)
}

func (q *noOpQueryStats) Update(newDocs int) error {
	return nil
}

func (q *noOpQueryStats) Stop() {
}

func (q *noOpQueryStats) Start() {
}
