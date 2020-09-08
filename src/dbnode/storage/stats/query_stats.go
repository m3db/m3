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
	"fmt"
	"time"

	"go.uber.org/atomic"
)

// For tracking query stats in past X duration such as blocks queried.
type queryStats struct {
	tracker QueryStatsTracker

	recentDocs      *atomic.Int64
	recentBytesRead *atomic.Int64
	stopCh          chan struct{}
}

type noOpQueryStats struct {
}

var (
	_ QueryStats = (*queryStats)(nil)
	_ QueryStats = (*noOpQueryStats)(nil)
)

// QueryStats provides an interface for updating query stats.
type QueryStats interface {
	UpdateDocs(newDocs int) error
	UpdateBytesRead(newBytesRead int) error
	Start()
	Stop()
}

// QueryStatsOptions holds options for how a tracker should handle query stats.
type QueryStatsOptions struct {
	// MaxDocs limits how many recently queried max
	// documents are allowed before queries are abandoned.
	MaxDocs int64
	// MaxDocsLookback specifies the lookback period over which max docs limit is enforced.
	MaxDocsLookback time.Duration
	// MaxBytesRead limits how many recently queried bytes
	// read from disk are allowed before queries are abandoned.
	MaxBytesRead int64
	// MaxBytesReadLookback specifies the lookback period over which max bytes read limit is enforced.
	MaxBytesReadLookback time.Duration
}

// QueryStatsValues stores values of query stats.
type QueryStatsValues struct {
	RecentDocs int64
	NewDocs    int64
	ResetDocs  bool

	RecentBytesRead int64
	NewBytesRead    int64
	ResetBytesRead  bool
}

// QueryStatsTracker provides an interface for tracking current query stats.
type QueryStatsTracker interface {
	Options() QueryStatsOptions
	TrackStats(stats QueryStatsValues) error
}

// NewQueryStats enables query stats to be tracked within a recency lookback duration.
func NewQueryStats(tracker QueryStatsTracker) QueryStats {
	return &queryStats{
		tracker:         tracker,
		recentDocs:      atomic.NewInt64(0),
		recentBytesRead: atomic.NewInt64(0),
		stopCh:          make(chan struct{}),
	}
}

// NoOpQueryStats returns inactive query stats.
func NoOpQueryStats() QueryStats {
	return &noOpQueryStats{}
}

// UpdateDocs adds new docs stats to be tracked.
func (q *queryStats) UpdateDocs(newDocs int) error {
	if q == nil {
		return nil
	}
	if newDocs < 0 {
		return nil
	}

	newDocsI64 := int64(newDocs)

	// Add the new stats to the global state.
	recentDocs := q.recentDocs.Add(newDocsI64)

	values := QueryStatsValues{
		RecentDocs: recentDocs,
		NewDocs:    newDocsI64,
		// Pass current bytes-read along with docs since we want
		// to check if above that limit as well.
		RecentBytesRead: q.recentBytesRead.Load(),
		NewBytesRead:    0,
	}

	// Invoke the custom tracker based on the new stats values.
	return q.tracker.TrackStats(values)
}

// UpdateBytesRead adds new bytes stats to be tracked.
func (q *queryStats) UpdateBytesRead(newBytesRead int) error {
	if q == nil {
		return nil
	}
	if newBytesRead < 0 {
		return nil
	}

	newBytesReadI64 := int64(newBytesRead)

	// Add the new stats to the global state.
	recentBytes := q.recentBytesRead.Add(newBytesReadI64)

	values := QueryStatsValues{
		RecentBytesRead: recentBytes,
		NewBytesRead:    newBytesReadI64,
		// Pass current docs along with bytes-read since we want
		// to check if above that limit as well.
		RecentDocs: q.recentDocs.Load(),
		NewDocs:    0,
	}

	// Invoke the custom tracker based on the new stats values.
	return q.tracker.TrackStats(values)
}

// Start initializes background processing for handling query stats.
func (q *queryStats) Start() {
	if q == nil {
		return
	}
	opts := q.tracker.Options()
	docsTicker := time.NewTicker(opts.MaxDocsLookback)
	bytesTicker := time.NewTicker(opts.MaxDocsLookback)
	go func() {
		defer docsTicker.Stop()
		defer bytesTicker.Stop()
		for {
			// Invoke the track func for current values before resetting.
			select {
			case <-docsTicker.C:
				q.trackReset(true, false)
				q.recentDocs.Store(0)
			case <-bytesTicker.C:
				q.trackReset(false, true)
				q.recentBytesRead.Store(0)
			case <-q.stopCh:
				return
			}
		}
	}()
}

func (q *queryStats) trackReset(resetDocs bool, resetBytesRead bool) {
	_ = q.tracker.TrackStats(QueryStatsValues{
		RecentBytesRead: q.recentBytesRead.Load(),
		NewBytesRead:    0,
		RecentDocs:      q.recentDocs.Load(),
		NewDocs:         0,
		ResetDocs:       resetDocs,
		ResetBytesRead:  resetBytesRead,
	})
}

func (q *queryStats) Stop() {
	if q == nil {
		return
	}
	close(q.stopCh)
}

func (q *noOpQueryStats) UpdateDocs(int) error {
	return nil
}

func (q *noOpQueryStats) UpdateBytesRead(int) error {
	return nil
}

func (q *noOpQueryStats) Stop() {
}

func (q *noOpQueryStats) Start() {
}

// Validate returns an error if the query stats options are invalid.
func (opts QueryStatsOptions) Validate() error {
	if opts.MaxDocs < 0 {
		return fmt.Errorf("query stats tracker requires max docs >= 0 (%d)", opts.MaxDocs)
	}
	if opts.MaxDocsLookback <= 0 {
		return fmt.Errorf("query stats tracker requires max docs lookback > 0 (%d)", opts.MaxDocsLookback)
	}
	if opts.MaxBytesRead < 0 {
		return fmt.Errorf("query stats tracker requires max bytes read >= 0 (%d)", opts.MaxBytesRead)
	}
	if opts.MaxBytesReadLookback <= 0 {
		return fmt.Errorf("query stats tracker requires max bytes lookback > 0 (%d)", opts.MaxBytesReadLookback)
	}
	return nil
}
