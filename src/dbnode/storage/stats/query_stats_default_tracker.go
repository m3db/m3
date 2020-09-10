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

	"github.com/m3db/m3/src/x/instrument"

	"github.com/uber-go/tally"
)

// DefaultLookback is the default lookback used for query stats tracking.
const DefaultLookback = time.Second * 5

// Tracker implementation that emits query stats as metrics.
type queryStatsTracker struct {
	recentDocs         tally.Gauge
	recentDocsMax      tally.Gauge
	totalDocs          tally.Counter
	recentBytesRead    tally.Gauge
	recentBytesReadMax tally.Gauge
	totalBytesRead     tally.Counter

	recentDocsLimitError      tally.Counter
	recentBytesReadLimitError tally.Counter

	options QueryStatsOptions
}

var _ QueryStatsTracker = (*queryStatsTracker)(nil)

// DefaultQueryStatsTracker provides a tracker
// implementation that emits query stats as metrics
// and enforces limits.
func DefaultQueryStatsTracker(
	instrumentOpts instrument.Options,
	queryStatsOpts QueryStatsOptions,
) QueryStatsTracker {
	scope := instrumentOpts.
		MetricsScope().
		SubScope("query-stats")
	return &queryStatsTracker{
		options:                   queryStatsOpts,
		recentDocs:                scope.Gauge("recent-docs-matched"),
		totalDocs:                 scope.Counter("total-docs-matched"),
		recentBytesRead:           scope.Gauge("recent-bytes-read"),
		totalBytesRead:            scope.Counter("total-bytes-read"),
		recentDocsLimitError:      scope.Tagged(map[string]string{"limit": "docs-matched"}).Counter("limit-exceeded"),
		recentBytesReadLimitError: scope.Tagged(map[string]string{"limit": "bytes-read"}).Counter("limit-exceeded"),
	}
}

func (t *queryStatsTracker) TrackStats(values QueryStatsValues) error {
	docsMatched := values.DocsMatched
	bytesRead := values.BytesRead

	// Only update the recent metrics on each reset so
	// we measure only consistently timed peak values.
	if docsMatched.Reset {
		t.recentDocs.Update(float64(docsMatched.Recent))
	}
	if bytesRead.Reset {
		t.recentBytesRead.Update(float64(bytesRead.Recent))
	}

	// Track stats as metrics.
	t.totalDocs.Inc(docsMatched.New)
	t.totalBytesRead.Inc(bytesRead.New)

	// Enforce max queried docs (if specified).
	if t.options.MaxDocs > 0 && docsMatched.Recent > t.options.MaxDocs {
		t.recentDocsLimitError.Inc(1)
		return fmt.Errorf(
			"query aborted, global recent time series blocks over limit: "+
				"limit=%d, current=%d, within=%s",
			t.options.MaxDocs, docsMatched.Recent, t.options.MaxDocsLookback)
	}
	// Enforce max queried docs (if specified).
	if t.options.MaxBytesRead > 0 && bytesRead.Recent > t.options.MaxBytesRead {
		t.recentBytesReadLimitError.Inc(1)
		return fmt.Errorf(
			"query aborted, global recent time series bytes read from disk over limit: "+
				"limit=%d, current=%d, within=%s",
			t.options.MaxBytesRead, bytesRead.Recent, t.options.MaxBytesReadLookback)
	}

	return nil
}

// Options returns the tracker options.
func (t *queryStatsTracker) Options() QueryStatsOptions {
	return t.options
}
