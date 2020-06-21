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
	recentDocs tally.Gauge
	totalDocs  tally.Counter

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
		options:    queryStatsOpts,
		recentDocs: scope.Gauge("recent-docs-per-block"),
		totalDocs:  scope.Counter("total-docs-per-block"),
	}
}

func (t *queryStatsTracker) TrackStats(values QueryStatsValues) error {
	// Track stats as metrics.
	t.recentDocs.Update(float64(values.RecentDocs))
	t.totalDocs.Inc(values.NewDocs)

	// Enforce max queried docs (if specified).
	if t.options.MaxDocs > 0 && values.RecentDocs > t.options.MaxDocs {
		return fmt.Errorf(
			"query aborted, global recent time series blocks over limit: "+
				"limit=%d, current=%d, within=%s",
			t.options.MaxDocs, values.RecentDocs, t.options.Lookback)
	}
	return nil
}

func (t *queryStatsTracker) Lookback() time.Duration {
	return t.options.Lookback
}
