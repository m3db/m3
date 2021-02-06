// Copyright (c) 2021 Uber Technologies, Inc.
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

package index

import (
	"math"
	"time"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/x/instrument"
)

// NewQueryMetrics returns a new QueryMetrics.
func NewQueryMetrics(name string, scope tally.Scope) QueryMetrics {
	return QueryMetrics{
		ByRange: NewQueryRangeMetrics(name, scope),
		ByDocs:  NewCardinalityMetrics(name, scope),
	}
}

// QueryMetrics is a composite type of QueryDurationMetrics and QueryCardinalityMetrics.
type QueryMetrics struct {
	ByRange QueryDurationMetrics
	ByDocs  QueryCardinalityMetrics
}

type queryRangeHist struct {
	threshold time.Duration
	timing    tally.Histogram
}

func newQueryRangeHist(
	threshold time.Duration,
	value string,
	scope tally.Scope,
) *queryRangeHist {
	return &queryRangeHist{
		threshold: threshold,
		timing: scope.
			Tagged(map[string]string{"range": value}).
			Histogram("timing", instrument.SparseHistogramTimerHistogramBuckets()),
	}
}

type queryDurationMetrics struct {
	histograms []*queryRangeHist
}

func (bm *queryDurationMetrics) Record(
	queryRange time.Duration,
	queryRuntime time.Duration,
) {
	for _, m := range bm.histograms {
		if queryRange <= m.threshold {
			m.timing.RecordDuration(queryRuntime)
			return
		}
	}
}

// NewQueryRangeMetrics creates query duration metrics.
func NewQueryRangeMetrics(metricType string, scope tally.Scope) QueryDurationMetrics {
	scope = scope.SubScope(metricType).SubScope("query_range")
	return &queryDurationMetrics{
		histograms: []*queryRangeHist{
			newQueryRangeHist(time.Minute*5, "5m", scope),
			newQueryRangeHist(time.Minute*30, "30m", scope),
			newQueryRangeHist(time.Hour, "1hr", scope),
			newQueryRangeHist(time.Hour*6, "6hr", scope),
			newQueryRangeHist(time.Hour*24, "1d", scope),
			newQueryRangeHist(time.Hour*24*5, "5d", scope),
			newQueryRangeHist(time.Hour*24*10, "10d", scope),
			newQueryRangeHist(time.Hour*24*30, "30d", scope),
			newQueryRangeHist(time.Duration(math.MaxInt32), "max", scope),
		},
	}
}

// QueryDurationMetrics are timing metrics bucketed by the query window (start, end).
type QueryDurationMetrics interface {
	// Record records the runtime for queries given their range.
	Record(queryRange time.Duration, queryRuntime time.Duration)
}

type cardinalityHist struct {
	threshold int
	timing    tally.Histogram
}

func newCardinalityHist(
	threshold int,
	value string,
	scope tally.Scope,
) *cardinalityHist {
	return &cardinalityHist{
		threshold: threshold,
		timing: scope.
			Tagged(map[string]string{"count": value}).
			Histogram("timing", instrument.SparseHistogramTimerHistogramBuckets()),
	}
}

type queryCardinality struct {
	cardinalityHists []*cardinalityHist
}

func (bm *queryCardinality) Record(seriesCount int, queryRuntime time.Duration) {
	for _, m := range bm.cardinalityHists {
		if seriesCount <= m.threshold {
			m.timing.RecordDuration(queryRuntime)
			return
		}
	}
}

// NewCardinalityMetrics creates query cardinality metrics.
func NewCardinalityMetrics(metricType string, scope tally.Scope) QueryCardinalityMetrics {
	scope = scope.SubScope(metricType).SubScope("cardinality")
	return &queryCardinality{
		cardinalityHists: []*cardinalityHist{
			newCardinalityHist(50, "50", scope),
			newCardinalityHist(200, "200", scope),
			newCardinalityHist(1000, "1k", scope),
			newCardinalityHist(5000, "5k", scope),
			newCardinalityHist(10_000, "10k", scope),
			newCardinalityHist(50_000, "50k", scope),
			newCardinalityHist(100_000, "100k", scope),
			newCardinalityHist(250_000, "250k", scope),
			newCardinalityHist(500_000, "500k", scope),
			newCardinalityHist(750_000, "750k", scope),
			newCardinalityHist(1_000_000, "1M", scope),
			newCardinalityHist(int(math.MaxInt32), "max", scope),
		},
	}
}

// QueryCardinalityMetrics are metrics bucketed by the # of documents returned by a query result.
type QueryCardinalityMetrics interface {
	// Record records the runtime for queries given their cardinality.
	Record(docCount int, queryRuntime time.Duration)
}
